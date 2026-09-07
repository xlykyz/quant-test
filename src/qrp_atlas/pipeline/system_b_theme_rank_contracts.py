"""Formal production contract for Task06-B System B Theme Trend Rank."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from qrp_atlas.contracts import (
    POPULARITY_SOURCE_AVAILABILITY,
    POPULARITY_SOURCE_AVAILABILITY_TABLE,
    SYSTEM_B_THEME_RANK_COMPONENT_AUDIT,
    SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE,
    SYSTEM_B_THEME_RANK_SNAPSHOT,
    SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_ID,
    THEME_M4_OBSERVATION,
    THEME_M4_OBSERVATION_TABLE,
    TRADE_DATE,
    TRADING_CALENDAR,
)
from qrp_atlas.orchestration.models import OverlapPolicy

from .contracts import (
    BusinessExecution,
    CheckResult,
    CompletionContract,
    ContractError,
    DiagnosticLevel,
    ExecutionPolicy,
    FreshnessContract,
    IdempotencyContract,
    InputContract,
    InputKind,
    NonTradingDayPolicy,
    OutputContract,
    OutputResult,
    PerformanceBudget,
    PipelineContract,
    PipelineDiagnostic,
    PipelineInvocation,
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline
from .system_b_theme_rank.service import (
    SystemBThemeRankProductionError,
    run_theme_rank_daily,
)

CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"

_THEME_RANK_TABLES = (
    TRADING_CALENDAR.name,
    POPULARITY_SOURCE_AVAILABILITY.name,
    THEME_M4_OBSERVATION.name,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
)


def _target_date(invocation: PipelineInvocation) -> TargetWindow:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def _validate_target_date(target_date: date, _invocation: PipelineInvocation) -> bool:
    return isinstance(target_date, date) and not isinstance(target_date, datetime)


SYSTEM_B_THEME_RANK_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="system_b_theme_rank_scheduled_shanghai_date_v1",
    description="Uses the scheduled Asia/Shanghai calendar date; closed dates are explicit no-ops.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _target(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("THEME_RANK_TARGET_DATE_MISSING")
    return target


def _inspect(
    path: Path,
    *,
    check_id: str,
    tables: tuple[str, ...],
    columns: dict[str, tuple[str, ...]],
    error_code: str,
) -> CheckResult:
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(path), read_only=True)
        actual_tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        missing_tables = sorted(set(tables) - actual_tables)
        if missing_tables:
            return CheckResult.failure(check_id, error_code, "required tables are missing", missing=missing_tables)
        missing_columns: dict[str, list[str]] = {}
        for table, required in columns.items():
            actual = {
                str(row[0])
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=?",
                    [table],
                ).fetchall()
            }
            missing = sorted(set(required) - actual)
            if missing:
                missing_columns[table] = missing
        if missing_columns:
            return CheckResult.failure(check_id, error_code, "required columns are missing", missing=missing_columns)
        return CheckResult.success(check_id, path=str(path), tables=list(tables))
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(check_id, error_code, "database could not be inspected", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()


def _quant_structure(context: PipelineRunContext) -> CheckResult:
    return _inspect(
        Path(context.settings.paths.duckdb_path),
        check_id="system_b_theme_rank_quant_structure",
        tables=_THEME_RANK_TABLES,
        columns={
            TRADING_CALENDAR.name: (TRADE_DATE, "is_open"),
            POPULARITY_SOURCE_AVAILABILITY.name: tuple(POPULARITY_SOURCE_AVAILABILITY.column_names()),
            THEME_M4_OBSERVATION.name: (THEME_ID, TRADE_DATE, "effective_member_count", "custom_index_episode_id"),
        },
        error_code="THEME_RANK_QUANT_INPUT_STRUCTURE_MISSING",
    )


def _is_open(context: PipelineRunContext) -> tuple[bool, date]:
    target = _target(context)
    connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
    try:
        row = connection.execute(
            f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date=?", [target]
        ).fetchone()
    finally:
        connection.close()
    return bool(row and row[0]), target


def _quant_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        return CheckResult.success(
            "system_b_theme_rank_quant_freshness",
            target_date=target.isoformat(),
            is_open=is_open,
            noop=not is_open,
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "system_b_theme_rank_quant_freshness",
            "THEME_RANK_QUANT_INPUT_STALE",
            "target calendar could not be read",
            exception=type(exc).__name__,
        )


def _m4_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        if not is_open:
            return CheckResult.success(
                "system_b_theme_rank_m4_freshness",
                target_date=target.isoformat(),
                noop=True,
            )
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            count = connection.execute(
                f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date=?",
                [target],
            ).fetchone()[0]
        finally:
            connection.close()
        if count == 0:
            return CheckResult.failure(
                "system_b_theme_rank_m4_freshness",
                "THEME_RANK_M4_INPUT_STALE",
                "M4 observations do not cover target date",
            )
        return CheckResult.success(
            "system_b_theme_rank_m4_freshness",
            target_date=target.isoformat(),
            m4_row_count=count,
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "system_b_theme_rank_m4_freshness",
            "THEME_RANK_M4_INPUT_STALE",
            "M4 observations could not be read",
            exception=type(exc).__name__,
        )


def _popularity_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        if not is_open:
            return CheckResult.success(
                "system_b_theme_rank_popularity_freshness",
                target_date=target.isoformat(),
                noop=True,
            )
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = connection.execute(
                f"SELECT source, source_status FROM {POPULARITY_SOURCE_AVAILABILITY_TABLE} WHERE trade_date=?",
                [target],
            ).fetchall()
        finally:
            connection.close()
        sources = {str(row[0]): str(row[1]).upper() for row in rows}
        required = {"dc_hot", "ths_hot"}
        missing = required - set(sources)
        if missing:
            return CheckResult.failure(
                "system_b_theme_rank_popularity_freshness",
                "THEME_RANK_POPULARITY_AVAILABILITY_MISSING",
                f"missing availability for {sorted(missing)}",
            )
        invalid = {s: st for s, st in sources.items() if s in required and st not in {"AVAILABLE", "UNAVAILABLE"}}
        if invalid:
            return CheckResult.failure(
                "system_b_theme_rank_popularity_freshness",
                "THEME_RANK_POPULARITY_AVAILABILITY_INVALID",
                f"invalid popularity availability status: {invalid}",
            )
        return CheckResult.success(
            "system_b_theme_rank_popularity_freshness",
            target_date=target.isoformat(),
            sources=sources,
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "system_b_theme_rank_popularity_freshness",
            "THEME_RANK_POPULARITY_AVAILABILITY_MISSING",
            "popularity availability could not be read",
            exception=type(exc).__name__,
        )


def _snapshot_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
    try:
        rows = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE} WHERE trade_date=?",
                [target],
            ).fetchone()[0]
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "system_b_theme_rank_snapshot_completion",
            "THEME_RANK_SNAPSHOT_COMPLETION_MISSING",
            "snapshot output could not be read",
            exception=type(exc).__name__,
        )
    finally:
        connection.close()
    return CheckResult.success(
        "system_b_theme_rank_snapshot_completion",
        actual_rows=rows,
        target_date=target.isoformat(),
    )


def _audit_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
    try:
        rows = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=?",
                [target],
            ).fetchone()[0]
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "system_b_theme_rank_audit_completion",
            "THEME_RANK_AUDIT_COMPLETION_MISSING",
            "component audit output could not be read",
            exception=type(exc).__name__,
        )
    finally:
        connection.close()
    return CheckResult.success(
        "system_b_theme_rank_audit_completion",
        actual_rows=rows,
        target_date=target.isoformat(),
    )


def _quality(table_name: str, check_id: str, key_columns: tuple[str, ...], error_code: str):
    def checker(context: PipelineRunContext) -> CheckResult:
        target = _target(context)
        group_by = ", ".join(key_columns)
        try:
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                duplicates = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM (SELECT {group_by}, COUNT(*) c FROM {table_name} WHERE trade_date=? GROUP BY {group_by} HAVING COUNT(*)>1)",
                        [target],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:  # noqa: BLE001
            return CheckResult.failure(check_id, error_code, "output uniqueness could not be checked", exception=type(exc).__name__)
        if duplicates:
            return CheckResult.failure(check_id, error_code, "duplicate output keys exist", duplicate_groups=duplicates)
        return CheckResult.success(check_id, target_date=target.isoformat(), duplicate_groups=0)

    checker.__name__ = check_id
    return checker


def _execute(context: PipelineRunContext) -> BusinessExecution:
    target = _target(context)
    try:
        report = run_theme_rank_daily(
            quant_database=Path(context.settings.paths.duckdb_path),
            trade_date=target,
            production_run_id=context.run_id,
            execution_control=context.execution_control,
        )
    except SystemBThemeRankProductionError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except Exception as exc:
        raise ContractError("THEME_RANK_EXECUTION_FAILED", type(exc).__name__) from exc

    if report["status"] == "NOOP":
        return BusinessExecution.noop(report["reason"], metrics=PipelineMetrics(dates_processed=1))

    diagnostics = tuple(
        PipelineDiagnostic(
            code=code,
            level=DiagnosticLevel.WARNING,
            message=f"popularity source condition: {code}",
        )
        for code in report.get("diagnostics", [])
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=report["theme_count"],
            rows_written=report["rows_written"],
            assets_processed=report["theme_count"],
            dates_processed=1,
            batches=1,
        ),
        outputs=(
            OutputResult(
                SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE,
                report["snapshot_rows"],
                "settings.paths.duckdb_path",
                True,
                {"target_date": target.isoformat()},
            ),
            OutputResult(
                SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE,
                report["component_audit_rows"],
                "settings.paths.duckdb_path",
                True,
                {"target_date": target.isoformat()},
            ),
        ),
        diagnostics=diagnostics,
    )


SYSTEM_B_THEME_RANK_PRODUCTION = register_pipeline(
    PipelineContract(
        pipeline_id="system_b_theme_rank_daily",
        name="System B Task06-B Theme Trend Rank",
        description="Calculates and atomically publishes Theme Trend Rank snapshots and component audits for one trading date.",
        contract_version="0.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute,
        target_date_policy=SYSTEM_B_THEME_RANK_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            InputContract(
                "theme_rank_quant_facts",
                InputKind.TABLE,
                "quant_db.trading_calendar,popularity_source_availability,theme_m4_observation",
                tuple(_THEME_RANK_TABLES),
                "target-date canonical Theme facts and popularity availability",
                "THEME_RANK_QUANT_INPUT_STRUCTURE_MISSING",
                _quant_structure,
                FreshnessContract(
                    "system_b_theme_rank_quant_freshness",
                    "target-date calendar is resolved before calculation",
                    0,
                    NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    "THEME_RANK_QUANT_INPUT_STALE",
                    _quant_freshness,
                ),
            ),
            InputContract(
                "theme_rank_m4",
                InputKind.UPSTREAM_PIPELINE,
                f"quant_db.{THEME_M4_OBSERVATION_TABLE}",
                tuple(THEME_M4_OBSERVATION.column_names()),
                "finalized M4 Theme observations on target date",
                "THEME_RANK_M4_INPUT_STRUCTURE_MISSING",
                _quant_structure,
                FreshnessContract(
                    "system_b_theme_rank_m4_freshness",
                    "M4 observations cover target date",
                    0,
                    NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    "THEME_RANK_M4_INPUT_STALE",
                    _m4_freshness,
                ),
                "theme_m4_production",
            ),
            InputContract(
                "theme_rank_popularity_availability",
                InputKind.TABLE,
                "quant_db.popularity_source_availability with dc_hot/ths_hot source rows",
                tuple(POPULARITY_SOURCE_AVAILABILITY.column_names()),
                "explicit AVAILABLE or expected UNAVAILABLE source facts",
                "THEME_RANK_POPULARITY_AVAILABILITY_MISSING",
                _quant_structure,
                FreshnessContract(
                    "system_b_theme_rank_popularity_freshness",
                    "availability checked without treating expected UNAVAILABLE as failure",
                    0,
                    NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    "THEME_RANK_POPULARITY_AVAILABILITY_MISSING",
                    _popularity_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE,
                QUANT_DB_RESOURCE,
                "settings.paths.duckdb_path",
                SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE,
                SYSTEM_B_THEME_RANK_SNAPSHOT.primary_key,
                WriteMode.REPLACE_TARGET_DATE,
                "one row per target-date canonical Theme in C_D",
                CompletionContract(
                    "canonical target-date Theme Rank snapshot is queryable",
                    "THEME_RANK_SNAPSHOT_COMPLETION_MISSING",
                    _snapshot_completion,
                ),
                (
                    _quality(
                        SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE,
                        "theme_rank_snapshot_unique",
                        SYSTEM_B_THEME_RANK_SNAPSHOT.primary_key,
                        "THEME_RANK_SNAPSHOT_DUPLICATE_KEY",
                    ),
                ),
                False,
            ),
            OutputContract(
                SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE,
                QUANT_DB_RESOURCE,
                "settings.paths.duckdb_path",
                SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE,
                SYSTEM_B_THEME_RANK_COMPONENT_AUDIT.primary_key,
                WriteMode.REPLACE_TARGET_DATE,
                "seven audited components per eligible Theme in U_D",
                CompletionContract(
                    "component audit is queryable after snapshot commit",
                    "THEME_RANK_AUDIT_COMPLETION_MISSING",
                    _audit_completion,
                ),
                (
                    _quality(
                        SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE,
                        "theme_rank_audit_unique",
                        SYSTEM_B_THEME_RANK_COMPONENT_AUDIT.primary_key,
                        "THEME_RANK_AUDIT_DUPLICATE_KEY",
                    ),
                ),
                False,
            ),
        ),
        dependencies=("theme_m4_production", "dc_hot_ingest", "ths_hot_ingest"),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(
            *(f"duckdb://{QUANT_DB_RESOURCE}#{table}" for table in _THEME_RANK_TABLES),
            f"duckdb://{QUANT_DB_RESOURCE}#{SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE}",
            f"duckdb://{QUANT_DB_RESOURCE}#{SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE}",
        ),
        idempotency=IdempotencyContract(
            "system_b_theme_rank_snapshot.trade_date,theme_id",
            "same target date replaces only that date and creates a new run provenance",
            "prior snapshot and audit remain visible until one transaction commits",
            "rerun explicitly after upstream corrections; no automatic mutation",
            False,
            "single quant.db transaction around snapshot and component-audit target replacement",
        ),
        transaction=TransactionContract(
            TransactionMode.DATABASE_TRANSACTION,
            "one quant.db target-date replacement for snapshot and component audit",
            "failed write rolls back both output tables",
        ),
        execution=ExecutionPolicy(OverlapPolicy.FORBID, 1),
        performance=PerformanceBudget(
            1800.0,
            1200.0,
            2400,
            "canonical Theme universe, M4 facts, Episode and popularity availability facts",
            "Task06-B offline acceptance benchmark",
        ),
        manual_execution_allowed=True,
    )
)

SYSTEM_B_THEME_RANK_PRODUCTION_CONTRACT = SYSTEM_B_THEME_RANK_PRODUCTION
SYSTEM_B_THEME_RANK_CONTRACTS = (SYSTEM_B_THEME_RANK_PRODUCTION,)

__all__ = [
    "SYSTEM_B_THEME_RANK_CONTRACTS",
    "SYSTEM_B_THEME_RANK_PRODUCTION",
    "SYSTEM_B_THEME_RANK_PRODUCTION_CONTRACT",
    "SYSTEM_B_THEME_RANK_TARGET_DATE_POLICY",
]
