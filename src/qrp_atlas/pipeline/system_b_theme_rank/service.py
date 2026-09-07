"""Production service for Task06-B System B Theme Trend Rank.

This service manages point-in-time input validation, deterministic M5 input
fingerprint verification, and one atomic quant.db target-date replacement.
All calculation formulas live in
``qrp_atlas.indicators.system_b.theme_ranking`` and are testable without
database side effects.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    COLLECTION_ID,
    CREATED_AT,
    DC_HOT,
    INPUT_SNAPSHOT_ID,
    POPULARITY_AVAILABLE,
    POPULARITY_SOURCE_AVAILABILITY,
    POPULARITY_SOURCE_AVAILABILITY_TABLE,
    POPULARITY_UNAVAILABLE,
    PRODUCTION_RUN_ID,
    RANK_ELIGIBLE,
    SYSTEM_B_THEME_RANK_CALCULATION_VERSION,
    SYSTEM_B_THEME_RANK_COMPONENT_AUDIT,
    SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE,
    SYSTEM_B_THEME_RANK_SNAPSHOT,
    SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_ID,
    THEME_M4_OBSERVATION_TABLE,
    THEME_M5_OBSERVATION_TABLE,
    THEME_M5_OBSERVATION_VERSION,
    THS_HOT,
    TRADE_DATE,
    TRADING_CALENDAR,
)
from qrp_atlas.indicators.system_b.theme_ranking import (
    ThemeRankingError,
    ThemeRankingResult,
    calculate_theme_ranking,
)
from qrp_atlas.pipeline.theme.m5_service import ThemeM5PipelineService
from qrp_atlas.pipeline.theme.service import ThemePipelineService


class SystemBThemeRankProductionError(RuntimeError):
    """Stable production-boundary error with a machine-readable code."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


def _tables(connection: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }


def _columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=?",
            [table_name],
        ).fetchall()
    }


def ensure_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """Create Task06-B Theme Rank output tables if absent and assert columns."""
    connection.execute(SYSTEM_B_THEME_RANK_SNAPSHOT.duckdb_create_sql())
    connection.execute(SYSTEM_B_THEME_RANK_COMPONENT_AUDIT.duckdb_create_sql())

    for schema, code in (
        (SYSTEM_B_THEME_RANK_SNAPSHOT, "THEME_RANK_SCHEMA_RECREATION_REQUIRED"),
        (SYSTEM_B_THEME_RANK_COMPONENT_AUDIT, "THEME_RANK_SCHEMA_RECREATION_REQUIRED"),
    ):
        actual = _columns(connection, schema.name)
        expected = set(schema.column_names())
        if not expected <= actual:
            raise SystemBThemeRankProductionError(code, f"{schema.name}: missing {sorted(expected - actual)}")


def _persist(
    connection: duckdb.DuckDBPyConnection,
    snapshot: pd.DataFrame,
    audit: pd.DataFrame,
    target: date,
) -> None:
    """Atomically replace target-date snapshot and component audit in one transaction."""
    snapshot_columns = ", ".join(SYSTEM_B_THEME_RANK_SNAPSHOT.column_names())
    audit_columns = ", ".join(SYSTEM_B_THEME_RANK_COMPONENT_AUDIT.column_names())
    registered_snapshot = False
    registered_audit = False

    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            f"DELETE FROM {SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE} WHERE trade_date=?",
            [target],
        )
        connection.execute(
            f"DELETE FROM {SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=?",
            [target],
        )

        if not snapshot.empty:
            connection.register("_theme_rank_snapshot_rows", snapshot)
            registered_snapshot = True
            connection.execute(
                f"INSERT INTO {SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE} ({snapshot_columns}) SELECT {snapshot_columns} FROM _theme_rank_snapshot_rows"
            )

        if not audit.empty:
            connection.register("_theme_rank_audit_rows", audit)
            registered_audit = True
            connection.execute(
                f"INSERT INTO {SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE} ({audit_columns}) SELECT {audit_columns} FROM _theme_rank_audit_rows"
            )

        # Assert completion
        check = connection.execute(
            f"SELECT COUNT(*) FROM {SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE} WHERE trade_date=?",
            [target],
        ).fetchone()
        if not check or int(check[0]) != len(snapshot):
            raise SystemBThemeRankProductionError("THEME_RANK_COMPLETION_CHECK_FAILED")

        audit_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=?",
                [target],
            ).fetchone()[0]
        )
        if audit_count != len(audit):
            raise SystemBThemeRankProductionError("THEME_RANK_AUDIT_COMPLETION_CHECK_FAILED")

        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        if registered_snapshot:
            try:
                connection.unregister("_theme_rank_snapshot_rows")
            except Exception:
                pass
        if registered_audit:
            try:
                connection.unregister("_theme_rank_audit_rows")
            except Exception:
                pass


def _connect(database: duckdb.DuckDBPyConnection | Path | str) -> tuple[duckdb.DuckDBPyConnection, bool]:
    if isinstance(database, duckdb.DuckDBPyConnection):
        return database, False
    con = duckdb.connect(str(database))
    return con, True


def run_theme_rank_daily(
    *,
    quant_database: duckdb.DuckDBPyConnection | Path | str,
    trade_date: date | str,
    production_run_id: str | None = None,
    execution_control: Any | None = None,
) -> dict[str, Any]:
    """Execute Task06-B Theme Rank daily calculation and atomic persistence."""
    target = pd.to_datetime(trade_date).date() if isinstance(trade_date, str) else trade_date
    run_id = production_run_id or f"system_b_theme_rank_{uuid4().hex}"
    now_ts = datetime.now(UTC)

    connection, should_close = _connect(quant_database)
    try:
        ensure_schema(connection)

        if execution_control is not None:
            execution_control.check()

        # Check trading calendar
        cal_row = connection.execute(
            f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date=?",
            [target],
        ).fetchone()
        if not cal_row:
            raise SystemBThemeRankProductionError("CALENDAR_DATE_MISSING", str(target))
        if not cal_row[0]:
            return {
                "status": "NOOP",
                "trade_date": target,
                "reason": "NON_TRADING_DAY",
                "theme_count": 0,
                "snapshot_rows": 0,
                "component_audit_rows": 0,
                "rows_written": 0,
                "diagnostics": (),
            }

        # Query trading calendar open days up to target date
        open_days_rows = connection.execute(
            f"SELECT trade_date FROM {TRADING_CALENDAR.name} WHERE is_open=true AND trade_date<=? ORDER BY trade_date",
            [target],
        ).fetchall()
        open_trading_days = [row[0] for row in open_days_rows]

        # Query canonical themes for target date
        theme_service = ThemePipelineService(connection)
        canonical_themes = theme_service._fetch_all_canonical_themes(target)
        if not canonical_themes:
            # Check if M4 exists
            m4_exists = connection.execute(
                f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date=?",
                [target],
            ).fetchone()[0] > 0
            if not m4_exists:
                raise SystemBThemeRankProductionError("THEME_M4_NOT_FINALIZED", f"No canonical themes and no M4 observations on {target}")
            m4_tuples = connection.execute(
                f"SELECT DISTINCT {THEME_ID}, {COLLECTION_ID} FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date=?",
                [target],
            ).fetchall()
            canonical_themes = [(str(row[0]), str(row[1])) for row in m4_tuples]

        canonical_ids = [t[0] for t in canonical_themes]

        # Load M4 observations
        m4_df = connection.execute(
            f"SELECT * FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date=?",
            [target],
        ).fetchdf()

        if m4_df.empty and canonical_ids:
            raise SystemBThemeRankProductionError("THEME_M4_NOT_FINALIZED", f"M4 observation missing on {target}")

        # Check all canonical themes are in M4
        m4_themes = set(m4_df[THEME_ID]) if not m4_df.empty else set()
        missing_m4 = set(canonical_ids) - m4_themes
        if missing_m4:
            raise SystemBThemeRankProductionError(
                "THEME_M4_INCOMPLETE",
                f"Canonical themes missing in M4 on {target}: {sorted(missing_m4)[:5]}",
            )

        if execution_control is not None:
            execution_control.check()

        # Load Popularity Source Availability for target date
        tables = _tables(connection)
        if POPULARITY_SOURCE_AVAILABILITY_TABLE not in tables:
            raise SystemBThemeRankProductionError("POPULARITY_SOURCE_AVAILABILITY_TABLE_MISSING")

        avail_df = connection.execute(
            f"SELECT source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance "
            f"FROM {POPULARITY_SOURCE_AVAILABILITY_TABLE} WHERE trade_date=?",
            [target],
        ).fetchdf()

        avail_by_source: dict[str, dict[str, Any]] = {}
        for _, row in avail_df.iterrows():
            avail_by_source[str(row["source"])] = dict(row)

        for req_src in ("dc_hot", "ths_hot"):
            if req_src not in avail_by_source:
                raise SystemBThemeRankProductionError(
                    "POPULARITY_AVAILABILITY_MISSING",
                    f"source {req_src} availability row missing on {target}",
                )

        is_pop_unavailable = any(
            str(avail_by_source[s].get("source_status", "")).upper() == POPULARITY_UNAVAILABLE
            for s in ("dc_hot", "ths_hot")
        )

        # Path A vs Path B
        m5_df: pd.DataFrame | None = None
        if not is_pop_unavailable:
            # Path B: Must verify M5 completed and input_snapshot_id match
            if THEME_M5_OBSERVATION_TABLE not in tables:
                raise SystemBThemeRankProductionError("THEME_M5_OBSERVATION_TABLE_MISSING")

            m5_df = connection.execute(
                f"SELECT * FROM {THEME_M5_OBSERVATION_TABLE} WHERE trade_date=?",
                [target],
            ).fetchdf()

            if m5_df.empty and canonical_ids:
                raise SystemBThemeRankProductionError("THEME_M5_NOT_FINALIZED", f"M5 observations missing on {target}")

            m5_themes = set(m5_df[THEME_ID]) if not m5_df.empty else set()
            missing_m5 = set(canonical_ids) - m5_themes
            if missing_m5:
                raise SystemBThemeRankProductionError(
                    "THEME_M5_INCOMPLETE",
                    f"Canonical themes missing in M5 on {target}: {sorted(missing_m5)[:5]}",
                )

            # Check calculation_version
            m5_versions = set(m5_df[CALCULATION_VERSION])
            if not m5_versions <= {THEME_M5_OBSERVATION_VERSION}:
                raise SystemBThemeRankProductionError(
                    "THEME_M5_CALCULATION_VERSION_INVALID",
                    f"unexpected M5 version {m5_versions}",
                )

            # Check all rows share single input_snapshot_id
            snapshot_ids = set(m5_df[INPUT_SNAPSHOT_ID])
            if len(snapshot_ids) != 1:
                raise SystemBThemeRankProductionError(
                    "THEME_M5_INPUT_SNAPSHOT_ID_NON_UNIQUE",
                    f"M5 contains multiple input_snapshot_ids: {snapshot_ids}",
                )
            persisted_snapshot_id = snapshot_ids.pop()

            # Recompute deterministic M5 fingerprint
            try:
                fresh_m5_facts = ThemeM5PipelineService(connection).calculate_m5_facts(
                    target,
                    execution_control=execution_control,
                )
            except Exception as exc:
                raise SystemBThemeRankProductionError(
                    "THEME_M5_RECOMPUTATION_FAILED",
                    f"Failed to calculate M5 deterministic fingerprint: {exc}",
                ) from exc

            if persisted_snapshot_id != fresh_m5_facts.input_snapshot_id:
                raise SystemBThemeRankProductionError(
                    "THEME_M5_INPUT_SNAPSHOT_ID_MISMATCH",
                    f"persisted {persisted_snapshot_id} != current source facts recomputed {fresh_m5_facts.input_snapshot_id}",
                )

        if execution_control is not None:
            execution_control.check()

        # Load Episodes pointed to by M4
        episode_ids = [
            str(eid).strip()
            for eid in m4_df["custom_index_episode_id"].dropna().unique()
            if str(eid).strip() != ""
        ]
        if episode_ids:
            # Load episodes
            episodes_df = connection.execute(
                f"SELECT * FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE} WHERE episode_id IN (SELECT UNNEST(?))",
                [episode_ids],
            ).fetchdf()
        else:
            episodes_df = pd.DataFrame()

        # Load Index Daily and State for relevant dates
        # Get min start date among episodes
        min_start_date = target
        if not episodes_df.empty:
            for d in episodes_df["episode_start_date"]:
                parsed = pd.to_datetime(d).date()
                if parsed < min_start_date:
                    min_start_date = parsed

        index_daily_df = connection.execute(
            f"SELECT theme_id, collection_id, trade_date, index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} "
            f"WHERE trade_date >= ? AND trade_date <= ?",
            [min_start_date, target],
        ).fetchdf()

        states_df = connection.execute(
            f"SELECT theme_id, collection_id, trade_date, is_above_or_equal_ma5 FROM {THEME_CUSTOM_INDEX_STATE_TABLE} "
            f"WHERE trade_date >= ? AND trade_date <= ?",
            [min_start_date, target],
        ).fetchdf()

        if execution_control is not None:
            execution_control.check()

        # Execute pure calculation
        try:
            result: ThemeRankingResult = calculate_theme_ranking(
                canonical_themes=canonical_themes,
                trade_date=target,
                m4_observations=m4_df,
                episodes=episodes_df,
                index_daily=index_daily_df,
                states=states_df,
                trading_calendar_open_days=open_trading_days,
                popularity_availability=avail_by_source,
                m5_observations=m5_df,
                production_run_id=run_id,
                created_at=now_ts,
            )
        except ThemeRankingError as exc:
            raise SystemBThemeRankProductionError(exc.code, exc.detail) from exc

        # Persist atomic replacement
        _persist(connection, result.snapshot, result.component_audit, target)

        return {
            "status": "COMPLETED",
            "trade_date": target,
            "theme_count": len(canonical_themes),
            "u_d_size": len(result.snapshot[result.snapshot[RANK_ELIGIBLE]]),
            "snapshot_rows": len(result.snapshot),
            "component_audit_rows": len(result.component_audit),
            "rows_written": len(result.snapshot) + len(result.component_audit),
            "run_status": result.run_status,
            "diagnostics": list(result.diagnostics),
            "production_run_id": run_id,
        }
    finally:
        if should_close:
            connection.close()


def get_theme_rank_snapshot(
    quant_database: duckdb.DuckDBPyConnection | Path | str,
    trade_date: date | str,
) -> pd.DataFrame:
    """Query persisted Theme Rank snapshot for trade_date."""
    target = pd.to_datetime(trade_date).date() if isinstance(trade_date, str) else trade_date
    con, should_close = _connect(quant_database)
    try:
        return con.execute(
            f"SELECT * FROM {SYSTEM_B_THEME_RANK_SNAPSHOT_TABLE} WHERE trade_date=? ORDER BY theme_id",
            [target],
        ).fetchdf()
    finally:
        if should_close:
            con.close()


def get_theme_rank_component_audit(
    quant_database: duckdb.DuckDBPyConnection | Path | str,
    trade_date: date | str,
) -> pd.DataFrame:
    """Query persisted Theme Rank component audit for trade_date."""
    target = pd.to_datetime(trade_date).date() if isinstance(trade_date, str) else trade_date
    con, should_close = _connect(quant_database)
    try:
        return con.execute(
            f"SELECT * FROM {SYSTEM_B_THEME_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=? ORDER BY theme_id, dimension, component",
            [target],
        ).fetchdf()
    finally:
        if should_close:
            con.close()
