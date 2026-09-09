"""Unified runner and dispatch harness for back() (Task08)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
import hashlib
import json
import os
import subprocess
from typing import Any
import uuid

import pandas as pd

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine
from qrp_atlas.backtest.portfolio.models import (
    PortfolioBacktestConfig,
    PortfolioExecutionRule,
)
from qrp_atlas.backtest.research.pipeline import run_cross_section_research
from qrp_atlas.contracts import ASSET_ID, TICKER, TRADE_DATE

from .experiment import evaluate_experiment_rules
from .models import (
    ALLOWED_UNIVERSE_PRESETS,
    BackRequest,
    BackResult,
    ExecutionSpec,
    ExperimentSpec,
    FactorSpec,
    HarnessError,
    HarnessValidationError,
    StrategySpec,
    SubjectType,
)
from .strategy_driver import run_formal_strategy


def _get_git_sha() -> str:
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
        if res.returncode == 0:
            return res.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _normalize_date_str(val: Any) -> str:
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m-%d")
    return pd.Timestamp(val).strftime("%Y-%m-%d")


def _filter_by_universe_and_period(
    df: pd.DataFrame,
    universe: str | Sequence[str] | None,
    period: tuple[str, str],
) -> pd.DataFrame:
    """Filter DataFrame by period [start, end] and universe tickers if given."""
    if df.empty:
        return df.copy()
    out = df.copy()
    if TRADE_DATE in out.columns:
        out[TRADE_DATE] = out[TRADE_DATE].apply(_normalize_date_str)
        out = out[(out[TRADE_DATE] >= period[0]) & (out[TRADE_DATE] <= period[1])]

    id_col = ASSET_ID if ASSET_ID in out.columns else (TICKER if TICKER in out.columns else None)
    if id_col and isinstance(universe, (tuple, list, set, frozenset)):
        allowed = {str(t).strip() for t in universe}
        out = out[out[id_col].astype(str).str.strip().isin(allowed)]

    return out.reset_index(drop=True)


def _resolve_execution_config(
    exec_spec: ExecutionSpec,
    subject_type: SubjectType,
    subject: FactorSpec | ExperimentSpec | StrategySpec,
    run_id: str,
) -> tuple[PortfolioBacktestConfig, dict[str, Any]]:
    """Resolve preset defaults, subject semantics, and explicit overrides with conflict checks."""
    provenance_sources: dict[str, str] = {}

    # 1. Preset defaults for a_share_daily
    if exec_spec.preset == "a_share_daily":
        initial_cash = 1_000_000.0
        max_positions = 10
        max_weight = 0.20
        price_field = "close"
        lot_size = 100
        min_commission = 5.0
        enforce_t_plus_one = True
        enforce_price_limits = True
        enforce_suspension = True
        commission_rate = 0.0003
        stamp_tax_rate = 0.0005
        slippage_bps = 10.0
    else:
        raise HarnessValidationError(f"Unknown execution preset: {exec_spec.preset!r}")

    for k in (
        "initial_cash", "max_positions", "max_weight_per_asset", "price_field",
        "lot_size", "minimum_commission", "commission_rate", "stamp_tax_rate", "slippage_bps"
    ):
        provenance_sources[k] = f"preset:{exec_spec.preset}"

    # 2. Subject semantics
    if subject_type == SubjectType.EXPERIMENT and isinstance(subject, ExperimentSpec):
        top_n = int(subject.portfolio.get("top_n", 6))
        max_positions = top_n
        provenance_sources["max_positions"] = "subject:experiment.portfolio.top_n"

        weight_each = subject.portfolio.get("weight_each")
        if weight_each is not None:
            max_weight = float(weight_each)
            provenance_sources["max_weight_per_asset"] = "subject:experiment.portfolio.weight_each"
        else:
            max_weight = 1.0 / top_n
            provenance_sources["max_weight_per_asset"] = "subject:experiment.portfolio.equal_weight"

    elif subject_type == SubjectType.STRATEGY and isinstance(subject, StrategySpec):
        if subject.code == "system_b_portfolio":
            max_positions = 6
            max_weight = 0.25
            provenance_sources["max_positions"] = "subject:system_b_portfolio.fixed_cap"
            provenance_sources["max_weight_per_asset"] = "subject:system_b_portfolio.fixed_cap"

    # 3. Explicit overrides & conflict resolution
    if exec_spec.initial_cash is not None:
        initial_cash = exec_spec.initial_cash
        provenance_sources["initial_cash"] = "explicit:override"

    if exec_spec.max_positions is not None:
        # Conflict check
        if subject_type == SubjectType.EXPERIMENT and isinstance(subject, ExperimentSpec):
            req_top_n = int(subject.portfolio.get("top_n", 6))
            if exec_spec.max_positions < req_top_n:
                raise HarnessValidationError(
                    f"Conflict: execution.max_positions ({exec_spec.max_positions}) cannot accommodate "
                    f"experiment portfolio.top_n ({req_top_n})"
                )
        max_positions = exec_spec.max_positions
        provenance_sources["max_positions"] = "explicit:override"

    if exec_spec.max_weight_per_asset is not None:
        if subject_type == SubjectType.EXPERIMENT and isinstance(subject, ExperimentSpec):
            req_weight = subject.portfolio.get("weight_each")
            if req_weight is not None and exec_spec.max_weight_per_asset < float(req_weight):
                raise HarnessValidationError(
                    f"Conflict: execution.max_weight_per_asset ({exec_spec.max_weight_per_asset}) is smaller than "
                    f"experiment portfolio.weight_each ({req_weight})"
                )
        max_weight = exec_spec.max_weight_per_asset
        provenance_sources["max_weight_per_asset"] = "explicit:override"

    if exec_spec.price_field is not None:
        price_field = exec_spec.price_field
        provenance_sources["price_field"] = "explicit:override"

    if exec_spec.commission_rate is not None:
        commission_rate = exec_spec.commission_rate
        provenance_sources["commission_rate"] = "explicit:override"

    if exec_spec.stamp_tax_rate is not None:
        stamp_tax_rate = exec_spec.stamp_tax_rate
        provenance_sources["stamp_tax_rate"] = "explicit:override"

    if exec_spec.slippage_rate is not None:
        slippage_bps = exec_spec.slippage_rate * 10000.0
        provenance_sources["slippage_bps"] = "explicit:override"

    config = PortfolioBacktestConfig(
        name=f"harness_{run_id[:8]}",
        initial_cash=initial_cash,
        max_positions=max_positions,
        max_weight_per_asset=max_weight,
        cost=CostRule(
            commission_rate=commission_rate,
            stamp_tax_rate=stamp_tax_rate,
            slippage_bps=slippage_bps,
        ),
        execution=PortfolioExecutionRule(
            price_field=price_field,
            mark_price_field=price_field,
            lot_size=lot_size,
            minimum_commission=min_commission,
            enforce_t_plus_one=enforce_t_plus_one,
            enforce_price_limits=enforce_price_limits,
            enforce_suspension=enforce_suspension,
        ),
    )

    resolved_summary = {
        "initial_cash": initial_cash,
        "max_positions": max_positions,
        "max_weight_per_asset": max_weight,
        "price_field": price_field,
        "commission_rate": commission_rate,
        "stamp_tax_rate": stamp_tax_rate,
        "slippage_bps": slippage_bps,
        "sources": provenance_sources,
    }

    return config, resolved_summary


def back(
    period: tuple[str, str],
    universe: str | Sequence[str] | None = None,
    *,
    factor: FactorSpec | Mapping[str, Any] | None = None,
    experiment: ExperimentSpec | Mapping[str, Any] | None = None,
    strategy: StrategySpec | Mapping[str, Any] | None = None,
    execution: ExecutionSpec | Mapping[str, Any] | None = None,
    output: Mapping[str, Any] | None = None,
    data: Mapping[str, pd.DataFrame] | None = None,
) -> BackResult:
    """Unified entry point for factor research, experiment, and strategy backtests (Task08)."""

    # 1. Determine subject type
    subjects_provided = [
        (SubjectType.FACTOR, factor),
        (SubjectType.EXPERIMENT, experiment),
        (SubjectType.STRATEGY, strategy),
    ]
    provided = [(st, val) for st, val in subjects_provided if val is not None]
    if len(provided) != 1:
        raise HarnessValidationError(
            f"Exactly one of factor, experiment, or strategy must be provided, got: {[p[0] for p in provided]}"
        )
    subject_type, raw_subject = provided[0]

    # Normalize specs
    factor_spec: FactorSpec | None = None
    experiment_spec: ExperimentSpec | None = None
    strategy_spec: StrategySpec | None = None

    if subject_type == SubjectType.FACTOR:
        factor_spec = FactorSpec.from_input(raw_subject)
    elif subject_type == SubjectType.EXPERIMENT:
        experiment_spec = ExperimentSpec.from_input(raw_subject)
    else:
        strategy_spec = StrategySpec.from_input(raw_subject)

    exec_spec = ExecutionSpec.from_input(execution)

    request = BackRequest(
        period=period,
        universe=universe,
        subject_type=subject_type,
        factor=factor_spec,
        experiment=experiment_spec,
        strategy=strategy_spec,
        execution=exec_spec,
        output=output or {},
        data=data,
    )

    run_id = str(uuid.uuid4())
    config_hash = request.compute_config_hash()
    git_sha = _get_git_sha()
    warnings: list[str] = []

    # 2. Resolve Execution Config
    portfolio_config, resolved_exec = _resolve_execution_config(
        exec_spec, subject_type, request.subject, run_id
    )

    # 3. Data Resolution
    price_df: pd.DataFrame
    factor_df: pd.DataFrame | None = None
    facts_df: pd.DataFrame | None = None

    if request.data is not None:
        data_source = "in_memory_injection"
        price_df = request.data.get("prices", pd.DataFrame())
        if price_df.empty:
            raise HarnessValidationError("Injected data mapping must include non-empty 'prices'")
        price_df = _filter_by_universe_and_period(price_df, request.universe, request.period)

        if subject_type in {SubjectType.FACTOR, SubjectType.EXPERIMENT}:
            factor_df = request.data.get("factors")
            if factor_df is None or factor_df.empty:
                # Fallback: check if factor fields are inside price_df
                factor_df = price_df
            else:
                factor_df = _filter_by_universe_and_period(factor_df, request.universe, request.period)

        if subject_type == SubjectType.STRATEGY:
            facts_df = request.data.get("facts")
            if facts_df is not None and not facts_df.empty:
                facts_df = _filter_by_universe_and_period(facts_df, request.universe, request.period)
    else:
        data_source = "database_pit"
        from qrp_atlas.backtest.data import load_stock_prices

        # Resolve universe
        asset_ids: list[str] | None = None
        if isinstance(request.universe, (tuple, list)):
            asset_ids = [str(t).strip() for t in request.universe]
        elif request.universe == "system_b_active_pools":
            raise HarnessValidationError(
                "Universe preset 'system_b_active_pools' is disabled to prevent PIT universe leakage. "
                "Use an explicit ticker list or 'all_a'."
            )

        price_df = load_stock_prices(
            start_date=request.period[0],
            end_date=request.period[1],
            tickers=asset_ids,
        )
        if price_df.empty:
            raise HarnessError(f"No price data loaded for period {request.period} and universe {request.universe}")

        if subject_type in {SubjectType.FACTOR, SubjectType.EXPERIMENT}:
            # For factors, check if field exists in price_df (or query daily_basic)
            field_name = factor_spec.field if factor_spec else (
                experiment_spec.score if isinstance(experiment_spec.score, str) else list(experiment_spec.score.keys())[0]
            )
            if field_name in price_df.columns:
                factor_df = price_df
            else:
                # Query daily_basic from DuckDB
                from qrp_atlas.config.settings import get_settings
                import duckdb
                con = duckdb.connect(str(get_settings().db_path), read_only=True)
                try:
                    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
                    if "daily_basic" in tables:
                        query = f"SELECT * FROM daily_basic WHERE trade_date >= '{request.period[0]}' AND trade_date <= '{request.period[1]}'"
                        if asset_ids:
                            quoted = ", ".join(f"'{a}'" for a in asset_ids)
                            query += f" AND ticker IN ({quoted})"
                        factor_df = con.execute(query).fetchdf()
                        if "ticker" in factor_df.columns and ASSET_ID not in factor_df.columns:
                            factor_df[ASSET_ID] = factor_df["ticker"]
                    else:
                        factor_df = price_df
                finally:
                    con.close()

    trading_days = sorted({_normalize_date_str(d) for d in price_df[TRADE_DATE].unique()})

    # 4. Dispatch Subject Execution
    factor_result: dict[str, Any] | None = None
    portfolio_result: dict[str, Any] | None = None
    replay_result: dict[str, Any] | None = None
    summary_metrics: dict[str, Any] = {}

    if subject_type == SubjectType.FACTOR:
        assert factor_spec is not None
        assert factor_df is not None
        research_res = run_cross_section_research(
            factor_frame=factor_df,
            price_df=price_df,
            trading_days=trading_days,
            factor_columns=[factor_spec.field],
            strategy_code=None,
            horizons=factor_spec.horizons,
            n_groups=factor_spec.quantiles,
            run_portfolio=False,
        )
        factor_result = {
            "daily_ic": research_res.daily_ic.to_dict(orient="list"),
            "ic_summary": research_res.ic_summary.to_dict(orient="list"),
            "group_returns": research_res.group_returns.to_dict(orient="list"),
            "group_spreads": research_res.group_spreads.to_dict(orient="list"),
            "forward_returns": research_res.forward_returns.to_dict(orient="list"),
            "diagnostics": list(research_res.diagnostics),
        }
        # Extract summary metrics from ic_summary
        ic_sum_df = research_res.ic_summary
        if not ic_sum_df.empty:
            for _, row in ic_sum_df.iterrows():
                horizon = row.get("horizon", "N/A")
                summary_metrics[f"ic_mean_{horizon}"] = row.get("mean_ic")
                summary_metrics[f"ic_ir_{horizon}"] = row.get("ic_ir")
                summary_metrics[f"rank_ic_mean_{horizon}"] = row.get("mean_rank_ic")

    elif subject_type == SubjectType.EXPERIMENT:
        assert experiment_spec is not None
        assert factor_df is not None
        target_weights = evaluate_experiment_rules(
            factor_df,
            experiment_spec,
            trading_days=trading_days,
        )
        engine = PortfolioBacktestEngine()
        p_res = engine.run(price_df, target_weights, portfolio_config)
        portfolio_result = p_res.to_dict()
        portfolio_result["target_weights"] = target_weights.to_dict(orient="list")

        summary_metrics = dict(p_res.summary)
        summary_metrics["total_orders"] = len(p_res.orders)
        summary_metrics["total_fills"] = len(p_res.fills)
        summary_metrics["final_equity"] = p_res.snapshots[-1].equity if p_res.snapshots else portfolio_config.initial_cash

    else:
        assert strategy_spec is not None
        driver_res = run_formal_strategy(
            spec=strategy_spec,
            price_df=price_df,
            config=portfolio_config,
            facts_df=facts_df,
        )
        p_res = driver_res.portfolio_result
        portfolio_result = p_res.to_dict()
        portfolio_result["target_weights"] = driver_res.target_weights.to_dict(orient="list")

        replay_targets = [t.to_dict() for t in driver_res.portfolio_targets]
        replay_result = {
            "portfolio_targets": replay_targets,
            "diagnostics": list(driver_res.replay_diagnostics),
        }

        summary_metrics = dict(p_res.summary)
        summary_metrics["total_orders"] = len(p_res.orders)
        summary_metrics["total_fills"] = len(p_res.fills)
        summary_metrics["final_equity"] = p_res.snapshots[-1].equity if p_res.snapshots else portfolio_config.initial_cash

    # 5. Provenance Package
    provenance = {
        "run_id": run_id,
        "config_hash": config_hash,
        "git_sha": git_sha,
        "data_source": data_source,
        "period": [request.period[0], request.period[1]],
        "universe": request.universe if not isinstance(request.universe, tuple) else list(request.universe),
        "subject_type": subject_type.value,
        "resolved_execution": resolved_exec,
        "warnings": warnings,
        "state_feedback_boundary": "target replay -> batch execution simulation (D16)",
    }

    return BackResult(
        status="SUCCESS",
        run_id=run_id,
        subject_type=subject_type,
        request_snapshot=request.to_canonical_dict(),
        provenance=provenance,
        summary_metrics=summary_metrics,
        factor=factor_result,
        portfolio=portfolio_result,
        replay=replay_result,
        warnings=tuple(warnings),
    )
