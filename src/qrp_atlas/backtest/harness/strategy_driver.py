"""Formal strategy driver and day-by-day System B replay runner (Task08)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine
from qrp_atlas.backtest.portfolio.models import PortfolioBacktestConfig, PortfolioBacktestResult
from qrp_atlas.backtest.portfolio.strategy import (
    run_strategy_portfolio_backtest,
    strategy_result_to_target_weights,
)
from qrp_atlas.contracts import ASSET_ID, TICKER, TRADE_DATE
from qrp_atlas.strategies import (
    StrategyHoldingState,
    StrategyInput,
    StrategyPortfolioTarget,
    StrategyRunResult,
    get_strategy,
    run_strategy_checked,
)
from qrp_atlas.strategies.validation import StrategyValidationError
from .models import HarnessError, HarnessValidationError, StrategySpec


VECTORIZED_STRATEGIES = frozenset({
    "cross_sectional_momentum_long_only",
    "multifactor_long_only",
})


@dataclass(frozen=True)
class StrategyDriverResult:
    """Output from formal strategy driver."""

    portfolio_result: PortfolioBacktestResult
    target_weights: pd.DataFrame
    strategy_results: tuple[StrategyRunResult, ...]
    portfolio_targets: tuple[StrategyPortfolioTarget, ...]
    replay_diagnostics: tuple[str, ...] = ()


def _normalize_date(val: Any) -> str:
    return pd.Timestamp(val).strftime("%Y-%m-%d")


def run_system_b_day_by_day_replay(
    facts_df: pd.DataFrame,
    price_df: pd.DataFrame,
    config: PortfolioBacktestConfig,
    *,
    parameters: Mapping[str, Any] | None = None,
    runtime_context: Mapping[str, Any] | None = None,
    strategy_code: str = "system_b_portfolio",
    version: str | None = None,
) -> StrategyDriverResult:
    """Execute day-by-day System B formal target replay followed by portfolio simulation.

    Enforces Design v1.1 D13 and D16:
    - Each formal strategy invocation contains exactly one trade_date.
    - Holdings state carries forward across trade dates.
    - Full native StrategyPortfolioTarget snapshots are converted to target weights.
    - Output marks the D16 state feedback boundary:
      `target replay -> batch execution simulation`.
    """
    if not isinstance(facts_df, pd.DataFrame):
        raise HarnessValidationError("facts_df must be a pandas DataFrame")
    if not isinstance(price_df, pd.DataFrame):
        raise HarnessValidationError("price_df must be a pandas DataFrame")

    strategy = get_strategy(strategy_code, version)

    # Standardize trade_date and ticker in facts_df
    facts = facts_df.copy()
    if TRADE_DATE not in facts.columns:
        raise HarnessValidationError("facts_df missing 'trade_date'")
    facts[TRADE_DATE] = facts[TRADE_DATE].apply(_normalize_date)
    if TICKER not in facts.columns:
        if ASSET_ID in facts.columns:
            facts[TICKER] = facts[ASSET_ID].astype(str).str.strip()
        else:
            raise HarnessValidationError("facts_df requires 'ticker' or 'asset_id'")

    dates = sorted(facts[TRADE_DATE].unique())
    if not dates:
        raise HarnessValidationError("facts_df contains no valid trade dates")

    params = dict(parameters or {})
    ctx = dict(runtime_context or {})

    # Injected authorization default if not provided
    if "authorization" not in ctx and "authorization" not in params:
        ctx["authorization"] = "AUTHORIZED"

    current_holdings: dict[str, StrategyHoldingState] = {}
    all_target_weight_frames: list[pd.DataFrame] = []
    strategy_results: list[StrategyRunResult] = []
    portfolio_targets: list[StrategyPortfolioTarget] = []

    for trade_date in dates:
        day_facts = facts[facts[TRADE_DATE] == trade_date].copy().reset_index(drop=True)

        day_params = dict(params)
        day_ctx = dict(ctx)

        # Adapt provenance trade_date for the day if a provenance mapping is provided
        raw_prov = day_ctx.get("comparison_score_provenance", day_params.get("comparison_score_provenance"))
        if isinstance(raw_prov, Mapping):
            prov_copy = dict(raw_prov)
            prov_copy["trade_date"] = trade_date
            if "comparison_score_provenance" in day_ctx:
                day_ctx["comparison_score_provenance"] = prov_copy
            else:
                day_params["comparison_score_provenance"] = prov_copy

        strategy_input = StrategyInput(
            prepared_data=day_facts,
            holdings=current_holdings,
            parameters=day_params,
            runtime_context=day_ctx,
        )

        try:
            res = run_strategy_checked(strategy, strategy_input)
        except Exception as exc:
            raise HarnessError(f"System B strategy failed on date {trade_date}: {exc}") from exc

        strategy_results.append(res)
        if res.portfolio_targets:
            target = res.portfolio_targets[0]
            portfolio_targets.append(target)

            # Update holding state for next trade date
            next_holdings: dict[str, StrategyHoldingState] = {}
            for pos in target.positions:
                if float(pos.target_weight) > 0.0:
                    entry_cnt = 1
                    if isinstance(pos.evidence, Mapping):
                        entry_cnt = int(pos.evidence.get("entry_count_after_if_selected", 1))
                    next_holdings[pos.asset_id] = StrategyHoldingState(
                        asset_id=pos.asset_id,
                        current_weight=float(pos.target_weight),
                        entry_count=entry_cnt,
                    )
            current_holdings = next_holdings

        # Convert daily targets to target weights
        weights_df = strategy_result_to_target_weights(
            res,
            max_positions=config.max_positions,
            max_weight_per_asset=config.max_weight_per_asset,
            emit_unchanged_snapshots=True,
        )
        if not weights_df.empty:
            all_target_weight_frames.append(weights_df)

    if all_target_weight_frames:
        combined_weights = pd.concat(all_target_weight_frames, ignore_index=True)
    else:
        combined_weights = pd.DataFrame(columns=["trade_date", "asset_id", "target_weight", "priority"])

    # Run PortfolioBacktestEngine
    engine = PortfolioBacktestEngine()
    portfolio_res = engine.run(price_df, combined_weights, config)

    diagnostics = (
        "STATE_FEEDBACK_BOUNDARY: target replay preceded batch execution simulation; "
        "rejections/fills do not alter subsequent strategy decisions in v1.1 Task08.",
    )

    return StrategyDriverResult(
        portfolio_result=portfolio_res,
        target_weights=combined_weights,
        strategy_results=tuple(strategy_results),
        portfolio_targets=tuple(portfolio_targets),
        replay_diagnostics=diagnostics,
    )


def run_formal_strategy(
    spec: StrategySpec,
    price_df: pd.DataFrame,
    config: PortfolioBacktestConfig,
    *,
    facts_df: pd.DataFrame | None = None,
) -> StrategyDriverResult:
    """Route formal strategy execution to day-by-day driver or proven vectorized runner."""
    code = spec.code

    if code == "system_b_portfolio":
        if facts_df is None:
            raise HarnessValidationError("Strategy 'system_b_portfolio' requires 'facts' DataFrame")
        return run_system_b_day_by_day_replay(
            facts_df=facts_df,
            price_df=price_df,
            config=config,
            parameters=spec.params,
            runtime_context=spec.runtime_context,
            strategy_code=spec.code,
            version=spec.version,
        )

    if code in VECTORIZED_STRATEGIES:
        run = run_strategy_portfolio_backtest(
            code=code,
            price_df=price_df,
            config=config,
            parameters=spec.params,
            version=spec.version,
            runtime_context=spec.runtime_context,
        )
        return StrategyDriverResult(
            portfolio_result=run.portfolio_result,
            target_weights=run.target_weights,
            strategy_results=(run.strategy_result,),
            portfolio_targets=(),
        )

    # For any unclassified strategy: if facts_df provided, try day-by-day; else vectorized
    if facts_df is not None:
        return run_system_b_day_by_day_replay(
            facts_df=facts_df,
            price_df=price_df,
            config=config,
            parameters=spec.params,
            runtime_context=spec.runtime_context,
            strategy_code=spec.code,
            version=spec.version,
        )
    run = run_strategy_portfolio_backtest(
        code=code,
        price_df=price_df,
        config=config,
        parameters=spec.params,
        version=spec.version,
        runtime_context=spec.runtime_context,
    )
    return StrategyDriverResult(
        portfolio_result=run.portfolio_result,
        target_weights=run.target_weights,
        strategy_results=(run.strategy_result,),
        portfolio_targets=(),
    )
