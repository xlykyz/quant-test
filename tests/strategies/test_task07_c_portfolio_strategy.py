"""End-to-end integration tests for SystemBPortfolioStrategy and checked runner."""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.backtest.portfolio import strategy_result_to_target_weights
from qrp_atlas.strategies import (
    StrategyAction,
    StrategyHoldingState,
    StrategyInput,
    StrategyPortfolioTarget,
    StrategyRunResult,
    StrategyValidationError,
    get_strategy,
    run_strategy,
    run_strategy_checked,
)
from qrp_atlas.strategies.builtin.system_b_portfolio import (
    ADD_ENTRY_SELECTED,
    NEW_ENTRY_SELECTED,
    POSITION_PRESERVED,
    SystemBPortfolioStrategy,
)


TRADE_DATE = "2024-01-10"


def _holding(asset_id: str, weight: float = 0.125, *, entry_count: int = 1) -> StrategyHoldingState:
    return StrategyHoldingState(
        asset_id=asset_id,
        current_weight=weight,
        entry_count=entry_count,
        first_entry_date="2024-01-02",
        last_entry_date="2024-01-02",
    )


def _facts(rows: list[dict[str, object]]) -> pd.DataFrame:
    base_rows: list[dict[str, object]] = []
    for r in rows:
        row: dict[str, object] = {
            "trade_date": TRADE_DATE,
            "comparison_score": 10.0,
            "entry_eligible": True,
            "system_b_exit_triggered": False,
            "severe_abnormal_supervision_status": "INACTIVE",
        }
        row.update(r)
        base_rows.append(row)
    df = pd.DataFrame(base_rows)
    for col in df.columns:
        if any(r.get(col) is None for r in base_rows):
            df[col] = [r.get(col) for r in base_rows]
    return df


def _provenance() -> dict[str, object]:
    return {
        "trade_date": TRADE_DATE,
        "score_calculation_version": "score-v1",
        "rule_version_set_id": "rules-v1",
        "parameter_set_id": "params-v1",
        "input_snapshot_id": "snapshot-v1",
    }


def test_system_b_portfolio_strategy_end_to_end() -> None:
    # H1 is held, exit triggered -> EXIT
    # H2 is held, no exit, score 60.0 (top score) -> ADD
    # H3 is held, no exit, score 40.0 (not top score) -> HOLD
    # N1 is candidate, score 60.0 > min(retained) (40.0) -> NEW
    # N2 is candidate, score 50.0 > min(retained) (40.0) -> NEW
    holdings = {
        "H1": _holding("H1", 0.125),
        "H2": _holding("H2", 0.125),
        "H3": _holding("H3", 0.125),
    }
    facts_df = _facts([
        {"ticker": "H1", "system_b_exit_triggered": True, "comparison_score": 50.0},
        {"ticker": "H2", "system_b_exit_triggered": False, "comparison_score": 60.0},
        {"ticker": "H3", "system_b_exit_triggered": False, "comparison_score": 40.0},
        {"ticker": "N1", "comparison_score": 60.0},
        {"ticker": "N2", "comparison_score": 50.0},
    ])

    strategy = SystemBPortfolioStrategy()
    strategy_input = StrategyInput(
        prepared_data=facts_df,
        holdings=holdings,
        parameters={
            "authorization": True,
            "comparison_score_provenance": _provenance(),
        },
    )

    result = strategy.run(strategy_input)

    assert isinstance(result, StrategyRunResult)
    assert len(result.decisions) == 5
    decision_map = {d.asset_id: d for d in result.decisions}
    assert decision_map["H1"].action is StrategyAction.EXIT
    assert decision_map["H2"].action is StrategyAction.ENTER
    assert decision_map["H2"].evidence["entry_kind"] == "ADD"
    assert decision_map["H3"].action is StrategyAction.HOLD
    assert decision_map["N1"].action is StrategyAction.ENTER
    assert decision_map["N1"].evidence["entry_kind"] == "NEW"
    assert decision_map["N2"].action is StrategyAction.ENTER
    assert decision_map["N2"].evidence["entry_kind"] == "NEW"

    assert len(result.portfolio_targets) == 1
    target = result.portfolio_targets[0]
    assert isinstance(target, StrategyPortfolioTarget)

    pos_map = {p.asset_id: p for p in target.positions}
    assert "H1" not in pos_map  # EXIT omitted
    assert pos_map["H2"].target_weight == 0.25
    assert pos_map["H2"].reason_code == ADD_ENTRY_SELECTED
    assert pos_map["H3"].target_weight == 0.125
    assert pos_map["H3"].reason_code == POSITION_PRESERVED
    assert pos_map["N1"].target_weight == 0.125
    assert pos_map["N1"].reason_code == NEW_ENTRY_SELECTED
    assert pos_map["N2"].target_weight == 0.125
    assert pos_map["N2"].reason_code == NEW_ENTRY_SELECTED

    # Positions stably sorted by asset_id ASC: H2, H3, N1, N2
    assert [p.asset_id for p in target.positions] == ["H2", "H3", "N1", "N2"]


def test_checked_runner_and_registry_integration() -> None:
    # Test via run_strategy_checked and registry lookup
    holdings = {"H1": _holding("H1", 0.125)}
    facts_df = _facts([
        {"ticker": "H1", "system_b_exit_triggered": False, "comparison_score": 50.0},
        {"ticker": "N1", "comparison_score": 90.0},
    ])

    strategy_input = StrategyInput(
        prepared_data=facts_df,
        holdings=holdings,
        parameters={
            "authorization": True,
            "comparison_score_provenance": _provenance(),
        },
    )

    # 1. via get_strategy and run_strategy_checked
    strategy = get_strategy("system_b_portfolio")
    checked_result = run_strategy_checked(strategy, strategy_input)
    assert len(checked_result.portfolio_targets) == 1
    target = checked_result.portfolio_targets[0]
    assert [p.asset_id for p in target.positions] == ["H1", "N1"]

    # 2. via public run_strategy helper
    registry_result = run_strategy("system_b_portfolio", strategy_input)
    assert len(registry_result.portfolio_targets) == 1
    assert [p.asset_id for p in registry_result.portfolio_targets[0].positions] == ["H1", "N1"]


def test_checked_runner_allows_nullable_system_b_fields() -> None:
    # Verify that NA/None in exit_triggered or supervision is not falsely rejected by checked runner
    holdings = {"H1": _holding("H1", 0.125)}
    facts_df = _facts([
        {"ticker": "H1", "system_b_exit_triggered": pd.NA, "comparison_score": 10.0},
        {"ticker": "N1", "severe_abnormal_supervision_status": None, "comparison_score": 50.0},
    ])

    strategy_input = StrategyInput(
        prepared_data=facts_df,
        holdings=holdings,
        parameters={
            "authorization": True,
            "comparison_score_provenance": _provenance(),
        },
    )

    strategy = get_strategy("system_b_portfolio")
    checked_result = run_strategy_checked(strategy, strategy_input)
    target = checked_result.portfolio_targets[0]

    # H1 exit status unavailable -> held NO_ACTION -> preserved
    assert len(target.positions) == 1
    assert target.positions[0].asset_id == "H1"
    assert target.positions[0].target_weight == 0.125
    assert target.positions[0].reason_code == POSITION_PRESERVED


def test_portfolio_target_converts_to_target_weights() -> None:
    holdings = {"H1": _holding("H1", 0.125)}
    facts_df = _facts([
        {"ticker": "H1", "system_b_exit_triggered": True},
        {"ticker": "N1", "comparison_score": 90.0},
    ])

    strategy_input = StrategyInput(
        prepared_data=facts_df,
        holdings=holdings,
        parameters={
            "authorization": True,
            "comparison_score_provenance": _provenance(),
        },
    )

    result = run_strategy_checked(get_strategy("system_b_portfolio"), strategy_input)
    weights_df = strategy_result_to_target_weights(
        result,
        max_positions=6,
        max_weight_per_asset=1.0,
    )

    records = weights_df.to_dict("records")
    # N1 has 0.125 target weight
    assert any(r["asset_id"] == "N1" and r["target_weight"] == 0.125 for r in records)
