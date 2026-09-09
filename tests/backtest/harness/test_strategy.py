"""Unit tests for formal strategy driver and System B day-by-day replay."""

import pandas as pd
import pytest

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio.models import PortfolioBacktestConfig
from qrp_atlas.backtest.portfolio.strategy import run_strategy_portfolio_backtest
from qrp_atlas.backtest.harness.strategy_driver import (
    run_formal_strategy,
    run_system_b_day_by_day_replay,
)
from qrp_atlas.backtest.harness.models import StrategySpec, HarnessValidationError
from qrp_atlas.strategies.validation import StrategyValidationError


def _sample_config():
    return PortfolioBacktestConfig(
        name="test_config",
        initial_cash=100000.0,
        max_positions=6,
        max_weight_per_asset=0.25,
        cost=CostRule(commission_rate=0.0003, stamp_tax_rate=0.0005, slippage_bps=10.0),
    )


def _sample_prices():
    return pd.DataFrame([
        # Day 1
        {"trade_date": "2024-01-02", "asset_id": "000001.SZ", "asset_name": "A", "asset_type": "stock", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2},
        {"trade_date": "2024-01-02", "asset_id": "000002.SZ", "asset_name": "B", "asset_type": "stock", "open": 20.0, "high": 20.5, "low": 19.8, "close": 20.1},
        # Day 2
        {"trade_date": "2024-01-03", "asset_id": "000001.SZ", "asset_name": "A", "asset_type": "stock", "open": 10.2, "high": 10.8, "low": 10.1, "close": 10.7},
        {"trade_date": "2024-01-03", "asset_id": "000002.SZ", "asset_name": "B", "asset_type": "stock", "open": 20.1, "high": 20.3, "low": 19.5, "close": 19.6},
    ])


def _sample_system_b_facts():
    return pd.DataFrame([
        # Day 1: 000001 has high score, 000002 low score
        {
            "trade_date": "2024-01-02",
            "ticker": "000001.SZ",
            "comparison_score": 85.0,
            "system_b_exit_triggered": False,
            "entry_eligible": True,
            "severe_abnormal_supervision_status": "INACTIVE",
        },
        {
            "trade_date": "2024-01-02",
            "ticker": "000002.SZ",
            "comparison_score": 40.0,
            "system_b_exit_triggered": False,
            "entry_eligible": True,
            "severe_abnormal_supervision_status": "INACTIVE",
        },
        # Day 2: 000001 continues, 000002 gets exit triggered
        {
            "trade_date": "2024-01-03",
            "ticker": "000001.SZ",
            "comparison_score": 88.0,
            "system_b_exit_triggered": False,
            "entry_eligible": True,
            "severe_abnormal_supervision_status": "INACTIVE",
        },
        {
            "trade_date": "2024-01-03",
            "ticker": "000002.SZ",
            "comparison_score": 20.0,
            "system_b_exit_triggered": True,
            "entry_eligible": False,
            "severe_abnormal_supervision_status": "INACTIVE",
        },
    ])


def test_system_b_day_by_day_replay_carries_holdings():
    facts_df = _sample_system_b_facts()
    price_df = _sample_prices()
    config = _sample_config()

    result = run_system_b_day_by_day_replay(
        facts_df=facts_df,
        price_df=price_df,
        config=config,
        runtime_context={
            "authorization": True,
            "comparison_score_provenance": {
                "score_calculation_version": "v1.0",
                "rule_version_set_id": "rules-v1",
                "parameter_set_id": "params-v1",
                "input_snapshot_id": "snap-v1",
            },
        },
    )

    assert len(result.portfolio_targets) == 2
    day1_target = result.portfolio_targets[0]
    day2_target = result.portfolio_targets[1]

    # Day 1: 000001 selected (target weight 0.125)
    day1_assets = {p.asset_id: p.target_weight for p in day1_target.positions}
    assert "000001.SZ" in day1_assets
    assert day1_assets["000001.SZ"] == 0.125

    # Day 2: 000001 retained
    day2_assets = {p.asset_id: p.target_weight for p in day2_target.positions}
    assert "000001.SZ" in day2_assets

    # Check that execution output was generated
    assert result.portfolio_result is not None
    assert len(result.portfolio_result.snapshots) >= 2


def test_guard_direct_multiday_system_b_in_vectorized_runner_fails():
    """Verify the architectural constraint: multi-day System B facts cannot be fed to run_strategy_portfolio_backtest."""
    price_df = _sample_prices()
    config = _sample_config()

    with pytest.raises(Exception):
        # run_strategy_portfolio_backtest will call prepare_strategy_data then run_strategy_checked
        # on the entire multi-day price_df, which will fail because SystemB requires single trade_date
        run_strategy_portfolio_backtest("system_b_portfolio", price_df, config)
