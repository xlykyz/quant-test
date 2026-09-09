"""Unit tests for ExperimentEvaluator."""

import pandas as pd
import pytest

from qrp_atlas.backtest.harness.experiment import evaluate_experiment_rules
from qrp_atlas.backtest.harness.models import ExperimentSpec, FilterPredicate, HarnessValidationError


def _sample_factor_df():
    return pd.DataFrame([
        # Day 1
        {"trade_date": "2024-01-02", "asset_id": "A", "momentum_20d": 0.10, "turnover_rate": 0.05, "pb": 1.5, "is_good": True},
        {"trade_date": "2024-01-02", "asset_id": "B", "momentum_20d": 0.20, "turnover_rate": 0.02, "pb": 0.8, "is_good": True},
        {"trade_date": "2024-01-02", "asset_id": "C", "momentum_20d": 0.05, "turnover_rate": 0.08, "pb": 3.0, "is_good": False},
        # Day 2
        {"trade_date": "2024-01-03", "asset_id": "A", "momentum_20d": 0.08, "turnover_rate": 0.03, "pb": 1.4, "is_good": True},
        {"trade_date": "2024-01-03", "asset_id": "B", "momentum_20d": 0.05, "turnover_rate": 0.01, "pb": 0.9, "is_good": True},
        {"trade_date": "2024-01-03", "asset_id": "C", "momentum_20d": 0.25, "turnover_rate": 0.10, "pb": 2.5, "is_good": True},
    ])


def test_single_score_and_top_n_selection():
    df = _sample_factor_df()
    spec = ExperimentSpec(
        score="momentum_20d",
        portfolio={"top_n": 1, "weight_each": 1.0},
        rank={"by": "score", "order": "desc"},
    )
    targets = evaluate_experiment_rules(df, spec)

    day1 = targets[targets["trade_date"] == "2024-01-02"]
    day2 = targets[targets["trade_date"] == "2024-01-03"]

    # Day 1: B has highest momentum (0.20)
    assert day1[day1["asset_id"] == "B"]["target_weight"].iloc[0] == 1.0

    # Day 2: C has highest momentum (0.25); B exits with 0.0 target weight
    assert day2[day2["asset_id"] == "C"]["target_weight"].iloc[0] == 1.0
    assert day2[day2["asset_id"] == "B"]["target_weight"].iloc[0] == 0.0


def test_linear_score_combination():
    df = _sample_factor_df()
    spec = ExperimentSpec(
        score={"momentum_20d": 10.0, "turnover_rate": 100.0},
        portfolio={"top_n": 2, "weight_each": 0.5},
    )
    targets = evaluate_experiment_rules(df, spec)
    assert not targets.empty


def test_filter_predicate_evaluation():
    df = _sample_factor_df()
    # Filter only is_good == True
    spec = ExperimentSpec(
        score="momentum_20d",
        filter=FilterPredicate("pb", "lt", 2.0),
        portfolio={"top_n": 2, "weight_each": 0.5},
    )
    targets = evaluate_experiment_rules(df, spec)
    # C has pb=3.0 on day 1, so it shouldn't be selected on day 1
    day1 = targets[targets["trade_date"] == "2024-01-02"]
    assert "C" not in set(day1[day1["target_weight"] > 0]["asset_id"])


def test_missing_column_fails_fast():
    df = _sample_factor_df()
    spec = ExperimentSpec(score="unknown_field")
    with pytest.raises(HarnessValidationError, match="missing from factor_df"):
        evaluate_experiment_rules(df, spec)


def test_ascending_rank_order_selection():
    df = _sample_factor_df()
    spec = ExperimentSpec(
        score="momentum_20d",
        portfolio={"top_n": 1, "weight_each": 1.0},
        rank={"by": "score", "order": "asc"},
    )
    targets = evaluate_experiment_rules(df, spec)
    day1 = targets[targets["trade_date"] == "2024-01-02"]
    # Day 1: C has lowest momentum (0.05), so ascending order selects C
    assert day1[day1["asset_id"] == "C"]["target_weight"].iloc[0] == 1.0

