"""Unit tests verifying in-memory data injection without DuckDB."""

import pandas as pd
import pytest

from qrp_atlas.backtest import back, BackResult, SubjectType


def _make_sample_prices():
    rows = []
    assets = ["000001.SZ", "000002.SZ", "600000.SH"]
    dates = [
        "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
        "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11",
        "2024-01-12", "2024-01-15", "2024-01-16", "2024-01-17",
    ]
    for d_idx, d in enumerate(dates):
        for a_idx, a in enumerate(assets):
            p = 10.0 + a_idx * 5.0 + d_idx * 0.2
            rows.append({
                "trade_date": d,
                "asset_id": a,
                "asset_name": a[:6],
                "asset_type": "stock",
                "open": p,
                "high": p + 0.5,
                "low": p - 0.5,
                "close": p + 0.1,
            })
    return pd.DataFrame(rows)


def _make_sample_factors(prices_df):
    factors = prices_df[["trade_date", "asset_id"]].copy()
    # Synthetic factor: asset_id rank + noise
    factors["momentum_20d"] = [
        0.1 if "000001" in a else (0.2 if "000002" in a else 0.05)
        for a in factors["asset_id"]
    ]
    factors["turnover_rate"] = [0.03 for _ in factors["asset_id"]]
    factors["pb"] = [1.5 for _ in factors["asset_id"]]
    return factors


def test_factor_mode_with_data_injection():
    prices = _make_sample_prices()
    factors = _make_sample_factors(prices)

    res = back(
        period=("2024-01-02", "2024-01-17"),
        universe=["000001.SZ", "000002.SZ"],
        factor={
            "field": "momentum_20d",
            "horizons": [5],
            "quantiles": 2,
        },
        data={"prices": prices, "factors": factors},
    )

    assert isinstance(res, BackResult)
    assert res.status == "SUCCESS"
    assert res.subject_type is SubjectType.FACTOR
    assert res.factor is not None
    assert "daily_ic" in res.factor
    assert "ic_summary" in res.factor
    assert res.provenance["data_source"] == "in_memory_injection"


def test_experiment_mode_with_data_injection():
    prices = _make_sample_prices()
    factors = _make_sample_factors(prices)

    res = back(
        period=("2024-01-02", "2024-01-17"),
        universe=["000001.SZ", "000002.SZ", "600000.SH"],
        experiment={
            "score": {"momentum_20d": 1.0},
            "portfolio": {"top_n": 1, "weight_each": 0.5},
            "rank": {"by": "score", "order": "desc"},
            "exit": "when_not_selected",
        },
        data={"prices": prices, "factors": factors},
    )

    assert isinstance(res, BackResult)
    assert res.status == "SUCCESS"
    assert res.subject_type is SubjectType.EXPERIMENT
    assert res.portfolio is not None
    assert "equity_curve" in res.portfolio
    assert "orders" in res.portfolio
    assert "fills" in res.portfolio
    assert "max_drawdown" in res.summary_metrics


def test_strategy_mode_with_data_injection():
    prices = _make_sample_prices()
    dates = sorted(prices["trade_date"].unique())
    facts_rows = []
    for d in dates:
        facts_rows.append({
            "trade_date": d,
            "ticker": "000001.SZ",
            "comparison_score": 90.0,
            "system_b_exit_triggered": False,
            "entry_eligible": True,
            "severe_abnormal_supervision_status": "INACTIVE",
        })
        facts_rows.append({
            "trade_date": d,
            "ticker": "000002.SZ",
            "comparison_score": 40.0,
            "system_b_exit_triggered": False,
            "entry_eligible": True,
            "severe_abnormal_supervision_status": "INACTIVE",
        })
    facts_df = pd.DataFrame(facts_rows)

    res = back(
        period=("2024-01-02", "2024-01-17"),
        strategy={
            "code": "system_b_portfolio",
            "runtime_context": {
                "authorization": True,
                "comparison_score_provenance": {
                    "score_calculation_version": "v1.0",
                    "rule_version_set_id": "rules-v1",
                    "parameter_set_id": "params-v1",
                    "input_snapshot_id": "snap-v1",
                },
            },
        },
        data={"prices": prices, "facts": facts_df},
    )

    assert isinstance(res, BackResult)
    assert res.status == "SUCCESS"
    assert res.subject_type is SubjectType.STRATEGY
    assert res.portfolio is not None
    assert res.replay is not None
    assert len(res.replay["portfolio_targets"]) == len(dates)
