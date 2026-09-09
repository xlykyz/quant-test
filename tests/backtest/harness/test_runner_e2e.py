"""End-to-end integration tests and single-parameter comparison for Task08 harness."""

import json
import pandas as pd
import pytest

from qrp_atlas.backtest import back, BackResult, SubjectType


def _make_e2e_dataset():
    # 5 assets across 10 trading days
    dates = [
        "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08",
        "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12", "2024-01-15",
    ]
    assets = ["000001.SZ", "000002.SZ", "000003.SZ", "600000.SH", "600001.SH"]
    prices_rows = []
    factors_rows = []

    for d_idx, d in enumerate(dates):
        for a_idx, a in enumerate(assets):
            base_p = 10.0 + a_idx * 4.0
            # A distinct drift for each asset
            drift = (a_idx - 2) * 0.1 * (d_idx + 1)
            close_p = round(base_p + drift, 2)
            prices_rows.append({
                "trade_date": d,
                "asset_id": a,
                "asset_name": a[:6],
                "asset_type": "stock",
                "open": close_p - 0.1,
                "high": close_p + 0.2,
                "low": close_p - 0.2,
                "close": close_p,
            })
            factors_rows.append({
                "trade_date": d,
                "asset_id": a,
                "momentum_20d": (a_idx + 1) * 0.05 + d_idx * 0.01,
                "turnover_rate": 0.02 + a_idx * 0.01,
                "pb": 1.0 + a_idx * 0.5,
            })

    return pd.DataFrame(prices_rows), pd.DataFrame(factors_rows)


def test_factor_smoke_e2e():
    prices_df, factors_df = _make_e2e_dataset()

    res = back(
        period=("2024-01-02", "2024-01-15"),
        universe=["000001.SZ", "000002.SZ", "000003.SZ", "600000.SH", "600001.SH"],
        factor={
            "field": "momentum_20d",
            "horizons": [5],
            "quantiles": 2,
        },
        data={"prices": prices_df, "factors": factors_df},
    )

    assert res.status == "SUCCESS"
    assert res.subject_type is SubjectType.FACTOR
    assert "daily_ic" in res.factor
    assert "ic_summary" in res.factor
    # Check JSON serializability of request snapshot
    json_str = json.dumps(res.to_dict())
    assert len(json_str) > 0


def test_single_parameter_change_experiment_comparison():
    """DoD requirement: modify one parameter, rerun, and compare reproducible results."""
    prices_df, factors_df = _make_e2e_dataset()
    universe = ["000001.SZ", "000002.SZ", "000003.SZ", "600000.SH", "600001.SH"]
    period = ("2024-01-02", "2024-01-15")

    # Run 1: Top 1 (weight 0.5)
    run1 = back(
        period=period,
        universe=universe,
        experiment={
            "score": "momentum_20d",
            "portfolio": {"top_n": 1, "weight_each": 0.5},
            "rank": {"by": "score", "order": "desc"},
        },
        data={"prices": prices_df, "factors": factors_df},
    )

    # Run 2: Top 3 (weight 0.25)
    run2 = back(
        period=period,
        universe=universe,
        experiment={
            "score": "momentum_20d",
            "portfolio": {"top_n": 3, "weight_each": 0.25},
            "rank": {"by": "score", "order": "desc"},
        },
        data={"prices": prices_df, "factors": factors_df},
    )

    assert run1.status == "SUCCESS"
    assert run2.status == "SUCCESS"

    # Provenance hashes are different and deterministic
    assert run1.provenance["config_hash"] != run2.provenance["config_hash"]

    # Fills and metrics differ predictably due to differing portfolio capacity
    assert run1.summary_metrics["final_equity"] != run2.summary_metrics["final_equity"]
    assert len(run1.portfolio["orders"]) != len(run2.portfolio["orders"])


def test_reproducibility_same_request_produces_identical_hash():
    prices_df, factors_df = _make_e2e_dataset()
    req_args = {
        "period": ("2024-01-02", "2024-01-15"),
        "universe": ["000001.SZ", "000002.SZ"],
        "experiment": {
            "score": "momentum_20d",
            "portfolio": {"top_n": 1, "weight_each": 0.5},
        },
        "data": {"prices": prices_df, "factors": factors_df},
    }

    run_a = back(**req_args)
    run_b = back(**req_args)

    assert run_a.provenance["config_hash"] == run_b.provenance["config_hash"]
    assert run_a.summary_metrics["final_equity"] == run_b.summary_metrics["final_equity"]
