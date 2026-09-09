"""Real historical data verification script for Task08 DoD on myserver."""

from __future__ import annotations

import json
from qrp_atlas.backtest import back, SubjectType


def main() -> None:
    print(">>> 1. Running Factor Mode Smoke on real DuckDB data...")
    universe = ["000001.SZ", "000002.SZ", "600000.SH", "600519.SH", "000858.SZ"]
    factor_res = back(
        period=("2024-01-02", "2024-03-29"),
        universe=universe,
        factor={
            "field": "close",
            "direction": "higher_is_better",
            "horizons": [5, 20],
            "quantiles": 2,
        },
    )
    print("Factor Status:", factor_res.status)
    print("Factor Summary:\n", factor_res.summary())
    print("Factor IC Summary:\n", json.dumps(factor_res.factor.get("ic_summary"), indent=2))
    assert factor_res.status == "SUCCESS"
    assert factor_res.factor is not None

    print("\n>>> 2. Running Experiment Mode Single-Parameter Comparison on real DuckDB data...")
    # Config 1: Top 2, weight 0.25 each
    exp1 = back(
        period=("2024-01-02", "2024-03-29"),
        universe=universe,
        experiment={
            "score": "close",
            "portfolio": {"top_n": 2, "weight_each": 0.25},
            "rank": {"by": "score", "order": "desc"},
        },
    )
    print("Exp 1 (Top 2, weight 0.25) Summary:\n", exp1.summary())

    # Config 2: Top 3, weight 0.25 each (Strict single-parameter comparison: ONLY top_n changed 2 -> 3)
    exp2 = back(
        period=("2024-01-02", "2024-03-29"),
        universe=universe,
        experiment={
            "score": "close",
            "portfolio": {"top_n": 3, "weight_each": 0.25},
            "rank": {"by": "score", "order": "desc"},
        },
    )
    print("Exp 2 (Top 3, weight 0.25) Summary:\n", exp2.summary())

    print("\n>>> 3. Verifying DoD Criteria...")
    assert exp1.status == "SUCCESS"
    assert exp2.status == "SUCCESS"

    # Verify strict single-parameter change: all request fields identical except portfolio.top_n
    req1 = exp1.request
    req2 = exp2.request
    assert req1.period == req2.period
    assert req1.universe == req2.universe
    assert req1.subject_type == req2.subject_type
    assert req1.experiment.score == req2.experiment.score
    assert req1.experiment.rank == req2.experiment.rank
    assert req1.experiment.filter == req2.experiment.filter
    assert req1.experiment.exit == req2.experiment.exit
    assert req1.experiment.portfolio.get("weight_each") == req2.experiment.portfolio.get("weight_each") == 0.25
    assert req1.experiment.portfolio.get("top_n") == 2
    assert req2.experiment.portfolio.get("top_n") == 3

    assert exp1.provenance["config_hash"] != exp2.provenance["config_hash"]
    print("Config Hash 1:", exp1.provenance["config_hash"])
    print("Config Hash 2:", exp2.provenance["config_hash"])
    print("Exp 1 Final Equity:", exp1.summary_metrics.get("final_equity"))
    print("Exp 2 Final Equity:", exp2.summary_metrics.get("final_equity"))
    print("\n[SUCCESS] All Task08 real historical data DoD requirements verified successfully!")


if __name__ == "__main__":
    main()
