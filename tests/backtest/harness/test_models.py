"""Unit tests for BackRequest, BackResult, and Spec contracts."""

import pytest
import pandas as pd

from qrp_atlas.backtest.harness.models import (
    BackRequest,
    BackResult,
    ExperimentSpec,
    FactorSpec,
    FilterPredicate,
    HarnessValidationError,
    StrategySpec,
    SubjectType,
)


def test_factor_spec_validation():
    spec = FactorSpec(field="turnover_rate", direction="higher_is_better", horizons=(5, 20), quantiles=5)
    assert spec.field == "turnover_rate"

    with pytest.raises(HarnessValidationError, match="field must not be empty"):
        FactorSpec(field="")

    with pytest.raises(HarnessValidationError, match="direction must be"):
        FactorSpec(field="x", direction="invalid")

    with pytest.raises(HarnessValidationError, match="horizons must be positive"):
        FactorSpec(field="x", horizons=(0,))

    with pytest.raises(HarnessValidationError, match="quantiles must be >= 2"):
        FactorSpec(field="x", quantiles=1)


def test_experiment_spec_validation():
    spec = ExperimentSpec(
        score={"momentum_20d": 0.5, "turnover_rate": 0.5},
        filter=FilterPredicate("pb", "gt", 0),
        rank={"by": "score", "order": "desc"},
        portfolio={"top_n": 6, "weight_each": 0.125},
    )
    assert isinstance(spec.score, dict)

    # Empty score
    with pytest.raises(HarnessValidationError, match="score"):
        ExperimentSpec(score="")

    # Non-numeric weight
    with pytest.raises(HarnessValidationError, match="must be numeric"):
        ExperimentSpec(score={"m": "invalid"})

    # Invalid exit
    with pytest.raises(HarnessValidationError, match="exit currently only supports 'when_not_selected'"):
        ExperimentSpec(score="m", exit="other")

    # Invalid portfolio
    with pytest.raises(HarnessValidationError, match="positive integer 'top_n'"):
        ExperimentSpec(score="m", portfolio={"top_n": 0})


def test_filter_predicate_whitelist():
    pred = FilterPredicate("pe", "lt", 30)
    assert pred.field == "pe"
    assert pred.op == "lt"

    with pytest.raises(HarnessValidationError, match="FilterPredicate.op must be one of"):
        FilterPredicate("pe", "regex_match", "pattern")


def test_back_request_exactly_one_subject():
    factor = FactorSpec(field="turnover_rate")
    experiment = ExperimentSpec(score="turnover_rate")

    # None provided
    with pytest.raises(HarnessValidationError, match="Exactly one of factor, experiment, or strategy"):
        BackRequest(
            period=("2024-01-01", "2024-06-30"),
            universe=["000001.SZ"],
            subject_type=SubjectType.FACTOR,
        )

    # Multiple provided
    with pytest.raises(HarnessValidationError, match="Exactly one of factor, experiment, or strategy"):
        BackRequest(
            period=("2024-01-01", "2024-06-30"),
            universe=["000001.SZ"],
            subject_type=SubjectType.FACTOR,
            factor=factor,
            experiment=experiment,
        )


def test_back_request_period_and_universe_validation():
    factor = FactorSpec(field="turnover_rate")

    # Start > End
    with pytest.raises(HarnessValidationError, match="start .* must be <= end"):
        BackRequest(
            period=("2024-06-30", "2024-01-01"),
            universe=["000001.SZ"],
            subject_type=SubjectType.FACTOR,
            factor=factor,
        )

    # Unknown universe string
    with pytest.raises(HarnessValidationError, match="Unknown universe preset 'system_b_universe'"):
        BackRequest(
            period=("2024-01-01", "2024-06-30"),
            universe="system_b_universe",
            subject_type=SubjectType.FACTOR,
            factor=factor,
        )

    # Valid universe preset
    req = BackRequest(
        period=("2024-01-01", "2024-06-30"),
        universe="all_a",
        subject_type=SubjectType.FACTOR,
        factor=factor,
    )
    assert req.universe == "all_a"
    assert req.compute_config_hash() is not None


def test_back_result_serialization():
    res = BackResult(
        status="SUCCESS",
        run_id="run-123",
        subject_type=SubjectType.FACTOR,
        request_snapshot={"period": ["2024-01-01", "2024-06-30"]},
        provenance={"config_hash": "abc"},
        summary_metrics={"ic_mean": 0.05},
        factor={"ic_summary": {"5D": 0.05}},
    )
    d = res.to_dict()
    assert d["status"] == "SUCCESS"
    assert d["summary_metrics"]["ic_mean"] == 0.05
    assert "run-123" in res.summary()
