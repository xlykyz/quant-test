"""Unit tests for execution preset resolution and conflict checks."""

import pytest
import pandas as pd

from qrp_atlas.backtest import back, HarnessValidationError
from qrp_atlas.backtest.harness.models import ExperimentSpec, ExecutionSpec, SubjectType
from qrp_atlas.backtest.harness.runner import _resolve_execution_config


def test_preset_expansion_defaults():
    exec_spec = ExecutionSpec(preset="a_share_daily")
    exp_spec = ExperimentSpec(score="factor_x", portfolio={"top_n": 8, "weight_each": 0.1})
    config, summary = _resolve_execution_config(exec_spec, SubjectType.EXPERIMENT, exp_spec, "test-run")

    assert config.initial_cash == 1_000_000.0
    # Experiment top_n overrides preset default
    assert config.max_positions == 8
    assert config.max_weight_per_asset == 0.1
    assert config.cost.commission_rate == 0.0003
    assert config.cost.stamp_tax_rate == 0.0005
    assert summary["sources"]["max_positions"] == "subject:experiment.portfolio.top_n"


def test_explicit_override_success():
    exec_spec = ExecutionSpec(
        preset="a_share_daily",
        initial_cash=500_000.0,
        max_positions=12,
        commission_rate=0.0001,
    )
    exp_spec = ExperimentSpec(score="factor_x", portfolio={"top_n": 8})
    config, summary = _resolve_execution_config(exec_spec, SubjectType.EXPERIMENT, exp_spec, "test-run")

    assert config.initial_cash == 500_000.0
    assert config.max_positions == 12
    assert config.cost.commission_rate == 0.0001
    assert summary["sources"]["initial_cash"] == "explicit:override"
    assert summary["sources"]["max_positions"] == "explicit:override"


def test_conflict_detection_max_positions_fail_fast():
    # Experiment requests top_n=6, user explicitly limits max_positions=4
    exec_spec = ExecutionSpec(preset="a_share_daily", max_positions=4)
    exp_spec = ExperimentSpec(score="factor_x", portfolio={"top_n": 6})

    with pytest.raises(HarnessValidationError, match="Conflict: execution.max_positions .* cannot accommodate"):
        _resolve_execution_config(exec_spec, SubjectType.EXPERIMENT, exp_spec, "test-run")


def test_conflict_detection_weight_fail_fast():
    # Experiment requests weight_each=0.25, user explicitly limits max_weight_per_asset=0.1
    exec_spec = ExecutionSpec(preset="a_share_daily", max_weight_per_asset=0.1)
    exp_spec = ExperimentSpec(score="factor_x", portfolio={"top_n": 4, "weight_each": 0.25})

    with pytest.raises(HarnessValidationError, match="Conflict: execution.max_weight_per_asset .* is smaller than"):
        _resolve_execution_config(exec_spec, SubjectType.EXPERIMENT, exp_spec, "test-run")


def test_unknown_preset_fail_fast():
    exec_spec = ExecutionSpec(preset="unknown_preset")
    exp_spec = ExperimentSpec(score="factor_x")
    with pytest.raises(HarnessValidationError, match="Unknown execution preset: 'unknown_preset'"):
        _resolve_execution_config(exec_spec, SubjectType.EXPERIMENT, exp_spec, "test-run")
