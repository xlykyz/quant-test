"""Focused regression for Task07-B typed holding validation."""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.strategies import StrategyHoldingState, StrategyValidationError
from qrp_atlas.strategies.builtin.system_b_decision import normalize_system_b_decision_input


TRADE_DATE = "2024-01-10"


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "A",
                "trade_date": TRADE_DATE,
                "comparison_score": 1.0,
                "entry_eligible": True,
                "system_b_exit_triggered": False,
                "severe_abnormal_supervision_status": "INACTIVE",
            }
        ]
    )


def _provenance() -> dict[str, str]:
    return {
        "trade_date": TRADE_DATE,
        "score_calculation_version": "score-v1",
        "rule_version_set_id": "rules-v1",
        "parameter_set_id": "params-v1",
        "input_snapshot_id": "snapshot-v1",
    }


def _normalize(holding: StrategyHoldingState) -> None:
    normalize_system_b_decision_input(
        _frame(),
        holdings={"A": holding},
        candidate_asset_ids=set(),
        authorization=True,
        comparison_score_provenance=_provenance(),
    )


@pytest.mark.parametrize("current_weight", [0.0, -0.1, float("inf"), float("nan"), True, "bad"])
def test_invalid_current_weight_is_rejected(current_weight: object) -> None:
    holding = StrategyHoldingState(
        asset_id="A",
        current_weight=current_weight,  # type: ignore[arg-type]
        entry_count=1,
        first_entry_date="2024-01-02",
        last_entry_date="2024-01-02",
    )

    with pytest.raises(StrategyValidationError, match="current_weight"):
        _normalize(holding)


@pytest.mark.parametrize("entry_count", [0, -1, 1.5, "one", True])
def test_non_integer_or_non_positive_entry_count_is_rejected(entry_count: object) -> None:
    holding = StrategyHoldingState(
        asset_id="A",
        current_weight=0.125,
        entry_count=entry_count,  # type: ignore[arg-type]
        first_entry_date="2024-01-02",
        last_entry_date="2024-01-02",
    )

    with pytest.raises(StrategyValidationError, match="entry_count"):
        _normalize(holding)


def test_invalid_holding_date_is_rejected_as_validation_error() -> None:
    holding = StrategyHoldingState(
        asset_id="A",
        current_weight=0.125,
        entry_count=1,
        first_entry_date="2024-1-2",
        last_entry_date="2024-01-02",
    )

    with pytest.raises(StrategyValidationError, match="first_entry_date"):
        _normalize(holding)


def test_first_entry_date_must_not_follow_last_entry_date() -> None:
    holding = StrategyHoldingState(
        asset_id="A",
        current_weight=0.125,
        entry_count=1,
        first_entry_date="2024-01-03",
        last_entry_date="2024-01-02",
    )

    with pytest.raises(StrategyValidationError, match="first_entry_date"):
        _normalize(holding)
