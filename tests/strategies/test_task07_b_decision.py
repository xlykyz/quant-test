"""Task07-B System B holding / entry / exit decision tests."""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.strategies import StrategyAction, StrategyHoldingState, StrategyValidationError
from qrp_atlas.strategies.builtin.system_b_decision import (
    SystemBAuthorizationStatus,
    SystemBEntryEligibilityStatus,
    SystemBExitStatus,
    SystemBScoreProvenanceStatus,
    SystemBSupervisionStatus,
    evaluate_system_b_holding_entry_exit,
    normalize_system_b_decision_input,
)


TRADE_DATE = "2024-01-10"


def _holding(asset_id: str, *, entry_count: int = 1) -> StrategyHoldingState:
    return StrategyHoldingState(
        asset_id=asset_id,
        current_weight=0.125,
        entry_count=entry_count,
        first_entry_date="2024-01-02",
        last_entry_date="2024-01-02",
    )


def _provenance(**overrides: object) -> dict[str, object]:
    result: dict[str, object] = {
        "trade_date": TRADE_DATE,
        "score_calculation_version": "score-v1",
        "rule_version_set_id": "rules-v1",
        "parameter_set_id": "params-v1",
        "input_snapshot_id": "snapshot-v1",
    }
    result.update(overrides)
    return result


def _row(
    asset_id: str,
    score: object,
    *,
    exit_triggered: object = False,
    eligible: object = True,
    supervision: object = "INACTIVE",
    **extra: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": asset_id,
        "trade_date": TRADE_DATE,
        "comparison_score": score,
        "entry_eligible": eligible,
        "system_b_exit_triggered": exit_triggered,
        "severe_abnormal_supervision_status": supervision,
    }
    row.update(extra)
    return row


def _evaluate(
    rows: list[dict[str, object]],
    *,
    holdings: dict[str, StrategyHoldingState] | None = None,
    candidates: set[str] | None = None,
    authorization: object = True,
    provenance: object = None,
):
    holdings = holdings or {}
    if candidates is None:
        candidates = {str(row["ticker"]) for row in rows} - set(holdings)
    normalized = normalize_system_b_decision_input(
        pd.DataFrame(rows),
        holdings=holdings,
        candidate_asset_ids=candidates,
        authorization=authorization,
        comparison_score_provenance=_provenance() if provenance is None else provenance,
    )
    decisions = evaluate_system_b_holding_entry_exit(
        normalized,
        strategy_code="system_b_portfolio",
        strategy_version="1.0.0",
    )
    return normalized, {decision.asset_id: decision for decision in decisions}


def test_same_day_exit_is_terminal_even_if_asset_is_best_entry_candidate() -> None:
    _, decisions = _evaluate(
        [_row("A", 999.0, exit_triggered=True)],
        holdings={"A": _holding("A")},
        candidates={"A"},
    )

    decision = decisions["A"]
    assert decision.action is StrategyAction.EXIT
    assert decision.reason_code == "MA5_TWO_ACTUAL_TRADING_DAYS_EXIT"
    assert decision.evidence["same_day_exit_terminal"] is True
    assert decision.evidence["entry_kind"] == "NONE"


def test_exit_releases_capacity_for_other_new_asset_and_uses_retained_five_threshold() -> None:
    holdings = {f"H{i}": _holding(f"H{i}") for i in range(1, 7)}
    rows = [
        _row("H1", 1.0),
        _row("H2", 2.0),
        _row("H3", 3.0),
        _row("H4", 4.0),
        _row("H5", 5.0),
        _row("H6", 6.0, exit_triggered=True),
        _row("N", 5.5),
    ]

    _, decisions = _evaluate(rows, holdings=holdings, candidates={"N"})

    assert decisions["H6"].action is StrategyAction.EXIT
    assert decisions["N"].action is StrategyAction.ENTER
    assert decisions["N"].reason_code == "NEW_ENTRY_ABOVE_ALL_HOLDING_SCORES"
    assert decisions["N"].evidence["retained_holding_count"] == 5
    assert decisions["N"].evidence["relative_score_threshold"] == 5.0


def test_new_entry_strict_threshold_for_one_to_three_holdings() -> None:
    holdings = {"H1": _holding("H1"), "H2": _holding("H2")}
    rows = [_row("H1", 2.0), _row("H2", 4.0), _row("PASS", 2.1), _row("TIE", 2.0)]

    _, decisions = _evaluate(rows, holdings=holdings, candidates={"PASS", "TIE"})

    assert decisions["PASS"].action is StrategyAction.ENTER
    assert decisions["PASS"].reason_code == "NEW_ENTRY_ABOVE_MIN_HOLDING_SCORE"
    assert decisions["TIE"].action is StrategyAction.NO_ACTION
    assert decisions["TIE"].reason_code == "NEW_ENTRY_SCORE_THRESHOLD_NOT_MET"


def test_new_entry_requires_above_max_for_four_to_five_holdings() -> None:
    holdings = {f"H{i}": _holding(f"H{i}") for i in range(1, 5)}
    rows = [_row(f"H{i}", float(i)) for i in range(1, 5)]
    rows.extend([_row("PASS", 4.1), _row("TIE", 4.0)])

    _, decisions = _evaluate(rows, holdings=holdings, candidates={"PASS", "TIE"})

    assert decisions["PASS"].action is StrategyAction.ENTER
    assert decisions["PASS"].reason_code == "NEW_ENTRY_ABOVE_ALL_HOLDING_SCORES"
    assert decisions["TIE"].action is StrategyAction.NO_ACTION


def test_six_retained_holdings_block_new_stock_but_do_not_block_add() -> None:
    holdings = {f"H{i}": _holding(f"H{i}") for i in range(1, 7)}
    rows = [_row(f"H{i}", float(i)) for i in range(1, 7)]
    rows.append(_row("N", 100.0))

    _, decisions = _evaluate(rows, holdings=holdings, candidates={"N"})

    assert decisions["N"].action is StrategyAction.NO_ACTION
    assert decisions["N"].reason_code == "DISTINCT_HOLDING_CAP_REACHED"


def test_add_requires_existing_holding_to_be_top_qualified_score() -> None:
    holdings = {"A": _holding("A"), "B": _holding("B")}
    rows = [_row("A", 10.0), _row("B", 9.0), _row("N", 8.0)]

    _, decisions = _evaluate(rows, holdings=holdings, candidates={"N"})

    assert decisions["A"].action is StrategyAction.ENTER
    assert decisions["A"].reason_code == "ADD_ENTRY_ELIGIBLE_TOP_SCORE"
    assert decisions["A"].evidence["entry_kind"] == "ADD"
    assert decisions["B"].action is StrategyAction.HOLD
    assert decisions["B"].reason_code == "ADD_ENTRY_NOT_TOP_SCORE"


def test_add_limit_keeps_second_entry_holding_without_third_buy() -> None:
    holdings = {"A": _holding("A", entry_count=2)}
    _, decisions = _evaluate([_row("A", 100.0)], holdings=holdings, candidates=set())

    assert decisions["A"].action is StrategyAction.HOLD
    assert decisions["A"].reason_code == "ADD_ENTRY_LIMIT_REACHED"


def test_authorization_and_supervision_block_entry_but_not_exit() -> None:
    holdings = {"EXIT": _holding("EXIT"), "HOLD": _holding("HOLD")}
    rows = [
        _row("EXIT", 1.0, exit_triggered=True, supervision="ACTIVE"),
        _row("HOLD", 2.0, supervision="ACTIVE"),
        _row("NEW", 3.0, supervision="ACTIVE"),
    ]

    _, decisions = _evaluate(
        rows,
        holdings=holdings,
        candidates={"NEW"},
        authorization=False,
    )

    assert decisions["EXIT"].action is StrategyAction.EXIT
    assert decisions["HOLD"].action is StrategyAction.HOLD
    assert decisions["NEW"].action is StrategyAction.NO_ACTION
    assert decisions["NEW"].reason_code == "NEW_POSITION_AUTHORIZATION_DENIED"


def test_exit_unknown_is_explicit_and_preserves_retained_capacity() -> None:
    normalized, decisions = _evaluate(
        [_row("A", 1.0, exit_triggered=pd.NA)],
        holdings={"A": _holding("A")},
        candidates=set(),
    )

    assert normalized.asset_facts["A"].exit_status is SystemBExitStatus.UNAVAILABLE
    assert decisions["A"].action is StrategyAction.NO_ACTION
    assert decisions["A"].reason_code == "EXIT_STATUS_UNAVAILABLE"
    assert decisions["A"].evidence["was_retained"] is True


def test_normalizer_maps_all_nullable_states_to_explicit_unavailable() -> None:
    normalized, _ = _evaluate(
        [_row("A", None, exit_triggered=pd.NA, eligible=pd.NA, supervision=None)],
        holdings={},
        candidates={"A"},
        authorization=None,
    )
    fact = normalized.asset_facts["A"]

    assert fact.exit_status is SystemBExitStatus.UNAVAILABLE
    assert fact.entry_eligibility_status is SystemBEntryEligibilityStatus.UNAVAILABLE
    assert fact.supervision_status is SystemBSupervisionStatus.UNAVAILABLE
    assert normalized.authorization_status is SystemBAuthorizationStatus.UNAVAILABLE
    assert fact.comparison_score is None


def test_invalid_state_value_fails_normalization() -> None:
    with pytest.raises(StrategyValidationError, match="supervision"):
        _evaluate([_row("A", 1.0, supervision="MAYBE")], candidates={"A"})


def test_domain_must_exactly_cover_holdings_union_candidates() -> None:
    with pytest.raises(StrategyValidationError, match="asset domain mismatch"):
        normalize_system_b_decision_input(
            pd.DataFrame([_row("A", 1.0)]),
            holdings={"H": _holding("H")},
            candidate_asset_ids={"A"},
            authorization=True,
            comparison_score_provenance=_provenance(),
        )


def test_duplicate_asset_row_fails_normalization() -> None:
    with pytest.raises(StrategyValidationError, match="duplicate"):
        _evaluate([_row("A", 1.0), _row("A", 2.0)], candidates={"A"})


@pytest.mark.parametrize("score", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_score_is_not_accepted_as_valid_score(score: float) -> None:
    with pytest.raises(StrategyValidationError, match="finite"):
        _evaluate([_row("A", score)], candidates={"A"})


def test_missing_provenance_blocks_entry_but_does_not_block_exit() -> None:
    holdings = {"H": _holding("H")}
    rows = [_row("H", 1.0, exit_triggered=True), _row("N", 2.0)]

    normalized, decisions = _evaluate(
        rows,
        holdings=holdings,
        candidates={"N"},
        provenance={},
    )

    assert normalized.comparison_score_provenance.status is SystemBScoreProvenanceStatus.UNAVAILABLE
    assert decisions["H"].action is StrategyAction.EXIT
    assert decisions["N"].action is StrategyAction.NO_ACTION
    assert decisions["N"].reason_code == "COMPARISON_SCORE_PROVENANCE_UNAVAILABLE"


def test_row_level_provenance_mismatch_blocks_new_and_add() -> None:
    holdings = {"H": _holding("H")}
    rows = [
        _row("H", 2.0, score_calculation_version="score-v1"),
        _row("N", 3.0, score_calculation_version="score-v2"),
    ]

    normalized, decisions = _evaluate(rows, holdings=holdings, candidates={"N"})

    assert normalized.comparison_score_provenance.status is SystemBScoreProvenanceStatus.MISMATCH
    assert decisions["H"].action is StrategyAction.HOLD
    assert decisions["H"].reason_code == "COMPARISON_SCORE_PROVENANCE_MISMATCH"
    assert decisions["N"].action is StrategyAction.NO_ACTION
    assert decisions["N"].reason_code == "COMPARISON_SCORE_PROVENANCE_MISMATCH"


def test_zero_score_is_valid_new_entry_and_is_preserved_in_decision_and_evidence() -> None:
    _, decisions = _evaluate([_row("ZERO", 0.0)], candidates={"ZERO"})

    decision = decisions["ZERO"]
    assert decision.action is StrategyAction.ENTER
    assert decision.score == 0.0
    assert decision.evidence["comparison_score"] == 0.0
    assert decision.evidence["comparison_score"] is not None


def test_zero_score_is_valid_add_and_is_preserved_in_decision_and_evidence() -> None:
    holdings = {"ZERO": _holding("ZERO")}
    rows = [_row("ZERO", 0.0), _row("NEW", -1.0)]

    _, decisions = _evaluate(rows, holdings=holdings, candidates={"NEW"})

    decision = decisions["ZERO"]
    assert decision.action is StrategyAction.ENTER
    assert decision.reason_code == "ADD_ENTRY_ELIGIBLE_TOP_SCORE"
    assert decision.score == 0.0
    assert decision.evidence["comparison_score"] == 0.0


def test_co_top_holdings_are_not_broken_by_ticker_order() -> None:
    holdings = {"Z": _holding("Z"), "A": _holding("A")}
    rows = [_row("Z", 5.0), _row("A", 5.0)]

    _, decisions = _evaluate(rows, holdings=holdings, candidates=set())

    assert decisions["A"].action is StrategyAction.ENTER
    assert decisions["Z"].action is StrategyAction.ENTER
    assert decisions["A"].reason_code == "ADD_ENTRY_ELIGIBLE_TOP_SCORE"
    assert decisions["Z"].reason_code == "ADD_ENTRY_ELIGIBLE_TOP_SCORE"
