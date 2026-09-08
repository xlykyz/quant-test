"""Task07-C portfolio constraint resolution unit tests."""

from __future__ import annotations

import pytest

from qrp_atlas.strategies import (
    PORTFOLIO_WEIGHT_TOLERANCE,
    StrategyAction,
    StrategyDecision,
    StrategyHoldingState,
    StrategyPortfolioTargetPosition,
    StrategyValidationError,
    resolve_system_b_portfolio_target,
)
from qrp_atlas.strategies.builtin.system_b_portfolio import (
    ADD_ENTRY_SELECTED,
    ADD_SINGLE_ASSET_CAP_EXCEEDED,
    ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED,
    NEW_DISTINCT_CAPACITY_INSUFFICIENT,
    NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED,
    NEW_ENTRY_SELECTED,
    PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT,
    POSITION_PRESERVED,
)


TRADE_DATE = "2024-01-10"
CODE = "system_b_portfolio"
VERSION = "1.0.0"


def _holding(
    asset_id: str,
    weight: float = 0.125,
    *,
    entry_count: int = 1,
) -> StrategyHoldingState:
    return StrategyHoldingState(
        asset_id=asset_id,
        current_weight=weight,
        entry_count=entry_count,
        first_entry_date="2024-01-02",
        last_entry_date="2024-01-02",
    )


def _decision(
    asset_id: str,
    action: StrategyAction,
    *,
    score: float | None = None,
    entry_kind: str = "NONE",
    reason_code: str = "TEST_REASON",
    trade_date: str = TRADE_DATE,
    strategy_code: str = CODE,
    strategy_version: str = VERSION,
    evidence: dict[str, object] | None = None,
) -> StrategyDecision:
    ev: dict[str, object] = {
        "entry_kind": entry_kind,
        "score_calculation_version": "score-v1",
        "rule_version_set_id": "rules-v1",
        "parameter_set_id": "params-v1",
        "input_snapshot_id": "snapshot-v1",
    }
    if evidence:
        ev.update(evidence)
    return StrategyDecision(
        trade_date=trade_date,
        asset_id=asset_id,
        action=action,
        direction="long",
        strategy_code=strategy_code,
        strategy_version=strategy_version,
        reason_code=reason_code,
        score=score,
        weight=None,
        evidence=ev,
    )


def _resolve(
    holdings: dict[str, StrategyHoldingState],
    decisions: list[StrategyDecision],
    trade_date: str = TRADE_DATE,
    strategy_code: str = CODE,
    strategy_version: str = VERSION,
):
    return resolve_system_b_portfolio_target(
        trade_date=trade_date,
        holdings=holdings,
        decisions=decisions,
        strategy_code=strategy_code,
        strategy_version=strategy_version,
    )


# ---------------------------------------------------------------------------
# 1. Base target / EXIT tests (§21.1)
# ---------------------------------------------------------------------------

def test_hold_preserves_current_weight() -> None:
    holdings = {"A": _holding("A", weight=0.18)}
    decisions = [_decision("A", StrategyAction.HOLD, score=10.0)]
    target = _resolve(holdings, decisions)

    assert len(target.positions) == 1
    pos = target.positions[0]
    assert pos.asset_id == "A"
    assert pos.target_weight == 0.18
    assert pos.reason_code == POSITION_PRESERVED
    assert pos.evidence["entry_increment"] == 0.0
    assert pos.evidence["prior_weight"] == 0.18


def test_held_no_action_preserves_current_weight() -> None:
    holdings = {"A": _holding("A", weight=0.15)}
    decisions = [_decision("A", StrategyAction.NO_ACTION, reason_code="EXIT_STATUS_UNAVAILABLE")]
    target = _resolve(holdings, decisions)

    assert len(target.positions) == 1
    assert target.positions[0].target_weight == 0.15
    assert target.positions[0].reason_code == POSITION_PRESERVED


def test_exit_omitted_from_target_and_releases_slot() -> None:
    # 6 initial holdings, one exits -> retained = 5, releases 1 slot for NEW
    holdings = {f"H{i}": _holding(f"H{i}", 0.125) for i in range(1, 7)}
    decisions = [_decision(f"H{i}", StrategyAction.HOLD, score=10.0) for i in range(1, 6)]
    decisions.append(_decision("H6", StrategyAction.EXIT, reason_code="MA5_EXIT"))
    decisions.append(_decision("NEW1", StrategyAction.ENTER, score=90.0, entry_kind="NEW"))

    target = _resolve(holdings, decisions)
    pos_assets = {p.asset_id for p in target.positions}

    assert "H6" not in pos_assets  # EXIT omitted
    assert "NEW1" in pos_assets  # slot successfully taken
    assert len(target.positions) == 6


def test_same_day_exit_cannot_have_enter_decision() -> None:
    holdings = {"A": _holding("A")}
    # Trying to pass an ENTER decision for an asset marked EXIT is fail-closed
    decisions = [_decision("A", StrategyAction.EXIT)]
    # Duplicate decision for A with ENTER
    with pytest.raises(StrategyValidationError, match="duplicate"):
        _resolve(holdings, decisions + [_decision("A", StrategyAction.ENTER, score=100.0, entry_kind="ADD")])


# ---------------------------------------------------------------------------
# 2. Fixed 1/8 increments tests (§21.2)
# ---------------------------------------------------------------------------

def test_new_selected_gets_exact_one_eighth() -> None:
    target = _resolve({}, [_decision("N1", StrategyAction.ENTER, score=90.0, entry_kind="NEW")])
    assert len(target.positions) == 1
    pos = target.positions[0]
    assert pos.target_weight == 0.125
    assert pos.reason_code == NEW_ENTRY_SELECTED
    assert pos.evidence["entry_increment"] == 0.125
    assert pos.evidence["prior_weight"] == 0.0


def test_add_selected_adds_exact_one_eighth() -> None:
    holdings = {"A": _holding("A", weight=0.125, entry_count=1)}
    decisions = [_decision("A", StrategyAction.ENTER, score=90.0, entry_kind="ADD")]
    target = _resolve(holdings, decisions)

    assert len(target.positions) == 1
    pos = target.positions[0]
    assert pos.target_weight == 0.25
    assert pos.reason_code == ADD_ENTRY_SELECTED
    assert pos.evidence["entry_increment"] == 0.125
    assert pos.evidence["prior_weight"] == 0.125
    assert pos.evidence["entry_count_after_if_selected"] == 2


def test_remaining_budget_0_1249_does_not_permit_partial_entry() -> None:
    # base gross = 0.8751 -> remaining budget = 0.1249 < 0.125
    holdings = {"A": _holding("A", weight=0.8751)}
    decisions = [
        _decision("A", StrategyAction.HOLD),
        _decision("N", StrategyAction.ENTER, score=90.0, entry_kind="NEW"),
    ]
    target = _resolve(holdings, decisions)

    assert len(target.positions) == 1  # N not selected
    assert target.positions[0].asset_id == "A"
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=N|reason={PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT}",
    )


# ---------------------------------------------------------------------------
# 3. Single asset cap / ADD invariant tests (§21.3)
# ---------------------------------------------------------------------------

def test_add_cap_30_exact_boundary() -> None:
    # 0.175 + 0.125 = 0.30 <= 0.30 -> selected
    holdings = {"A": _holding("A", weight=0.175, entry_count=1)}
    decisions = [_decision("A", StrategyAction.ENTER, score=90.0, entry_kind="ADD")]
    target = _resolve(holdings, decisions)

    assert target.positions[0].target_weight == 0.30
    assert target.positions[0].reason_code == ADD_ENTRY_SELECTED
    assert not target.diagnostics


def test_add_cap_exceeded_rejects_add_and_preserves_position() -> None:
    # 0.18 + 0.125 = 0.305 > 0.30 -> rejected
    holdings = {"A": _holding("A", weight=0.18, entry_count=1)}
    decisions = [_decision("A", StrategyAction.ENTER, score=90.0, entry_kind="ADD")]
    target = _resolve(holdings, decisions)

    assert target.positions[0].target_weight == 0.18
    assert target.positions[0].reason_code == POSITION_PRESERVED
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=A|reason={ADD_SINGLE_ASSET_CAP_EXCEEDED}",
    )


def test_passive_holding_above_30_preserved_without_trim() -> None:
    holdings = {"A": _holding("A", weight=0.35, entry_count=2)}
    decisions = [_decision("A", StrategyAction.HOLD)]
    target = _resolve(holdings, decisions)

    assert target.positions[0].target_weight == 0.35
    assert target.positions[0].reason_code == POSITION_PRESERVED
    assert not target.diagnostics


def test_add_invariant_violation_entry_count_2_fails_closed() -> None:
    # entry_count >= 2 + ENTER(ADD) is contract violation -> StrategyValidationError
    holdings = {"A": _holding("A", weight=0.125, entry_count=2)}
    decisions = [_decision("A", StrategyAction.ENTER, score=90.0, entry_kind="ADD")]

    with pytest.raises(StrategyValidationError, match="entry_count"):
        _resolve(holdings, decisions)


def test_add_asset_not_in_holdings_fails_closed() -> None:
    # ENTER(ADD) for an asset not initially held is contract violation
    decisions = [_decision("GHOST", StrategyAction.ENTER, score=90.0, entry_kind="ADD")]
    with pytest.raises(StrategyValidationError, match="ADD entry asset GHOST is not in initial holdings"):
        _resolve({}, decisions)


def test_new_asset_in_holdings_fails_closed() -> None:
    # ENTER(NEW) for an asset already held is contract violation
    holdings = {"A": _holding("A")}
    decisions = [_decision("A", StrategyAction.ENTER, score=90.0, entry_kind="NEW")]
    with pytest.raises(StrategyValidationError, match="NEW entry asset A is in initial holdings"):
        _resolve(holdings, decisions)


# ---------------------------------------------------------------------------
# 4. Distinct holdings & slot resolution tests (§21.4)
# ---------------------------------------------------------------------------

def test_slot_exhaustion_is_insufficient_not_tie() -> None:
    # retained = 5, available slots = 1.
    # NEW A (95.0) takes the only slot.
    # NEW B (80.0) finds remaining_slots == 0 -> NEW_DISTINCT_CAPACITY_INSUFFICIENT
    holdings = {f"H{i}": _holding(f"H{i}", 0.10) for i in range(1, 6)}
    decisions = [_decision(f"H{i}", StrategyAction.HOLD) for i in range(1, 6)]
    decisions.extend([
        _decision("NEW_A", StrategyAction.ENTER, score=95.0, entry_kind="NEW"),
        _decision("NEW_B", StrategyAction.ENTER, score=80.0, entry_kind="NEW"),
    ])

    target = _resolve(holdings, decisions)
    pos_map = {p.asset_id: p for p in target.positions}

    assert "NEW_A" in pos_map
    assert "NEW_B" not in pos_map
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=NEW_B|reason={NEW_DISTINCT_CAPACITY_INSUFFICIENT}",
    )


def test_true_cutoff_tie_rejects_entire_group_and_blocks_leapfrog() -> None:
    # retained = 5, available slots = 1.
    # NEW A (95.0) and NEW B (95.0) compete for 1 slot -> TIE!
    # NEW C (80.0) is lower score -> must not leapfrog!
    holdings = {f"H{i}": _holding(f"H{i}", 0.10) for i in range(1, 6)}
    decisions = [_decision(f"H{i}", StrategyAction.HOLD) for i in range(1, 6)]
    decisions.extend([
        _decision("NEW_A", StrategyAction.ENTER, score=95.0, entry_kind="NEW"),
        _decision("NEW_B", StrategyAction.ENTER, score=95.0, entry_kind="NEW"),
        _decision("NEW_C", StrategyAction.ENTER, score=80.0, entry_kind="NEW"),
    ])

    target = _resolve(holdings, decisions)
    pos_assets = {p.asset_id for p in target.positions}

    assert "NEW_A" not in pos_assets
    assert "NEW_B" not in pos_assets
    assert "NEW_C" not in pos_assets
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=NEW_A|reason={NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED}",
        f"SYSTEM_B_TARGET_REJECTION|asset_id=NEW_B|reason={NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED}",
        f"SYSTEM_B_TARGET_REJECTION|asset_id=NEW_C|reason={NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED}",
    )


def test_retained_6_blocks_new_but_allows_add() -> None:
    # retained = 6, available slots = 0
    # NEW is rejected with INSUFFICIENT
    # ADD does not consume distinct slots, so ADD can still enter
    holdings = {f"H{i}": _holding(f"H{i}", 0.10) for i in range(1, 7)}
    decisions = [_decision(f"H{i}", StrategyAction.HOLD) for i in range(2, 7)]
    decisions.append(_decision("H1", StrategyAction.ENTER, score=100.0, entry_kind="ADD"))
    decisions.append(_decision("NEW1", StrategyAction.ENTER, score=90.0, entry_kind="NEW"))

    target = _resolve(holdings, decisions)
    pos_map = {p.asset_id: p for p in target.positions}

    assert pos_map["H1"].target_weight == 0.10 + 0.125
    assert pos_map["H1"].reason_code == ADD_ENTRY_SELECTED
    assert "NEW1" not in pos_map
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=NEW1|reason={NEW_DISTINCT_CAPACITY_INSUFFICIENT}",
    )


def test_retained_greater_than_6_fails_closed() -> None:
    holdings = {f"H{i}": _holding(f"H{i}", 0.10) for i in range(1, 8)}
    decisions = [_decision(f"H{i}", StrategyAction.HOLD) for i in range(1, 8)]

    with pytest.raises(StrategyValidationError, match="retained holdings count 7 exceeds 6"):
        _resolve(holdings, decisions)


# ---------------------------------------------------------------------------
# 5. Score priority & Exact equality tests (§21.5, §21.6)
# ---------------------------------------------------------------------------

def test_exact_equal_scores_vs_very_close_scores() -> None:
    # Two scores differ by 1e-15: they are NOT equal and must not be grouped into a tie!
    holdings = {f"H{i}": _holding(f"H{i}", 0.10) for i in range(1, 6)}  # 1 slot left
    s1 = 95.0 + 1e-14
    s2 = 95.0
    decisions = [_decision(f"H{i}", StrategyAction.HOLD) for i in range(1, 6)]
    decisions.extend([
        _decision("NEW_A", StrategyAction.ENTER, score=s1, entry_kind="NEW"),
        _decision("NEW_B", StrategyAction.ENTER, score=s2, entry_kind="NEW"),
    ])

    target = _resolve(holdings, decisions)
    pos_assets = {p.asset_id for p in target.positions}

    # NEW_A has strictly higher score -> gets the 1 slot!
    assert "NEW_A" in pos_assets
    assert "NEW_B" not in pos_assets
    # NEW_B is ordinary insufficient, NOT tie!
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=NEW_B|reason={NEW_DISTINCT_CAPACITY_INSUFFICIENT}",
    )


def test_non_finite_enter_score_fails_closed() -> None:
    with pytest.raises(StrategyValidationError, match="non-finite score"):
        _resolve({}, [_decision("N", StrategyAction.ENTER, score=float("nan"), entry_kind="NEW")])

    with pytest.raises(StrategyValidationError, match="non-finite score"):
        _resolve({}, [_decision("N", StrategyAction.ENTER, score=float("inf"), entry_kind="NEW")])

    with pytest.raises(StrategyValidationError, match="non-finite score"):
        _resolve({}, [_decision("N", StrategyAction.ENTER, score=None, entry_kind="NEW")])


def test_input_row_order_does_not_affect_outcome() -> None:
    d1 = _decision("N_B", StrategyAction.ENTER, score=90.0, entry_kind="NEW")
    d2 = _decision("N_A", StrategyAction.ENTER, score=90.0, entry_kind="NEW")

    # Order 1
    t1 = _resolve({}, [d1, d2])
    # Order 2
    t2 = _resolve({}, [d2, d1])

    assert [p.asset_id for p in t1.positions] == [p.asset_id for p in t2.positions]
    assert t1.diagnostics == t2.diagnostics


# ---------------------------------------------------------------------------
# 6. Gross capacity & tie resolution tests (§21.7)
# ---------------------------------------------------------------------------

def test_gross_capacity_resolution_two_fit_third_exceeds() -> None:
    # base gross = 0.70. Budget = 0.30.
    # N1 (100) needs 0.125 -> remaining 0.175
    # N2 (90) needs 0.125 -> remaining 0.050
    # N3 (80) needs 0.125 > 0.050 -> rejected for PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT
    holdings = {"H1": _holding("H1", 0.70)}
    decisions = [
        _decision("H1", StrategyAction.HOLD),
        _decision("N1", StrategyAction.ENTER, score=100.0, entry_kind="NEW"),
        _decision("N2", StrategyAction.ENTER, score=90.0, entry_kind="NEW"),
        _decision("N3", StrategyAction.ENTER, score=80.0, entry_kind="NEW"),
    ]

    target = _resolve(holdings, decisions)
    pos_assets = {p.asset_id for p in target.positions}

    assert "N1" in pos_assets
    assert "N2" in pos_assets
    assert "N3" not in pos_assets
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=N3|reason={PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT}",
    )


def test_gross_capacity_tie_unresolved() -> None:
    # base gross = 0.80. Budget = 0.20.
    # N1 and N2 both score 90.0 (need 0.25 > 0.20, but 0.20 >= 0.125) -> TIE!
    holdings = {"H1": _holding("H1", 0.80)}
    decisions = [
        _decision("H1", StrategyAction.HOLD),
        _decision("N1", StrategyAction.ENTER, score=90.0, entry_kind="NEW"),
        _decision("N2", StrategyAction.ENTER, score=90.0, entry_kind="NEW"),
    ]

    target = _resolve(holdings, decisions)
    pos_assets = {p.asset_id for p in target.positions}

    assert "N1" not in pos_assets
    assert "N2" not in pos_assets
    assert target.diagnostics == (
        f"SYSTEM_B_TARGET_REJECTION|asset_id=N1|reason={ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED}",
        f"SYSTEM_B_TARGET_REJECTION|asset_id=N2|reason={ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED}",
    )


# ---------------------------------------------------------------------------
# 7. Floating boundary alignment tests (§21.8)
# ---------------------------------------------------------------------------

def test_gross_weight_exact_one_point_zero_is_valid() -> None:
    # 4 holdings * 0.1875 = 0.75 gross. Max 6 distinct -> 2 slots for NEW.
    # 2 NEW * 0.125 = 0.25. Total gross = 0.75 + 0.25 = 1.0. Total distinct = 6 <= 6.
    holdings = {f"H{i}": _holding(f"H{i}", 0.1875) for i in range(1, 5)}
    decisions = [_decision(f"H{i}", StrategyAction.HOLD) for i in range(1, 5)]
    decisions.append(_decision("N1", StrategyAction.ENTER, score=90.0, entry_kind="NEW"))
    decisions.append(_decision("N2", StrategyAction.ENTER, score=80.0, entry_kind="NEW"))

    target = _resolve(holdings, decisions)
    assert len(target.positions) == 6
    assert sum(p.target_weight for p in target.positions) == 1.0


def test_gross_weight_within_tolerance_is_valid() -> None:
    # 1.0 + 1e-12
    holdings = {"H1": _holding("H1", 1.0 + PORTFOLIO_WEIGHT_TOLERANCE)}
    decisions = [_decision("H1", StrategyAction.HOLD)]
    target = _resolve(holdings, decisions)
    assert len(target.positions) == 1


def test_gross_weight_exceeding_tolerance_fails_closed() -> None:
    # 1.0 + 2e-12 -> StrategyValidationError
    holdings = {"H1": _holding("H1", 1.0 + 2e-12)}
    decisions = [_decision("H1", StrategyAction.HOLD)]
    with pytest.raises(StrategyValidationError, match="gross weight"):
        _resolve(holdings, decisions)


# ---------------------------------------------------------------------------
# 8. Full snapshot invariants tests (§21.9)
# ---------------------------------------------------------------------------

def test_full_snapshot_ordering_and_evidence() -> None:
    holdings = {"B": _holding("B", 0.125, entry_count=1)}
    decisions = [
        _decision("B", StrategyAction.ENTER, score=95.0, entry_kind="ADD"),
        _decision("A", StrategyAction.ENTER, score=90.0, entry_kind="NEW"),
    ]
    target = _resolve(holdings, decisions)

    # Must be canonically sorted by asset_id ASC: A, then B
    assert [p.asset_id for p in target.positions] == ["A", "B"]
    pos_a = target.positions[0]
    assert pos_a.target_weight == 0.125
    assert pos_a.reason_code == NEW_ENTRY_SELECTED
    assert pos_a.evidence["score_calculation_version"] == "score-v1"

    pos_b = target.positions[1]
    assert pos_b.target_weight == 0.25
    assert pos_b.reason_code == ADD_ENTRY_SELECTED
    assert pos_b.evidence["entry_count_before"] == 1
    assert pos_b.evidence["entry_count_after_if_selected"] == 2
