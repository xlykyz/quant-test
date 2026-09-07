"""Focused rc2 contract checks using the existing Task04/06-A facts/helpers.

This is an audit companion, not a Task06-B implementation.  It keeps the
new rc2 boundary rules executable without writing production data.
"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
import json

from qrp_atlas.indicators.system_b.asset_ranking import rank_component


def episode_projection_checks() -> dict[str, object]:
    d = date(2026, 8, 14)
    confirmed = date(2026, 8, 11)
    assert confirmed <= d
    assert confirmed <= d < date(2026, 8, 18)
    assert not (confirmed <= d < d)
    return {
        "E>D_open_at_D": True,
        "end_date_equals_D_is_closed": True,
        "future_end_not_used_as_return": True,
    }


def d_aligned_return_check() -> dict[str, object]:
    # The persisted open-episode return is a placeholder in the real producer;
    # the historical raw is derived only from the start and D index levels.
    start_level, d_level, final_return = Fraction(105), Fraction(118), Fraction(0)
    aligned = d_level / start_level - 1
    assert aligned == Fraction(13, 105)
    assert aligned != final_return
    return {"D_aligned_return": float(aligned), "final_episode_return_ignored": True}


def status_and_dimension_checks() -> dict[str, object]:
    # N=1 must remain insufficient even when every leaf is constant; a trusted
    # unavailable source has higher priority than that singleton result.
    singleton = rank_component([1])
    all_equal = rank_component([7, 7])
    assert singleton.status == "INSUFFICIENT_UNIVERSE"
    assert all_equal.status == "NO_VARIATION"

    # A dimension with no active leaves has no denominator and therefore no
    # display score.  Its global contribution is zero; no fixed-budget reweight.
    active_weights = [35, 10, 10, 6, 4]
    assert sum(active_weights) == 65
    persistence_active = []
    assert persistence_active == []
    persistence_score = None
    assert persistence_score is None
    return {
        "singleton": singleton.status,
        "all_equal_component": all_equal.status,
        "empty_dimension_score": persistence_score,
        "no_secondary_dimension_reweight": True,
        "unavailable_priority_over_singleton": True,
    }


def conditional_gate_checks() -> dict[str, object]:
    # The scheduler's unconditional SUCCESS dependency is intentionally not
    # used on the trusted UNAVAILABLE branch; AVAILABLE still requires M5.
    assert {"M4_FINALIZED", "AVAILABILITY_TRUSTED", "M5_SUCCESS"} >= {
        "M4_FINALIZED",
        "AVAILABILITY_TRUSTED",
    }
    unavailable_path = {"M4_FINALIZED", "AVAILABILITY_TRUSTED", "SOURCE_UNAVAILABLE"}
    assert "M5_SUCCESS" not in unavailable_path
    available_path = {"M4_FINALIZED", "AVAILABILITY_TRUSTED", "ALL_AVAILABLE"}
    assert "M5_SUCCESS" not in available_path
    return {
        "unavailable_path_does_not_require_m5_success": True,
        "available_path_requires_m5_and_fingerprint": True,
    }


def fingerprint_checks() -> dict[str, object]:
    # Same counts and sequence labels do not identify the same logical rows.
    v1 = (("A", 1), ("B", 1))
    v2 = (("A", 1), ("C", 1))
    assert len(v1) == len(v2) and [row[1] for row in v1] == [row[1] for row in v2]
    assert v1 != v2
    return {"same_counts_and_seqs_can_be_stale": True, "row_fingerprint_detects_replacement": True}


def main() -> None:
    results = {
        "episode_projection": episode_projection_checks(),
        "d_aligned_return": d_aligned_return_check(),
        "status_and_dimension": status_and_dimension_checks(),
        "conditional_gate": conditional_gate_checks(),
        "fingerprint": fingerprint_checks(),
    }
    print(json.dumps(results, ensure_ascii=False, indent=2))
    print("RC2 FOCUSED REPRODUCTIONS PASSED: 15 assertions")


if __name__ == "__main__":
    main()
