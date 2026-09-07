"""Targeted unit tests for Task06-B pure Theme Trend Rank calculations."""

from datetime import date
from fractions import Fraction
import math

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    COLLECTION_ID,
    IS_ABOVE_OR_EQUAL_MA5,
    POPULARITY_AVAILABLE,
    POPULARITY_UNAVAILABLE,
    RANK_ELIGIBLE,
    RANK_ELIGIBILITY_REASON,
    THEME_COMPONENT_EPISODE_ABOVE_MA5_RATIO,
    THEME_COMPONENT_EPISODE_DURATION,
    THEME_COMPONENT_EPISODE_RETURN,
    THEME_COMPONENT_HOT_APPEARANCE_RATE,
    THEME_COMPONENT_HOT_STOCK_RATIO,
    THEME_COMPONENT_LIMIT_UP_DIFFUSION,
    THEME_COMPONENT_THEME_DAILY_RETURN,
    THEME_DIMENSION_CURRENT_STRUCTURE,
    THEME_DIMENSION_POPULARITY_SUPPORT,
    THEME_DIMENSION_TREND_PERSISTENCE,
    THEME_DIMENSION_TREND_STRENGTH,
    THEME_ID,
    THEME_RANK,
    THEME_RANK_ELIGIBLE,
    THEME_RANK_INCOMPLETE_INPUT,
    THEME_RANK_INSUFFICIENT_UNIVERSE,
    THEME_RANK_NO_EFFECTIVE_MEMBERS,
    THEME_RANK_NO_OPEN_EPISODE,
    THEME_RANK_NO_VARIATION,
    THEME_RANK_NOT_ELIGIBLE,
    THEME_RANK_OK,
    THEME_RANK_UNAVAILABLE,
    THEME_SCORE,
    THEME_STATUS,
    TRADE_DATE,
)
from qrp_atlas.indicators.system_b.theme_ranking import (
    ThemeRankingError,
    calculate_above_ma5_ratio,
    calculate_episode_duration,
    calculate_hot_appearance_rate,
    calculate_theme_ranking,
)


def test_calculate_episode_duration_counts_open_trading_days_only() -> None:
    # 2026-08-11 (Tuesday) to 2026-08-14 (Friday) -> 4 trading days
    calendar = [
        date(2026, 8, 10),
        date(2026, 8, 11),
        date(2026, 8, 12),
        date(2026, 8, 13),
        date(2026, 8, 14),
        date(2026, 8, 17),
    ]
    duration = calculate_episode_duration(date(2026, 8, 11), date(2026, 8, 14), calendar)
    assert duration == 4

    # Confirmed date is day 1
    duration_same = calculate_episode_duration(date(2026, 8, 11), date(2026, 8, 11), calendar)
    assert duration_same == 1

    # Confirmed date in the future raises error
    with pytest.raises(ThemeRankingError, match="EPISODE_CONFIRMED_DATE_FUTURE"):
        calculate_episode_duration(date(2026, 8, 17), date(2026, 8, 14), calendar)


def test_calculate_above_ma5_ratio_handles_legal_null_and_incomplete_history() -> None:
    expected_days = [
        date(2026, 8, 10),
        date(2026, 8, 11),
        date(2026, 8, 12),
        date(2026, 8, 13),
        date(2026, 8, 14),
    ]
    # 5 expected days: [True, True, None, None, None]
    states = pd.DataFrame({
        TRADE_DATE: expected_days,
        IS_ABOVE_OR_EQUAL_MA5: [True, True, None, None, None],
    })
    ratio, meta = calculate_above_ma5_ratio(states, expected_days)
    assert ratio == 1.0
    assert meta["expected_days"] == 5
    assert meta["valid_days"] == 2
    assert meta["null_days"] == 3
    assert meta["true_days"] == 2
    assert meta["false_days"] == 0

    # Incomplete history (missing a day) must fail-fast
    missing_one = states.iloc[:4]
    with pytest.raises(ThemeRankingError, match="EPISODE_STATE_HISTORY_INCOMPLETE"):
        calculate_above_ma5_ratio(missing_one, expected_days)

    # All NULL valid days = 0 must fail-fast
    all_null = pd.DataFrame({
        TRADE_DATE: expected_days,
        IS_ABOVE_OR_EQUAL_MA5: [None, None, None, None, None],
    })
    with pytest.raises(ThemeRankingError, match="EPISODE_ABOVE_MA5_VALID_DAYS_ZERO"):
        calculate_above_ma5_ratio(all_null, expected_days)


def test_calculate_hot_appearance_rate() -> None:
    # 25 appearances, 20 members, 3+2 snapshots -> 25 / (20 * 5) = 0.25
    rate = calculate_hot_appearance_rate(25, 20, 5)
    assert rate == 0.25

    with pytest.raises(ThemeRankingError, match="THEME_MEMBER_COUNT_NON_POSITIVE"):
        calculate_hot_appearance_rate(10, 0, 5)

    with pytest.raises(ThemeRankingError, match="HOT_APPEARANCE_RATE_OUT_OF_BOUNDS"):
        calculate_hot_appearance_rate(101, 20, 5)


def _fixture_setup():
    d = date(2026, 8, 14)
    themes = [("THM:1", "COL:1"), ("THM:2", "COL:2"), ("THM:3", "COL:3")]
    calendar = [
        date(2026, 8, 10),
        date(2026, 8, 11),
        date(2026, 8, 12),
        date(2026, 8, 13),
        date(2026, 8, 14),
    ]
    m4 = pd.DataFrame([
        {THEME_ID: "THM:1", COLLECTION_ID: "COL:1", TRADE_DATE: d, "effective_member_count": 10, "custom_index_episode_id": "EP:1", "theme_daily_return": 0.02, "theme_limit_up_count": 2},
        {THEME_ID: "THM:2", COLLECTION_ID: "COL:2", TRADE_DATE: d, "effective_member_count": 15, "custom_index_episode_id": "EP:2", "theme_daily_return": 0.01, "theme_limit_up_count": 1},
        {THEME_ID: "THM:3", COLLECTION_ID: "COL:3", TRADE_DATE: d, "effective_member_count": 20, "custom_index_episode_id": "EP:3", "theme_daily_return": 0.03, "theme_limit_up_count": 4},
    ])
    episodes = pd.DataFrame([
        {"episode_id": "EP:1", THEME_ID: "THM:1", COLLECTION_ID: "COL:1", "episode_start_date": date(2026, 8, 10), "episode_confirmed_date": date(2026, 8, 11), "episode_end_date": None},
        {"episode_id": "EP:2", THEME_ID: "THM:2", COLLECTION_ID: "COL:2", "episode_start_date": date(2026, 8, 10), "episode_confirmed_date": date(2026, 8, 11), "episode_end_date": None},
        {"episode_id": "EP:3", THEME_ID: "THM:3", COLLECTION_ID: "COL:3", "episode_start_date": date(2026, 8, 10), "episode_confirmed_date": date(2026, 8, 11), "episode_end_date": None},
    ])
    index_daily = pd.DataFrame([
        {THEME_ID: "THM:1", TRADE_DATE: date(2026, 8, 10), "index_level": 1000.0},
        {THEME_ID: "THM:1", TRADE_DATE: d, "index_level": 1100.0},
        {THEME_ID: "THM:2", TRADE_DATE: date(2026, 8, 10), "index_level": 1000.0},
        {THEME_ID: "THM:2", TRADE_DATE: d, "index_level": 1050.0},
        {THEME_ID: "THM:3", TRADE_DATE: date(2026, 8, 10), "index_level": 1000.0},
        {THEME_ID: "THM:3", TRADE_DATE: d, "index_level": 1150.0},
    ])
    states_rows = []
    for tid in ("THM:1", "THM:2", "THM:3"):
        for c_day in calendar:
            states_rows.append({
                THEME_ID: tid,
                COLLECTION_ID: f"COL:{tid[-1]}",
                TRADE_DATE: c_day,
                IS_ABOVE_OR_EQUAL_MA5: True if c_day > date(2026, 8, 11) else None,
            })
    states = pd.DataFrame(states_rows)
    avail = {
        "dc_hot": {"source_status": POPULARITY_AVAILABLE, "valid_snapshot_count": 2},
        "ths_hot": {"source_status": POPULARITY_AVAILABLE, "valid_snapshot_count": 2},
    }
    m5 = pd.DataFrame([
        {THEME_ID: "THM:1", COLLECTION_ID: "COL:1", TRADE_DATE: d, "theme_member_count": 10, "theme_hot_stock_count": 2, "theme_hot_stock_ratio": 0.2, "theme_hot_list_appearance_count": 4, "input_snapshot_id": "SNAP:1"},
        {THEME_ID: "THM:2", COLLECTION_ID: "COL:2", TRADE_DATE: d, "theme_member_count": 15, "theme_hot_stock_count": 1, "theme_hot_stock_ratio": 0.0667, "theme_hot_list_appearance_count": 2, "input_snapshot_id": "SNAP:1"},
        {THEME_ID: "THM:3", COLLECTION_ID: "COL:3", TRADE_DATE: d, "theme_member_count": 20, "theme_hot_stock_count": 6, "theme_hot_stock_ratio": 0.3, "theme_hot_list_appearance_count": 12, "input_snapshot_id": "SNAP:1"},
    ])
    return d, themes, m4, episodes, index_daily, states, calendar, avail, m5


def test_theme_ranking_pure_calculation_complete() -> None:
    d, themes, m4, episodes, index_daily, states, calendar, avail, m5 = _fixture_setup()
    result = calculate_theme_ranking(
        canonical_themes=themes,
        trade_date=d,
        m4_observations=m4,
        episodes=episodes,
        index_daily=index_daily,
        states=states,
        trading_calendar_open_days=calendar,
        popularity_availability=avail,
        m5_observations=m5,
    )
    assert result.run_status == "COMPLETE"
    assert len(result.snapshot) == 3
    assert len(result.component_audit) == 21  # 3 themes * 7 leaves

    # THM:3 is strongest in almost all metrics
    snap = result.snapshot.set_index(THEME_ID)
    assert snap.loc["THM:3", THEME_RANK] == 1.0
    assert snap.loc["THM:3", THEME_STATUS] == THEME_RANK_OK
    assert snap.loc["THM:3", THEME_SCORE] == 100.0


def test_theme_ranking_singleton_insufficient_universe() -> None:
    d, themes, m4, episodes, index_daily, states, calendar, avail, m5 = _fixture_setup()
    # Only 1 theme
    result = calculate_theme_ranking(
        canonical_themes=themes[:1],
        trade_date=d,
        m4_observations=m4.iloc[:1],
        episodes=episodes.iloc[:1],
        index_daily=index_daily[index_daily[THEME_ID] == "THM:1"],
        states=states[states[THEME_ID] == "THM:1"],
        trading_calendar_open_days=calendar,
        popularity_availability=avail,
        m5_observations=m5.iloc[:1],
    )
    snap = result.snapshot.iloc[0]
    assert snap[THEME_STATUS] == THEME_RANK_INSUFFICIENT_UNIVERSE
    assert snap[THEME_RANK] == 1.0
    assert pd.isna(snap[THEME_SCORE])


def test_theme_ranking_path_a_trusted_unavailable_overrides_singleton() -> None:
    d, themes, m4, episodes, index_daily, states, calendar, avail, m5 = _fixture_setup()
    avail["ths_hot"] = {"source_status": POPULARITY_UNAVAILABLE, "valid_snapshot_count": 0}

    # With N=1, UNAVAILABLE must take priority: status is INCOMPLETE_INPUT
    result = calculate_theme_ranking(
        canonical_themes=themes[:1],
        trade_date=d,
        m4_observations=m4.iloc[:1],
        episodes=episodes.iloc[:1],
        index_daily=index_daily[index_daily[THEME_ID] == "THM:1"],
        states=states[states[THEME_ID] == "THM:1"],
        trading_calendar_open_days=calendar,
        popularity_availability=avail,
        m5_observations=None,  # Not required in Path A!
    )
    assert result.run_status == "INCOMPLETE_INPUT"
    snap = result.snapshot.iloc[0]
    assert snap[THEME_STATUS] == THEME_RANK_INCOMPLETE_INPUT
    assert pd.isna(snap[THEME_RANK])
    assert pd.isna(snap[THEME_SCORE])


def test_theme_ranking_component_no_variation_reweight() -> None:
    d, themes, m4, episodes, index_daily, states, calendar, avail, m5 = _fixture_setup()
    # Make episode duration identical for all (e.g. all confirmed on 2026-08-11 -> duration 4)
    # They are already identical (duration=4 for all three).
    result = calculate_theme_ranking(
        canonical_themes=themes,
        trade_date=d,
        m4_observations=m4,
        episodes=episodes,
        index_daily=index_daily,
        states=states,
        trading_calendar_open_days=calendar,
        popularity_availability=avail,
        m5_observations=m5,
    )
    audit = result.component_audit
    duration_audit = audit[audit["component"] == THEME_COMPONENT_EPISODE_DURATION]
    assert (duration_audit["status"] == THEME_RANK_NO_VARIATION).all()
    assert duration_audit["effective_weight"].isna().all()

    # In _fixture_setup, both episode_duration (0.15) and episode_above_ma5_ratio (0.20)
    # have identical values across all 3 themes, so both are NO_VARIATION.
    # Total active weight = 1.0 - 0.15 - 0.20 = 0.65.
    other_audit = audit[audit["component"] == THEME_COMPONENT_EPISODE_RETURN]
    expected_effective_weight = 0.35 / 0.65
    assert other_audit["effective_weight"].iloc[0] == pytest.approx(expected_effective_weight)


def test_theme_ranking_empty_dimension_score_when_all_leaves_no_variation() -> None:
    d, themes, m4, episodes, index_daily, states, calendar, avail, m5 = _fixture_setup()
    # Make current_structure (limit_up_diffusion) equal for all
    m4_mod = m4.copy()
    m4_mod["theme_limit_up_count"] = 0  # diffusion is 0 for all 3 themes
    result = calculate_theme_ranking(
        canonical_themes=themes,
        trade_date=d,
        m4_observations=m4_mod,
        episodes=episodes,
        index_daily=index_daily,
        states=states,
        trading_calendar_open_days=calendar,
        popularity_availability=avail,
        m5_observations=m5,
    )
    snap = result.snapshot
    # current_structure_score must be None for all
    assert snap["current_structure_score"].isna().all()
    # But other dimension scores still exist
    assert snap["trend_strength_score"].notna().all()


def test_theme_ranking_eligibility_reasons_and_non_eligible() -> None:
    d, themes, m4, episodes, index_daily, states, calendar, avail, m5 = _fixture_setup()
    # THM:1 has effective_member_count = 0 -> NO_EFFECTIVE_MEMBERS
    # THM:2 has episode_end_date = d -> NO_OPEN_EPISODE
    # THM:3 is eligible
    m4_mod = m4.copy()
    m4_mod.loc[m4_mod[THEME_ID] == "THM:1", "effective_member_count"] = 0
    episodes_mod = episodes.copy()
    episodes_mod.loc[episodes_mod["episode_id"] == "EP:2", "episode_end_date"] = d

    result = calculate_theme_ranking(
        canonical_themes=themes,
        trade_date=d,
        m4_observations=m4_mod,
        episodes=episodes_mod,
        index_daily=index_daily,
        states=states,
        trading_calendar_open_days=calendar,
        popularity_availability=avail,
        m5_observations=m5,
    )
    snap = result.snapshot.set_index(THEME_ID)
    assert snap.loc["THM:1", RANK_ELIGIBLE] == False
    assert snap.loc["THM:1", RANK_ELIGIBILITY_REASON] == THEME_RANK_NO_EFFECTIVE_MEMBERS
    assert snap.loc["THM:1", THEME_STATUS] == THEME_RANK_NOT_ELIGIBLE

    assert snap.loc["THM:2", RANK_ELIGIBLE] == False
    assert snap.loc["THM:2", RANK_ELIGIBILITY_REASON] == THEME_RANK_NO_OPEN_EPISODE
    assert snap.loc["THM:2", THEME_STATUS] == THEME_RANK_NOT_ELIGIBLE

    assert snap.loc["THM:3", RANK_ELIGIBLE] == True
    assert snap.loc["THM:3", RANK_ELIGIBILITY_REASON] == THEME_RANK_ELIGIBLE
    # U_D has only 1 theme -> INSUFFICIENT_UNIVERSE
    assert snap.loc["THM:3", THEME_STATUS] == THEME_RANK_INSUFFICIENT_UNIVERSE
    assert snap.loc["THM:3", "theme_universe_size"] == 1
