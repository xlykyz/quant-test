"""Pure Task06-B System B Theme Trend Rank calculations.

This module contains pure, deterministic calculations without database or
scheduler side effects.  It consumes resolved canonical Theme facts and
transforms them into normalized cross-sectional ranks, dimension scores,
and comprehensive component audit trails.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from fractions import Fraction
import json
import math
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    BASE_WEIGHT,
    CALCULATION_VERSION,
    COLLECTION_ID,
    COMPONENT,
    CREATED_AT,
    CURRENT_STRUCTURE_SCORE,
    DIMENSION,
    DIRECTION,
    EFFECTIVE_WEIGHT,
    EVIDENCE,
    INDEX_LEVEL,
    INPUT_PROVENANCE,
    IS_ABOVE_OR_EQUAL_MA5,
    METADATA_JSON,
    NORMALIZED_RANK_SCORE,
    POPULARITY_AVAILABLE,
    POPULARITY_UNAVAILABLE,
    POPULARITY_SUPPORT_SCORE,
    PRODUCTION_RUN_ID,
    RANK_ELIGIBLE,
    RANK_ELIGIBILITY_REASON,
    RAW_RANK,
    RAW_VALUE,
    SOURCE_PROVENANCE,
    STATUS,
    SYSTEM_B_THEME_RANK_COMPONENT_AUDIT,
    SYSTEM_B_THEME_RANK_SNAPSHOT,
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
    THEME_RANK_CALCULATION_VERSION,
    THEME_RANK_ELIGIBLE,
    THEME_RANK_INCOMPLETE_INPUT,
    THEME_RANK_INSUFFICIENT_UNIVERSE,
    THEME_RANK_MISSING_INPUT,
    THEME_RANK_NO_EFFECTIVE_MEMBERS,
    THEME_RANK_NO_OPEN_EPISODE,
    THEME_RANK_NO_VARIATION,
    THEME_RANK_NOT_ELIGIBLE,
    THEME_RANK_OK,
    THEME_RANK_UNAVAILABLE,
    THEME_RAW_SCORE,
    THEME_SCORE,
    THEME_STATUS,
    THEME_UNIVERSE_SIZE,
    TIE_COUNT,
    TRADE_DATE,
    TREND_PERSISTENCE_SCORE,
    TREND_STRENGTH_SCORE,
    WEIGHTED_CONTRIBUTION,
)
from qrp_atlas.indicators.system_b.asset_ranking import HIGHER_IS_BETTER, rank_component


class ThemeRankingError(ValueError):
    """Raised when ranking inputs violate a deterministic fact contract."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


@dataclass(frozen=True)
class LeafComponentDefinition:
    component: str
    dimension: str
    base_weight_int: int
    base_weight: float
    direction: str = HIGHER_IS_BETTER


LEAF_COMPONENTS: tuple[LeafComponentDefinition, ...] = (
    LeafComponentDefinition(
        THEME_COMPONENT_EPISODE_RETURN,
        THEME_DIMENSION_TREND_STRENGTH,
        35,
        0.35,
    ),
    LeafComponentDefinition(
        THEME_COMPONENT_THEME_DAILY_RETURN,
        THEME_DIMENSION_TREND_STRENGTH,
        10,
        0.10,
    ),
    LeafComponentDefinition(
        THEME_COMPONENT_EPISODE_DURATION,
        THEME_DIMENSION_TREND_PERSISTENCE,
        15,
        0.15,
    ),
    LeafComponentDefinition(
        THEME_COMPONENT_EPISODE_ABOVE_MA5_RATIO,
        THEME_DIMENSION_TREND_PERSISTENCE,
        20,
        0.20,
    ),
    LeafComponentDefinition(
        THEME_COMPONENT_LIMIT_UP_DIFFUSION,
        THEME_DIMENSION_CURRENT_STRUCTURE,
        10,
        0.10,
    ),
    LeafComponentDefinition(
        THEME_COMPONENT_HOT_STOCK_RATIO,
        THEME_DIMENSION_POPULARITY_SUPPORT,
        6,
        0.06,
    ),
    LeafComponentDefinition(
        THEME_COMPONENT_HOT_APPEARANCE_RATE,
        THEME_DIMENSION_POPULARITY_SUPPORT,
        4,
        0.04,
    ),
)

LEAF_MAP: dict[str, LeafComponentDefinition] = {leaf.component: leaf for leaf in LEAF_COMPONENTS}

DIMENSION_LEAVES: dict[str, tuple[str, ...]] = {
    THEME_DIMENSION_TREND_STRENGTH: (
        THEME_COMPONENT_EPISODE_RETURN,
        THEME_COMPONENT_THEME_DAILY_RETURN,
    ),
    THEME_DIMENSION_TREND_PERSISTENCE: (
        THEME_COMPONENT_EPISODE_DURATION,
        THEME_COMPONENT_EPISODE_ABOVE_MA5_RATIO,
    ),
    THEME_DIMENSION_CURRENT_STRUCTURE: (THEME_COMPONENT_LIMIT_UP_DIFFUSION,),
    THEME_DIMENSION_POPULARITY_SUPPORT: (
        THEME_COMPONENT_HOT_STOCK_RATIO,
        THEME_COMPONENT_HOT_APPEARANCE_RATE,
    ),
}


@dataclass(frozen=True)
class ThemeRankingResult:
    """Complete pure Task06-B Theme Rank output."""

    snapshot: pd.DataFrame
    component_audit: pd.DataFrame
    diagnostics: tuple[str, ...] = ()
    input_provenance: Mapping[str, Any] | None = None
    run_status: str = "COMPLETE"

    @property
    def frame(self) -> pd.DataFrame:
        """Compatibility alias for callers expecting ``frame``."""
        return self.snapshot


def _parse_date(val: Any) -> date:
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return pd.to_datetime(val).date()


def calculate_episode_duration(
    confirmed_date: date,
    target_date: date,
    open_calendar_days: Sequence[date],
) -> int:
    """Calculate open trading days from confirmed_date to target_date inclusive.

    confirmed_date is day 1. If confirmed_date > target_date, raises ThemeRankingError.
    """
    if confirmed_date > target_date:
        raise ThemeRankingError(
            "EPISODE_CONFIRMED_DATE_FUTURE",
            f"confirmed_date {confirmed_date} > target_date {target_date}",
        )
    open_set = set(open_calendar_days)
    if confirmed_date not in open_set:
        raise ThemeRankingError(
            "EPISODE_CONFIRMED_DATE_NOT_TRADING_DAY",
            f"confirmed_date {confirmed_date} is not an open trading day",
        )
    count = sum(1 for d in open_calendar_days if confirmed_date <= d <= target_date)
    if count < 1:
        raise ThemeRankingError(
            "EPISODE_DURATION_INVALID",
            f"duration between {confirmed_date} and {target_date} is {count}",
        )
    return count


def calculate_above_ma5_ratio(
    states_df: pd.DataFrame,
    expected_open_days: Sequence[date],
) -> tuple[float, dict[str, int]]:
    """Calculate Above-MA5 ratio over start_date..target_date open days.

    Every expected day must have exactly one state row.
    Legal NULL is preserved in null_days and excluded from valid denominator.
    """
    expected_set = set(expected_open_days)
    expected_count = len(expected_set)
    if expected_count == 0:
        raise ThemeRankingError(
            "EPISODE_STATE_HISTORY_EMPTY",
            "expected trading days count is zero",
        )

    if states_df.empty:
        raise ThemeRankingError(
            "EPISODE_STATE_HISTORY_INCOMPLETE",
            f"missing all {expected_count} expected state rows",
        )

    states_by_date: dict[date, Any] = {}
    for _, row in states_df.iterrows():
        d_val = _parse_date(row[TRADE_DATE])
        if d_val in states_by_date:
            raise ThemeRankingError(
                "EPISODE_STATE_HISTORY_DUPLICATE",
                f"duplicate state row on date {d_val}",
            )
        states_by_date[d_val] = row[IS_ABOVE_OR_EQUAL_MA5]

    missing_days = [d for d in expected_open_days if d not in states_by_date]
    if missing_days:
        raise ThemeRankingError(
            "EPISODE_STATE_HISTORY_INCOMPLETE",
            f"missing {len(missing_days)} state rows: {missing_days[:5]}",
        )

    true_days = 0
    false_days = 0
    null_days = 0
    for d in expected_open_days:
        flag = states_by_date[d]
        if pd.isna(flag) or flag is None:
            null_days += 1
        elif bool(flag):
            true_days += 1
        else:
            false_days += 1

    valid_days = true_days + false_days
    if valid_days == 0:
        raise ThemeRankingError(
            "EPISODE_ABOVE_MA5_VALID_DAYS_ZERO",
            f"all {expected_count} expected days are NULL",
        )

    raw_ratio = true_days / valid_days
    if not (0.0 <= raw_ratio <= 1.0):
        raise ThemeRankingError(
            "EPISODE_ABOVE_MA5_RATIO_OUT_OF_BOUNDS",
            f"calculated ratio {raw_ratio} is outside [0, 1]",
        )

    metadata = {
        "expected_days": expected_count,
        "valid_days": valid_days,
        "null_days": null_days,
        "true_days": true_days,
        "false_days": false_days,
    }
    return raw_ratio, metadata


def calculate_hot_appearance_rate(
    appearance_count: int,
    member_count: int,
    valid_snapshot_count_total: int,
) -> float:
    """Calculate hot appearance rate = appearances / (members * snapshots)."""
    if member_count <= 0:
        raise ThemeRankingError(
            "THEME_MEMBER_COUNT_NON_POSITIVE",
            f"member_count {member_count} <= 0",
        )
    if valid_snapshot_count_total <= 0:
        raise ThemeRankingError(
            "VALID_SNAPSHOT_COUNT_NON_POSITIVE",
            f"valid_snapshot_count_total {valid_snapshot_count_total} <= 0",
        )
    denominator = member_count * valid_snapshot_count_total
    rate = appearance_count / denominator
    if not (0.0 <= rate <= 1.0):
        raise ThemeRankingError(
            "HOT_APPEARANCE_RATE_OUT_OF_BOUNDS",
            f"rate {rate} ({appearance_count}/{denominator}) outside [0, 1]",
        )
    return float(rate)


def calculate_theme_ranking(
    *,
    canonical_themes: Sequence[tuple[str, str]],  # (theme_id, collection_id)
    trade_date: date,
    m4_observations: pd.DataFrame,
    episodes: pd.DataFrame,
    index_daily: pd.DataFrame,
    states: pd.DataFrame,
    trading_calendar_open_days: Sequence[date],
    popularity_availability: Mapping[str, Mapping[str, Any]],
    m5_observations: pd.DataFrame | None = None,
    production_run_id: str = "theme_rank_run",
    created_at: Any = None,
) -> ThemeRankingResult:
    """Pure calculation of Task06-B Theme Rank for trade_date."""
    trade_date = _parse_date(trade_date)
    trading_calendar_open_days = [_parse_date(d) for d in trading_calendar_open_days]
    now_ts = pd.to_datetime(created_at) if created_at is not None else pd.Timestamp.now()
    all_canonical_ids = [t[0] for t in canonical_themes]
    theme_to_collection = dict(canonical_themes)
    total_canonical_count = len(canonical_themes)

    if len(all_canonical_ids) != len(set(all_canonical_ids)):
        raise ThemeRankingError("CANONICAL_THEMES_DUPLICATE_ID", "Duplicate theme_id in canonical_themes")

    # Determine Popularity availability path
    # Required sources: dc_hot and ths_hot
    required_sources = ("dc_hot", "ths_hot")
    source_statuses: dict[str, str] = {}
    valid_snapshot_counts: dict[str, int] = {}
    diagnostics_list: list[str] = []

    for src in required_sources:
        avail = popularity_availability.get(src)
        if not avail:
            raise ThemeRankingError(
                "POPULARITY_AVAILABILITY_MISSING",
                f"source {src} availability row missing",
            )
        status = str(avail.get("source_status", avail.get("status", ""))).upper()
        if status not in {POPULARITY_AVAILABLE, POPULARITY_UNAVAILABLE}:
            raise ThemeRankingError(
                "POPULARITY_AVAILABILITY_INVALID",
                f"source {src} invalid status {status}",
            )
        source_statuses[src] = status
        v_count = avail.get("valid_snapshot_count", 0)
        valid_snapshot_counts[src] = int(v_count)
        if status == POPULARITY_UNAVAILABLE:
            diagnostics_list.append(f"{src.upper()}_SOURCE_UNAVAILABLE")

    is_popularity_unavailable = any(st == POPULARITY_UNAVAILABLE for st in source_statuses.values())

    # Build M4 lookup
    m4_by_theme: dict[str, pd.Series] = {}
    if not m4_observations.empty:
        for _, row in m4_observations.iterrows():
            tid = str(row[THEME_ID])
            m4_by_theme[tid] = row

    # Build Episode lookup
    episodes_by_id: dict[str, pd.Series] = {}
    if not episodes.empty:
        for _, row in episodes.iterrows():
            eid = str(row["episode_id"])
            episodes_by_id[eid] = row

    # Build Index Daily lookup: (theme_id, trade_date) -> index_level
    index_levels: dict[tuple[str, date], float] = {}
    if not index_daily.empty:
        for _, row in index_daily.iterrows():
            tid = str(row[THEME_ID])
            d_val = _parse_date(row[TRADE_DATE])
            index_levels[(tid, d_val)] = float(row[INDEX_LEVEL])

    # Build States lookup: theme_id -> DataFrame of states
    states_by_theme: dict[str, pd.DataFrame] = {}
    if not states.empty:
        for tid, group in states.groupby(THEME_ID):
            states_by_theme[str(tid)] = group.copy()

    # Build M5 lookup if available
    m5_by_theme: dict[str, pd.Series] = {}
    if not is_popularity_unavailable:
        if m5_observations is None or m5_observations.empty:
            raise ThemeRankingError("M5_OBSERVATIONS_MISSING", "m5_observations required when popularity is AVAILABLE")
        for _, row in m5_observations.iterrows():
            tid = str(row[THEME_ID])
            m5_by_theme[tid] = row

    # Determine eligibility for each theme in C_D
    # U_D = eligible themes
    eligible_themes: list[str] = []
    eligibility_map: dict[str, tuple[bool, str, str | None]] = {}  # theme_id -> (eligible, reason, episode_id)

    open_days_set = set(trading_calendar_open_days)

    for theme_id in all_canonical_ids:
        collection_id = theme_to_collection[theme_id]
        m4_row = m4_by_theme.get(theme_id)

        if m4_row is None:
            # M4 is required for canonical themes on D
            raise ThemeRankingError("M4_OBSERVATION_MISSING", f"Theme {theme_id} missing M4 observation on target date")

        effective_member_count = int(m4_row.get("effective_member_count", 0))
        ep_id = m4_row.get("custom_index_episode_id")
        if pd.isna(ep_id) or ep_id is None or str(ep_id).strip() == "":
            ep_id = None
        else:
            ep_id = str(ep_id).strip()

        if effective_member_count == 0:
            eligibility_map[theme_id] = (False, THEME_RANK_NO_EFFECTIVE_MEMBERS, ep_id)
            continue

        if ep_id is None:
            eligibility_map[theme_id] = (False, THEME_RANK_NO_OPEN_EPISODE, None)
            continue

        # Look up episode
        ep_row = episodes_by_id.get(ep_id)
        if ep_row is None:
            raise ThemeRankingError("EPISODE_NOT_FOUND", f"Episode {ep_id} for theme {theme_id} not found in episodes")

        # Check identity matching
        if str(ep_row.get(THEME_ID)) != theme_id:
            raise ThemeRankingError(
                "EPISODE_THEME_ID_MISMATCH",
                f"Episode {ep_id} theme_id {ep_row.get(THEME_ID)} != {theme_id}",
            )
        if str(ep_row.get(COLLECTION_ID)) != collection_id:
            raise ThemeRankingError(
                "EPISODE_COLLECTION_ID_MISMATCH",
                f"Episode {ep_id} collection_id {ep_row.get(COLLECTION_ID)} != {collection_id}",
            )

        start_date = _parse_date(ep_row["episode_start_date"])
        confirmed_date = _parse_date(ep_row["episode_confirmed_date"])
        raw_end = ep_row.get("episode_end_date")
        end_date = _parse_date(raw_end) if raw_end is not None and not pd.isna(raw_end) else None

        if confirmed_date > trade_date:
            raise ThemeRankingError(
                "EPISODE_CONFIRMED_DATE_FUTURE",
                f"Episode {ep_id} confirmed_date {confirmed_date} > target date {trade_date}",
            )
        if end_date is not None and end_date < trade_date:
            raise ThemeRankingError(
                "EPISODE_END_DATE_PAST",
                f"Episode {ep_id} end_date {end_date} < target date {trade_date} but pointed by M4",
            )

        if end_date == trade_date:
            # Legal end date: rank_eligible = False, reason = NO_OPEN_EPISODE
            eligibility_map[theme_id] = (False, THEME_RANK_NO_OPEN_EPISODE, ep_id)
            continue

        # Confirmed and OPEN_AT_D
        eligibility_map[theme_id] = (True, THEME_RANK_ELIGIBLE, ep_id)
        eligible_themes.append(theme_id)

    u_d_size = len(eligible_themes)

    # Calculate raw leaf values for U_D
    # Component data structures
    raw_values: dict[str, dict[str, float | None]] = {leaf.component: {} for leaf in LEAF_COMPONENTS}
    metadata_map: dict[str, dict[str, dict[str, Any]]] = {leaf.component: {} for leaf in LEAF_COMPONENTS}
    source_prov_map: dict[str, dict[str, str]] = {leaf.component: {} for leaf in LEAF_COMPONENTS}

    valid_snapshot_count_total = sum(valid_snapshot_counts.values())

    for theme_id in eligible_themes:
        _, _, ep_id = eligibility_map[theme_id]
        assert ep_id is not None
        ep_row = episodes_by_id[ep_id]
        m4_row = m4_by_theme[theme_id]

        start_date = _parse_date(ep_row["episode_start_date"])
        confirmed_date = _parse_date(ep_row["episode_confirmed_date"])
        raw_end = ep_row.get("episode_end_date")
        end_date = _parse_date(raw_end) if raw_end is not None and not pd.isna(raw_end) else None

        # 1. episode_return
        start_lvl = index_levels.get((theme_id, start_date))
        d_lvl = index_levels.get((theme_id, trade_date))
        if start_lvl is None or pd.isna(start_lvl) or start_lvl <= 0:
            raise ThemeRankingError(
                "THEME_INDEX_START_LEVEL_INVALID",
                f"theme {theme_id} index level at start_date {start_date} is invalid ({start_lvl})",
            )
        if d_lvl is None or pd.isna(d_lvl) or d_lvl <= 0:
            raise ThemeRankingError(
                "THEME_INDEX_D_LEVEL_INVALID",
                f"theme {theme_id} index level at target_date {trade_date} is invalid ({d_lvl})",
            )
        ep_ret = float(d_lvl / start_lvl - 1.0)
        raw_values[THEME_COMPONENT_EPISODE_RETURN][theme_id] = ep_ret
        metadata_map[THEME_COMPONENT_EPISODE_RETURN][theme_id] = {
            "episode_id": ep_id,
            "start_date": str(start_date),
            "start_index_level": start_lvl,
            "target_date": str(trade_date),
            "target_index_level": d_lvl,
        }
        source_prov_map[THEME_COMPONENT_EPISODE_RETURN][theme_id] = json.dumps(
            {"table": "theme_custom_index_daily", "episode_id": ep_id}
        )

        # 2. theme_daily_return
        daily_ret = m4_row.get("theme_daily_return")
        if daily_ret is None or pd.isna(daily_ret):
            raise ThemeRankingError(
                "THEME_DAILY_RETURN_MISSING",
                f"theme {theme_id} missing theme_daily_return in M4",
            )
        raw_values[THEME_COMPONENT_THEME_DAILY_RETURN][theme_id] = float(daily_ret)
        metadata_map[THEME_COMPONENT_THEME_DAILY_RETURN][theme_id] = {
            "theme_daily_return": float(daily_ret),
        }
        source_prov_map[THEME_COMPONENT_THEME_DAILY_RETURN][theme_id] = json.dumps(
            {"table": "theme_m4_observation"}
        )

        # 3. episode_duration
        duration = calculate_episode_duration(confirmed_date, trade_date, trading_calendar_open_days)
        raw_values[THEME_COMPONENT_EPISODE_DURATION][theme_id] = float(duration)
        metadata_map[THEME_COMPONENT_EPISODE_DURATION][theme_id] = {
            "episode_id": ep_id,
            "confirmed_date": str(confirmed_date),
            "target_date": str(trade_date),
            "open_trading_days": duration,
        }
        source_prov_map[THEME_COMPONENT_EPISODE_DURATION][theme_id] = json.dumps(
            {"table": "theme_custom_index_episode", "episode_id": ep_id}
        )

        # 4. episode_above_ma5_ratio
        ep_expected_open_days = [d for d in trading_calendar_open_days if start_date <= d <= trade_date]
        theme_states = states_by_theme.get(theme_id, pd.DataFrame())
        above_ratio, above_meta = calculate_above_ma5_ratio(theme_states, ep_expected_open_days)
        raw_values[THEME_COMPONENT_EPISODE_ABOVE_MA5_RATIO][theme_id] = above_ratio
        metadata_map[THEME_COMPONENT_EPISODE_ABOVE_MA5_RATIO][theme_id] = above_meta
        source_prov_map[THEME_COMPONENT_EPISODE_ABOVE_MA5_RATIO][theme_id] = json.dumps(
            {"table": "theme_custom_index_state", "episode_id": ep_id}
        )

        # 5. limit_up_diffusion
        limit_up_cnt = m4_row.get("theme_limit_up_count")
        eff_cnt = m4_row.get("effective_member_count")
        if limit_up_cnt is None or pd.isna(limit_up_cnt):
            raise ThemeRankingError(
                "THEME_LIMIT_UP_COUNT_MISSING",
                f"theme {theme_id} missing theme_limit_up_count in M4",
            )
        limit_up_diffusion = float(limit_up_cnt / eff_cnt)
        if not (0.0 <= limit_up_diffusion <= 1.0):
            raise ThemeRankingError(
                "LIMIT_UP_DIFFUSION_OUT_OF_BOUNDS",
                f"theme {theme_id} limit_up_diffusion {limit_up_diffusion} outside [0, 1]",
            )
        raw_values[THEME_COMPONENT_LIMIT_UP_DIFFUSION][theme_id] = limit_up_diffusion
        metadata_map[THEME_COMPONENT_LIMIT_UP_DIFFUSION][theme_id] = {
            "limit_up_count": int(limit_up_cnt),
            "effective_member_count": int(eff_cnt),
        }
        source_prov_map[THEME_COMPONENT_LIMIT_UP_DIFFUSION][theme_id] = json.dumps(
            {"table": "theme_m4_observation"}
        )

        # 6. hot_stock_ratio & 7. hot_appearance_rate
        if is_popularity_unavailable:
            raw_values[THEME_COMPONENT_HOT_STOCK_RATIO][theme_id] = None
            raw_values[THEME_COMPONENT_HOT_APPEARANCE_RATE][theme_id] = None
            metadata_map[THEME_COMPONENT_HOT_STOCK_RATIO][theme_id] = {"status": POPULARITY_UNAVAILABLE}
            metadata_map[THEME_COMPONENT_HOT_APPEARANCE_RATE][theme_id] = {"status": POPULARITY_UNAVAILABLE}
            source_prov_map[THEME_COMPONENT_HOT_STOCK_RATIO][theme_id] = json.dumps(
                {"availability": source_statuses}
            )
            source_prov_map[THEME_COMPONENT_HOT_APPEARANCE_RATE][theme_id] = json.dumps(
                {"availability": source_statuses}
            )
        else:
            m5_row = m5_by_theme.get(theme_id)
            if m5_row is None:
                raise ThemeRankingError(
                    "M5_OBSERVATION_MISSING",
                    f"theme {theme_id} missing in M5 observation",
                )
            # hot stock ratio
            hs_ratio = m5_row.get("theme_hot_stock_ratio")
            if hs_ratio is None or pd.isna(hs_ratio):
                raise ThemeRankingError(
                    "M5_HOT_STOCK_RATIO_MISSING",
                    f"theme {theme_id} missing theme_hot_stock_ratio in M5",
                )
            hs_ratio_f = float(hs_ratio)
            if not (0.0 <= hs_ratio_f <= 1.0):
                raise ThemeRankingError(
                    "M5_HOT_STOCK_RATIO_OUT_OF_BOUNDS",
                    f"theme {theme_id} hot_stock_ratio {hs_ratio_f} outside [0, 1]",
                )
            raw_values[THEME_COMPONENT_HOT_STOCK_RATIO][theme_id] = hs_ratio_f
            metadata_map[THEME_COMPONENT_HOT_STOCK_RATIO][theme_id] = {
                "theme_member_count": int(m5_row["theme_member_count"]),
                "theme_hot_stock_count": int(m5_row["theme_hot_stock_count"]),
            }
            source_prov_map[THEME_COMPONENT_HOT_STOCK_RATIO][theme_id] = json.dumps(
                {"table": "theme_m5_observation", "input_snapshot_id": str(m5_row.get("input_snapshot_id", ""))}
            )

            # hot appearance rate
            app_cnt = int(m5_row.get("theme_hot_list_appearance_count", 0))
            mem_cnt = int(m5_row.get("theme_member_count", 0))
            app_rate = calculate_hot_appearance_rate(app_cnt, mem_cnt, valid_snapshot_count_total)
            raw_values[THEME_COMPONENT_HOT_APPEARANCE_RATE][theme_id] = app_rate
            metadata_map[THEME_COMPONENT_HOT_APPEARANCE_RATE][theme_id] = {
                "appearance_count": app_cnt,
                "theme_member_count": mem_cnt,
                "valid_snapshot_count_total": valid_snapshot_count_total,
            }
            source_prov_map[THEME_COMPONENT_HOT_APPEARANCE_RATE][theme_id] = json.dumps(
                {"table": "theme_m5_observation", "input_snapshot_id": str(m5_row.get("input_snapshot_id", ""))}
            )

    # Cross-sectional ranking on fixed U_D
    component_ranks: dict[str, pd.Series] = {}
    component_scores: dict[str, pd.Series] = {}
    component_statuses: dict[str, str] = {}
    component_ties: dict[str, pd.Series] = {}

    active_leaves: list[str] = []

    for leaf in LEAF_COMPONENTS:
        comp = leaf.component
        if u_d_size == 0:
            component_statuses[comp] = THEME_RANK_INSUFFICIENT_UNIVERSE
            continue

        if is_popularity_unavailable and comp in {THEME_COMPONENT_HOT_STOCK_RATIO, THEME_COMPONENT_HOT_APPEARANCE_RATE}:
            component_statuses[comp] = THEME_RANK_UNAVAILABLE
            component_ranks[comp] = pd.Series(np.nan, index=eligible_themes)
            component_scores[comp] = pd.Series(np.nan, index=eligible_themes)
            component_ties[comp] = pd.Series(0, index=eligible_themes, dtype="int64")
            continue

        series = pd.Series([raw_values[comp][tid] for tid in eligible_themes], index=eligible_themes)
        # Check no missing input in fixed U_D
        if series.isna().any():
            raise ThemeRankingError(
                "COMPONENT_RAW_VALUE_MISSING",
                f"Component {comp} has missing values in fixed U_D",
            )

        res = rank_component(series, direction=leaf.direction)
        component_ranks[comp] = res.ranks
        component_scores[comp] = res.scores
        component_ties[comp] = res.frame[TIE_COUNT]
        component_statuses[comp] = res.status

        if res.status == THEME_RANK_OK:
            active_leaves.append(comp)

    # Reweighting active leaves
    active_weight_sum = sum(LEAF_MAP[comp].base_weight for comp in active_leaves)

    # Compute exact integer tie key K_i and display scores
    theme_raw_scores: dict[str, float | None] = {}
    theme_final_ranks: dict[str, float | None] = {}
    theme_final_scores: dict[str, float | None] = {}
    theme_statuses: dict[str, str] = {}

    dimension_scores: dict[str, dict[str, float | None]] = {
        dim: {} for dim in DIMENSION_LEAVES
    }

    if u_d_size == 0:
        pass
    elif is_popularity_unavailable:
        # Path A: Trusted unavailable -> eligible rows INCOMPLETE_INPUT
        # Calculate dimension scores for active dimensions
        for dim, leaves in DIMENSION_LEAVES.items():
            dim_active = [c for c in leaves if c in active_leaves]
            dim_budget = sum(LEAF_MAP[c].base_weight for c in dim_active)
            for tid in eligible_themes:
                if dim_budget > 0:
                    dim_score = sum(
                        LEAF_MAP[c].base_weight * component_scores[c].loc[tid]
                        for c in dim_active
                    ) / dim_budget
                    dimension_scores[dim][tid] = float(dim_score)
                else:
                    dimension_scores[dim][tid] = None

        for tid in eligible_themes:
            theme_raw_scores[tid] = None
            theme_final_ranks[tid] = None
            theme_final_scores[tid] = None
            theme_statuses[tid] = THEME_RANK_INCOMPLETE_INPUT
    elif u_d_size == 1:
        # N = 1
        tid = eligible_themes[0]
        theme_raw_scores[tid] = None
        theme_final_ranks[tid] = 1.0
        theme_final_scores[tid] = None
        theme_statuses[tid] = THEME_RANK_INSUFFICIENT_UNIVERSE
        for dim in DIMENSION_LEAVES:
            dimension_scores[dim][tid] = None
    elif len(active_leaves) == 0:
        # All leaves NO_VARIATION
        for tid in eligible_themes:
            theme_raw_scores[tid] = None
            theme_final_ranks[tid] = None
            theme_final_scores[tid] = None
            theme_statuses[tid] = THEME_RANK_NO_VARIATION
            for dim in DIMENSION_LEAVES:
                dimension_scores[dim][tid] = None
    else:
        # Active leaves present and popularity available
        # Exact integer key K_i
        keys: dict[str, int] = {}
        raw_composite: dict[str, float] = {}

        for tid in eligible_themes:
            k_val = 0
            raw_s = 0.0
            for comp in active_leaves:
                leaf_def = LEAF_MAP[comp]
                r_rank = float(component_ranks[comp].loc[tid])
                q = 2 * u_d_size - 2 * r_rank
                k_val += leaf_def.base_weight_int * int(round(q))
                eff_w = leaf_def.base_weight / active_weight_sum
                raw_s += eff_w * float(component_scores[comp].loc[tid])
            keys[tid] = k_val
            raw_composite[tid] = raw_s

        # Rank keys descending
        key_series = pd.Series(keys)
        if key_series.nunique() == 1:
            # All composite keys identical
            avg_rank = (u_d_size + 1) / 2.0
            for tid in eligible_themes:
                theme_raw_scores[tid] = raw_composite[tid]
                theme_final_ranks[tid] = avg_rank
                theme_final_scores[tid] = None
                theme_statuses[tid] = THEME_RANK_NO_VARIATION
        else:
            rank_s = key_series.rank(ascending=False, method="average")
            score_s = 100.0 * (u_d_size - rank_s) / (u_d_size - 1.0)
            for tid in eligible_themes:
                theme_raw_scores[tid] = raw_composite[tid]
                theme_final_ranks[tid] = float(rank_s.loc[tid])
                theme_final_scores[tid] = float(score_s.loc[tid])
                theme_statuses[tid] = THEME_RANK_OK

        # Dimension scores
        for dim, leaves in DIMENSION_LEAVES.items():
            dim_active = [c for c in leaves if c in active_leaves]
            dim_budget = sum(LEAF_MAP[c].base_weight for c in dim_active)
            for tid in eligible_themes:
                if dim_budget > 0:
                    dim_score = sum(
                        LEAF_MAP[c].base_weight * component_scores[c].loc[tid]
                        for c in dim_active
                    ) / dim_budget
                    dimension_scores[dim][tid] = float(dim_score)
                else:
                    dimension_scores[dim][tid] = None

    # Construct Component Audit Rows
    audit_rows: list[dict[str, Any]] = []
    for comp_def in LEAF_COMPONENTS:
        comp = comp_def.component
        dim = comp_def.dimension
        b_weight = comp_def.base_weight

        for theme_id in eligible_themes:
            collection_id = theme_to_collection[theme_id]
            r_val = raw_values[comp].get(theme_id)
            c_status = component_statuses.get(comp, THEME_RANK_MISSING_INPUT)

            if c_status == THEME_RANK_OK:
                r_rank = float(component_ranks[comp].loc[theme_id])
                n_score = float(component_scores[comp].loc[theme_id])
                eff_w = b_weight / active_weight_sum
                w_contrib = eff_w * n_score
                t_count = int(component_ties[comp].loc[theme_id])
            elif c_status == THEME_RANK_NO_VARIATION:
                r_rank = float(component_ranks[comp].loc[theme_id])
                n_score = None
                eff_w = None
                w_contrib = None
                t_count = int(component_ties[comp].loc[theme_id])
            elif c_status == THEME_RANK_INSUFFICIENT_UNIVERSE:
                r_rank = 1.0
                n_score = None
                eff_w = None
                w_contrib = None
                t_count = 1
            else:  # UNAVAILABLE
                r_rank = None
                n_score = None
                eff_w = None
                w_contrib = None
                t_count = 0

            audit_rows.append(
                {
                    TRADE_DATE: trade_date,
                    THEME_ID: theme_id,
                    COLLECTION_ID: collection_id,
                    DIMENSION: dim,
                    COMPONENT: comp,
                    RAW_VALUE: r_val,
                    DIRECTION: comp_def.direction,
                    RAW_RANK: r_rank,
                    NORMALIZED_RANK_SCORE: n_score,
                    BASE_WEIGHT: b_weight,
                    EFFECTIVE_WEIGHT: eff_w,
                    WEIGHTED_CONTRIBUTION: w_contrib,
                    "universe_size": u_d_size,
                    TIE_COUNT: t_count,
                    STATUS: c_status,
                    SOURCE_PROVENANCE: source_prov_map[comp].get(theme_id, "{}"),
                    METADATA_JSON: json.dumps(metadata_map[comp].get(theme_id, {})),
                    PRODUCTION_RUN_ID: production_run_id,
                    CALCULATION_VERSION: THEME_RANK_CALCULATION_VERSION,
                    CREATED_AT: now_ts,
                }
            )

    audit_columns = list(SYSTEM_B_THEME_RANK_COMPONENT_AUDIT.column_names())
    audit_df = pd.DataFrame(audit_rows, columns=audit_columns) if audit_rows else pd.DataFrame(columns=audit_columns)

    # Construct Snapshot Rows (covering all C_D)
    snapshot_rows: list[dict[str, Any]] = []

    provenance_payload = {
        "trade_date": str(trade_date),
        "calculation_version": THEME_RANK_CALCULATION_VERSION,
        "canonical_themes_count": total_canonical_count,
        "u_d_size": u_d_size,
        "popularity_source_statuses": source_statuses,
        "active_leaves": active_leaves,
    }
    input_provenance_str = json.dumps(provenance_payload)
    diagnostics_str = json.dumps(diagnostics_list)

    for theme_id in all_canonical_ids:
        collection_id = theme_to_collection[theme_id]
        is_eligible, elig_reason, ep_id = eligibility_map[theme_id]

        evidence_payload = {
            "eligible": is_eligible,
            "reason": elig_reason,
            "episode_id": ep_id,
        }

        if is_eligible:
            t_status = theme_statuses.get(theme_id, THEME_RANK_OK)
            raw_s = theme_raw_scores.get(theme_id)
            f_rank = theme_final_ranks.get(theme_id)
            f_score = theme_final_scores.get(theme_id)
            ts_score = dimension_scores[THEME_DIMENSION_TREND_STRENGTH].get(theme_id)
            tp_score = dimension_scores[THEME_DIMENSION_TREND_PERSISTENCE].get(theme_id)
            cs_score = dimension_scores[THEME_DIMENSION_CURRENT_STRUCTURE].get(theme_id)
            ps_score = dimension_scores[THEME_DIMENSION_POPULARITY_SUPPORT].get(theme_id)
        else:
            t_status = THEME_RANK_NOT_ELIGIBLE
            raw_s = None
            f_rank = None
            f_score = None
            ts_score = None
            tp_score = None
            cs_score = None
            ps_score = None

        snapshot_rows.append(
            {
                TRADE_DATE: trade_date,
                THEME_ID: theme_id,
                COLLECTION_ID: collection_id,
                RANK_ELIGIBLE: is_eligible,
                RANK_ELIGIBILITY_REASON: elig_reason,
                TREND_STRENGTH_SCORE: ts_score,
                TREND_PERSISTENCE_SCORE: tp_score,
                CURRENT_STRUCTURE_SCORE: cs_score,
                POPULARITY_SUPPORT_SCORE: ps_score,
                THEME_RAW_SCORE: raw_s,
                THEME_RANK: f_rank,
                THEME_SCORE: f_score,
                THEME_STATUS: t_status,
                THEME_UNIVERSE_SIZE: u_d_size,
                INPUT_PROVENANCE: input_provenance_str,
                "diagnostics": diagnostics_str,
                EVIDENCE: json.dumps(evidence_payload),
                PRODUCTION_RUN_ID: production_run_id,
                CALCULATION_VERSION: THEME_RANK_CALCULATION_VERSION,
                CREATED_AT: now_ts,
            }
        )

    snapshot_columns = list(SYSTEM_B_THEME_RANK_SNAPSHOT.column_names())
    snapshot_df = pd.DataFrame(snapshot_rows, columns=snapshot_columns) if snapshot_rows else pd.DataFrame(columns=snapshot_columns)

    if total_canonical_count == 0:
        run_status = "EMPTY_UNIVERSE"
    elif is_popularity_unavailable:
        run_status = "INCOMPLETE_INPUT"
    else:
        run_status = "COMPLETE"

    return ThemeRankingResult(
        snapshot=snapshot_df,
        component_audit=audit_df,
        diagnostics=tuple(diagnostics_list),
        input_provenance=provenance_payload,
        run_status=run_status,
    )
