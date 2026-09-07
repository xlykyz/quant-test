"""Task06-B System B Theme Trend Rank production boundary tests."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CREATED_AT,
    DC_HOT,
    INPUT_SNAPSHOT_ID,
    POPULARITY_SOURCE_AVAILABILITY,
    PRODUCTION_RUN_ID,
    RANK_ELIGIBILITY_REASON,
    RANK_ELIGIBLE,
    SYSTEM_B_THEME_RANK_COMPONENT_AUDIT,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_M4_OBSERVATION_TABLE,
    THEME_M5_OBSERVATION_TABLE,
    THEME_M5_OBSERVATION_VERSION,
    THEME_RANK_ELIGIBLE,
    THEME_RANK_NO_OPEN_EPISODE,
    THEME_RANK_NOT_ELIGIBLE,
    THEME_STATUS,
    THS_HOT,
    TRADING_CALENDAR,
    init_database,
)
from qrp_atlas.pipeline.system_b_theme_rank import (
    SystemBThemeRankProductionError,
    get_theme_rank_component_audit,
    get_theme_rank_snapshot,
    run_theme_rank_daily,
)
from qrp_atlas.pipeline.theme.m5_service import (
    ThemeM5PipelineService,
    validate_complete_popularity_frame,
)
from qrp_atlas.stock_collections.service import StockCollectionService


def _popularity_frame(table, target: date, hot_tickers: list[str]) -> pd.DataFrame:
    source, list_name = ("EASTMONEY", "POPULARITY") if table is DC_HOT else ("THS", "HOT_STOCK")
    fillers = [f"{600000 + number:06d}.SH" for number in range(100)]
    tickers = (list(hot_tickers) + fillers)[:100]
    timestamp = datetime.combine(target, datetime.min.time()) + timedelta(hours=10)
    rows = []
    for rank, ticker in enumerate(tickers, 1):
        row = {
            "trade_date": target,
            "source": source,
            "list_name": list_name,
            "ticker": ticker,
            "name": ticker,
            "rank_position": rank,
            "pct_change": 0.0,
            "current_price": 10.0,
            "source_rank_time": str(timestamp),
            "snapshot_seq": 1,
            "snapshot_started_at": timestamp,
            "snapshot_completed_at": timestamp,
        }
        if table is THS_HOT:
            row.update(hot=float(101 - rank), concept="test", rank_reason="test")
        rows.append(row)
    frame = pd.DataFrame(rows, columns=[col for col in table.column_names() if col != "created_at"])
    return validate_complete_popularity_frame(
        frame,
        table_name=table.name,
        expected_source=source,
        expected_list_name=list_name,
        trade_date=target,
    )


def _setup_theme_db(path: Path) -> tuple[Path, list[date], str, str]:
    con = duckdb.connect(str(path))
    init_database(con)

    days = list(pd.bdate_range("2026-08-03", periods=15).date)
    con.executemany(
        f"INSERT INTO {TRADING_CALENDAR.name} (trade_date, is_open) VALUES (?, true)",
        [(d,) for d in days],
    )
    con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('000001.SZ', 'TestStock', '2020-01-01')")

    service = StockCollectionService(
        con,
        clock=lambda: datetime.combine(days[0] - timedelta(days=3), datetime.min.time(), UTC),
    )
    theme, collection = service.create_canonical_theme(
        theme_name="TEST_THEME",
        source_key="TEST_THEME",
        effective_from=days[0],
        available_trade_date=days[0],
    )
    service.add_member(
        theme_id=theme.theme_id,
        collection_id=collection.collection_id,
        asset_id="000001.SZ",
        effective_from=days[0],
        available_trade_date=days[0],
    )

    theme_id = theme.theme_id
    collection_id = collection.collection_id
    con.close()
    return path, days, theme_id, collection_id


def test_rc2_b1_closure_open_at_d_and_historical_replay(tmp_path: Path) -> None:
    """Test rc2 B1 closure: OPEN_AT_D historical projection and replay stability."""
    db_path, days, theme_id, collection_id = _setup_theme_db(tmp_path / "quant.duckdb")
    target_d = days[9]  # 2026-08-14
    end_e = days[12]    # 2026-08-19

    con = duckdb.connect(str(db_path))
    # Create Episode starting at days[5] (08-10), confirmed at days[6] (08-11), ending at end_e
    con.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_EPISODE_TABLE}
        (episode_id, theme_id, collection_id, episode_no, episode_start_date, episode_confirmed_date, episode_end_date, ma5_reentry_count, episode_return, rule_version, production_run_id, input_snapshot_id, created_at)
        VALUES ('EP:1', ?, ?, 1, ?, ?, ?, 0, 0.0, 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-14 10:00:00')
        """,
        [theme_id, collection_id, days[5], days[6], end_e],
    )
    # Custom index daily: start index 1000, target index 1120
    con.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, index_level, base_level, effective_member_count, total_member_count, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.0, 1000.0, 1000.0, 1, 1, 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-14 10:00:00')
        """,
        [theme_id, collection_id, days[5]],
    )
    con.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, index_level, base_level, effective_member_count, total_member_count, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.02, 1120.0, 1000.0, 1, 1, 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-14 10:00:00')
        """,
        [theme_id, collection_id, target_d],
    )
    # M4 observation for target_d
    con.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, total_member_count, comparison_universe_size, comparison_universe_version, custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id, qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.02, 0, 1, 1, 1, 1, 'v1', 'UP', 4, 'EP:1', 'QUALIFIED', 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-14 10:00:00')
        """,
        [theme_id, collection_id, target_d],
    )
    # States for days[5]..target_d
    for d_item in days[5:10]:
        con.execute(
            f"""
            INSERT INTO {THEME_CUSTOM_INDEX_STATE_TABLE}
            (theme_id, collection_id, trade_date, close, ma5, ma10, trend_state, previous_trend_state, custom_index_trend_run_days, is_above_or_equal_ma5, state_changed, rule_version, production_run_id, input_snapshot_id, created_at)
            VALUES (?, ?, ?, 100.0, 95.0, 90.0, 'UP', 'UP', 4, true, false, 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-14 10:00:00')
            """,
            [theme_id, collection_id, d_item],
        )

    # Popularity availability (Path A: UNAVAILABLE to test pure projection without M5)
    for src in ("dc_hot", "ths_hot"):
        con.execute(
            f"""
            INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
            (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
            VALUES (?, ?, 'UNAVAILABLE', 0, '[]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-14 10:00:00')
            """,
            [target_d, src],
        )
    con.close()

    # 1. Run for target_d (even though episode ends at end_e > target_d in the DB)
    report1 = run_theme_rank_daily(quant_database=db_path, trade_date=target_d, production_run_id="run_d_1")
    assert report1["status"] == "COMPLETED"
    snap1 = get_theme_rank_snapshot(db_path, target_d)
    assert len(snap1) == 1
    assert snap1.iloc[0][RANK_ELIGIBLE] == True
    assert snap1.iloc[0][RANK_ELIGIBILITY_REASON] == THEME_RANK_ELIGIBLE

    # Audit row for episode_return
    audit1 = get_theme_rank_component_audit(db_path, target_d)
    ep_ret_audit = audit1[audit1["component"] == "episode_return"].iloc[0]
    # D-aligned return: 1120 / 1000 - 1 = 0.12
    assert ep_ret_audit["raw_value"] == pytest.approx(0.12)

    # 2. Replay target_d -> Result must be identical
    report2 = run_theme_rank_daily(quant_database=db_path, trade_date=target_d, production_run_id="run_d_2")
    assert report2["status"] == "COMPLETED"
    snap2 = get_theme_rank_snapshot(db_path, target_d)
    pd.testing.assert_frame_equal(
        snap1.drop(columns=["production_run_id", "created_at"]),
        snap2.drop(columns=["production_run_id", "created_at"]),
    )

    # 3. On end date E, episode_end_date == E: legal end date形态
    con = duckdb.connect(str(db_path))
    con.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, total_member_count, comparison_universe_size, comparison_universe_version, custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id, qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, -0.05, 0, 1, 1, 1, 1, 'v1', 'BREAK', 0, 'EP:1', 'QUALIFIED', 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-19 10:00:00')
        """,
        [theme_id, collection_id, end_e],
    )
    for src in ("dc_hot", "ths_hot"):
        con.execute(
            f"""
            INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
            (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
            VALUES (?, ?, 'UNAVAILABLE', 0, '[]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-19 10:00:00')
            """,
            [end_e, src],
        )
    con.close()

    report_end = run_theme_rank_daily(quant_database=db_path, trade_date=end_e, production_run_id="run_e")
    assert report_end["status"] == "COMPLETED"
    snap_end = get_theme_rank_snapshot(db_path, end_e)
    assert len(snap_end) == 1
    assert snap_end.iloc[0][RANK_ELIGIBLE] == False
    assert snap_end.iloc[0][RANK_ELIGIBILITY_REASON] == THEME_RANK_NO_OPEN_EPISODE
    assert snap_end.iloc[0][THEME_STATUS] == THEME_RANK_NOT_ELIGIBLE


def test_rc2_m1_closure_trusted_unavailable_path_a_without_m5_success(tmp_path: Path) -> None:
    """Test rc2 M1 closure: trusted UNAVAILABLE allows execution without requiring M5 SUCCESS."""
    db_path, days, theme_id, collection_id = _setup_theme_db(tmp_path / "quant.duckdb")
    target = days[5]

    con = duckdb.connect(str(db_path))
    # M4 observation present
    con.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, total_member_count, comparison_universe_size, comparison_universe_version, custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id, qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.01, 0, 1, 1, 1, 1, 'v1', 'BREAK', 0, NULL, 'QUALIFIED', 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [theme_id, collection_id, target],
    )
    # Popularity availability: dc_hot AVAILABLE, ths_hot UNAVAILABLE
    con.execute(
        f"""
        INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
        (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
        VALUES (?, 'dc_hot', 'AVAILABLE', 1, '[1]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [target],
    )
    con.execute(
        f"""
        INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
        (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
        VALUES (?, 'ths_hot', 'UNAVAILABLE', 0, '[]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [target],
    )
    # NOTE: theme_m5_observation table is completely empty!
    con.close()

    # Must complete successfully in Path A without raising M5 missing error!
    report = run_theme_rank_daily(quant_database=db_path, trade_date=target)
    assert report["status"] == "COMPLETED"
    assert "THS_HOT_SOURCE_UNAVAILABLE" in report["diagnostics"]


def test_rc2_m2_closure_stale_m5_fingerprint_mismatch_fails_fast(tmp_path: Path) -> None:
    """Test rc2 M2 closure: same-day popularity modification with stale M5 causes fingerprint mismatch fail-fast."""
    db_path, days, theme_id, collection_id = _setup_theme_db(tmp_path / "quant.duckdb")
    target = days[5]

    con = duckdb.connect(str(db_path))
    # M4
    con.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, total_member_count, comparison_universe_size, comparison_universe_version, custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id, qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.01, 0, 1, 1, 1, 1, 'v1', 'BREAK', 0, NULL, 'QUALIFIED', 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [theme_id, collection_id, target],
    )
    # Both sources AVAILABLE
    for src in ("dc_hot", "ths_hot"):
        con.execute(
            f"""
            INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
            (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
            VALUES (?, ?, 'AVAILABLE', 1, '[1]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-10 10:00:00')
            """,
            [target, src],
        )
    # Stale persisted M5 row with old input_snapshot_id
    con.execute(
        f"""
        INSERT INTO {THEME_M5_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_member_count, theme_hot_stock_count, theme_hot_stock_ratio, theme_hot_list_appearance_count, theme_hot_source_count, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 1, 0, 0.0, 0, 0, '{THEME_M5_OBSERVATION_VERSION}', 'run1', 'SNAP:OLD_STALE_FINGERPRINT', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [theme_id, collection_id, target],
    )
    # Actual hot tables in DB have rows that compute a DIFFERENT fingerprint
    dc_frame = _popularity_frame(DC_HOT, target, ["000001.SZ"])
    ths_frame = _popularity_frame(THS_HOT, target, [])
    cols_dc = ", ".join(dc_frame.columns)
    con.register("_dc_import", dc_frame)
    con.execute(f"INSERT INTO {DC_HOT.name} ({cols_dc}) SELECT {cols_dc} FROM _dc_import")
    cols_ths = ", ".join(ths_frame.columns)
    con.register("_ths_import", ths_frame)
    con.execute(f"INSERT INTO {THS_HOT.name} ({cols_ths}) SELECT {cols_ths} FROM _ths_import")
    con.close()

    with pytest.raises(SystemBThemeRankProductionError, match="THEME_M5_INPUT_SNAPSHOT_ID_MISMATCH"):
        run_theme_rank_daily(quant_database=db_path, trade_date=target)


def test_atomic_replace_rollback_on_failure(tmp_path: Path) -> None:
    """Test that atomic persistence rolls back both tables when an error occurs."""
    db_path, days, theme_id, collection_id = _setup_theme_db(tmp_path / "quant.duckdb")
    target = days[5]

    con = duckdb.connect(str(db_path))
    con.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, total_member_count, comparison_universe_size, comparison_universe_version, custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id, qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.01, 0, 1, 1, 1, 1, 'v1', 'BREAK', 0, NULL, 'QUALIFIED', 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [theme_id, collection_id, target],
    )
    for src in ("dc_hot", "ths_hot"):
        con.execute(
            f"""
            INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
            (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
            VALUES (?, ?, 'UNAVAILABLE', 0, '[]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-10 10:00:00')
            """,
            [target, src],
        )
    con.close()

    # 1. Initial run succeeds
    run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_init")
    snap_before = get_theme_rank_snapshot(db_path, target)
    assert len(snap_before) == 1

    # 2. Simulate failure during _persist by corrupting a table or schema constraint
    con = duckdb.connect(str(db_path))
    from qrp_atlas.pipeline.system_b_theme_rank.service import _persist
    snap_dup = pd.concat([snap_before, snap_before], ignore_index=True)
    audit_empty = pd.DataFrame(columns=[c for c in SYSTEM_B_THEME_RANK_COMPONENT_AUDIT.column_names()])

    with pytest.raises(duckdb.ConstraintException):
        _persist(con, snap_dup, audit_empty, target)
    con.close()

    # Snapshot before remains intact!
    snap_after = get_theme_rank_snapshot(db_path, target)
    pd.testing.assert_frame_equal(snap_before, snap_after)


def _setup_path_b_db(tmp_path: Path) -> tuple[Path, date, str, str]:
    db_path, days, theme_id, collection_id = _setup_theme_db(tmp_path / "quant.duckdb")
    target = days[5]
    con = duckdb.connect(str(db_path))
    # M4
    con.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE}
        (theme_id, collection_id, trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, total_member_count, comparison_universe_size, comparison_universe_version, custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id, qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at)
        VALUES (?, ?, ?, 0.01, 0, 1, 1, 1, 1, 'v1', 'BREAK', 0, NULL, 'QUALIFIED', 'v1', 'run1', 'snap1', TIMESTAMP '2026-08-10 10:00:00')
        """,
        [theme_id, collection_id, target],
    )
    # Popularity availability (both AVAILABLE, 1 snapshot)
    for src in ("dc_hot", "ths_hot"):
        con.execute(
            f"""
            INSERT INTO {POPULARITY_SOURCE_AVAILABILITY.name}
            (trade_date, source, source_status, valid_snapshot_count, snapshot_seqs, input_version, source_provenance, source_pipeline_run_id, created_at)
            VALUES (?, ?, 'AVAILABLE', 1, '[1]', 'v1', '{{}}', 'run1', TIMESTAMP '2026-08-10 10:00:00')
            """,
            [target, src],
        )
    # Popularity tables
    dc_frame = _popularity_frame(DC_HOT, target, ["000001.SZ"])
    ths_frame = _popularity_frame(THS_HOT, target, [])
    cols_dc = ", ".join(dc_frame.columns)
    con.register("_dc_import", dc_frame)
    con.execute(f"INSERT INTO {DC_HOT.name} ({cols_dc}) SELECT {cols_dc} FROM _dc_import")
    cols_ths = ", ".join(ths_frame.columns)
    con.register("_ths_import", ths_frame)
    con.execute(f"INSERT INTO {THS_HOT.name} ({cols_ths}) SELECT {cols_ths} FROM _ths_import")

    # Run M5 service to calculate true facts and true snapshot_id
    m5_facts = ThemeM5PipelineService(con).calculate_m5_facts(target)
    true_obs = m5_facts.observations.copy()
    true_obs[CALCULATION_VERSION] = THEME_M5_OBSERVATION_VERSION
    true_obs[PRODUCTION_RUN_ID] = "m5_run_1"
    true_obs[INPUT_SNAPSHOT_ID] = m5_facts.input_snapshot_id
    true_obs[CREATED_AT] = datetime.combine(target, datetime.min.time(), UTC)
    con.register("_m5_import", true_obs)
    cols_m5 = ", ".join(true_obs.columns)
    con.execute(f"INSERT INTO {THEME_M5_OBSERVATION_TABLE} ({cols_m5}) SELECT {cols_m5} FROM _m5_import")
    con.close()
    return db_path, target, theme_id, collection_id


def test_path_b_m5_arithmetic_inconsistent_fails_fast_and_preserves_rank(tmp_path: Path) -> None:
    """Test that arithmetic inconsistencies in persisted M5 fail-fast and preserve prior Rank."""
    db_path, target, theme_id, _collection_id = _setup_path_b_db(tmp_path)

    # 1. Run successfully once to create prior Rank snapshot
    report = run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_init")
    assert report["status"] == "COMPLETED"
    snap_before = get_theme_rank_snapshot(db_path, target)
    assert len(snap_before) == 1

    # 2. Corrupt persisted M5 arithmetic: set hot_stock_count > member_count
    con = duckdb.connect(str(db_path))
    con.execute(
        f"UPDATE {THEME_M5_OBSERVATION_TABLE} SET theme_hot_stock_count = 99, theme_hot_stock_ratio = 99.0 WHERE theme_id = ?",
        [theme_id],
    )
    con.close()

    with pytest.raises(SystemBThemeRankProductionError, match="THEME_M5_ARITHMETIC_INCONSISTENT"):
        run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_bad")

    # Verify prior Rank snapshot is preserved
    snap_after = get_theme_rank_snapshot(db_path, target)
    pd.testing.assert_frame_equal(snap_before, snap_after)


def test_path_b_m5_business_output_mismatch_fails_fast_and_preserves_rank(tmp_path: Path) -> None:
    """Test that business output mismatch between persisted M5 and fresh calculation fails-fast."""
    db_path, target, theme_id, _collection_id = _setup_path_b_db(tmp_path)

    # 1. Initial run succeeds
    run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_init")
    snap_before = get_theme_rank_snapshot(db_path, target)

    # 2. Mutate persisted M5 to be internally consistent (hot_count = 0, appearance = 0, source = 0, ratio = 0)
    # but fresh fact calculation has hot_count = 1 (since 000001.SZ is in dc_hot!)
    con = duckdb.connect(str(db_path))
    con.execute(
        f"""
        UPDATE {THEME_M5_OBSERVATION_TABLE}
        SET theme_hot_stock_count = 0, theme_hot_stock_ratio = 0.0, theme_hot_list_appearance_count = 0, theme_hot_source_count = 0
        WHERE theme_id = ?
        """,
        [theme_id],
    )
    con.close()

    with pytest.raises(SystemBThemeRankProductionError, match="THEME_M5_BUSINESS_OUTPUT_MISMATCH"):
        run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_bad")

    snap_after = get_theme_rank_snapshot(db_path, target)
    pd.testing.assert_frame_equal(snap_before, snap_after)


def test_path_b_popularity_snapshot_count_and_seq_mismatch_fails_fast(tmp_path: Path) -> None:
    """Test that fresh DC/THS snapshot count/seq mismatch with availability fails-fast."""
    db_path, target, _theme_id, _collection_id = _setup_path_b_db(tmp_path)

    run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_init")
    snap_before = get_theme_rank_snapshot(db_path, target)

    # 1. Mutate availability count to 2 while DB has only 1 snapshot
    con = duckdb.connect(str(db_path))
    con.execute(
        f"UPDATE {POPULARITY_SOURCE_AVAILABILITY.name} SET valid_snapshot_count = 2, snapshot_seqs = '[1, 2]' WHERE source = 'dc_hot' AND trade_date = ?",
        [target],
    )
    con.close()

    with pytest.raises(SystemBThemeRankProductionError, match="POPULARITY_SNAPSHOT_COUNT_MISMATCH"):
        run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_bad_count")

    # 2. Mutate availability seq to [2] while DB has [1]
    con = duckdb.connect(str(db_path))
    con.execute(
        f"UPDATE {POPULARITY_SOURCE_AVAILABILITY.name} SET valid_snapshot_count = 1, snapshot_seqs = '[2]' WHERE source = 'dc_hot' AND trade_date = ?",
        [target],
    )
    con.close()

    with pytest.raises(SystemBThemeRankProductionError, match="POPULARITY_SNAPSHOT_SEQUENCE_MISMATCH"):
        run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_bad_seq")

    snap_after = get_theme_rank_snapshot(db_path, target)
    pd.testing.assert_frame_equal(snap_before, snap_after)


def test_snapshot_input_provenance_full_rc2_coverage(tmp_path: Path) -> None:
    """Test that snapshot input_provenance comprehensively covers all rc2 §12 required fields."""
    db_path, target, theme_id, _collection_id = _setup_path_b_db(tmp_path)

    run_theme_rank_daily(quant_database=db_path, trade_date=target, production_run_id="run_prov")
    snap = get_theme_rank_snapshot(db_path, target)
    assert len(snap) == 1

    prov_str = snap.iloc[0]["input_provenance"]
    prov = json.loads(prov_str)

    # 1. trade_date
    assert prov["trade_date"] == str(target)

    # 2. calculation_version
    assert prov["calculation_version"] == "system_b_theme_rank@0.1.0"

    # 3. c_d_provenance
    assert "c_d_provenance" in prov
    assert prov["c_d_provenance"]["canonical_themes_count"] == 1
    assert prov["c_d_provenance"]["canonical_theme_ids"] == [theme_id]
    assert len(prov["c_d_provenance"]["fingerprint"]) == 64

    # 4. m4_lineage
    assert "m4_lineage" in prov
    assert "calculation_version" in prov["m4_lineage"]
    assert "input_snapshot_id" in prov["m4_lineage"]

    # 5. m5_lineage (Path B: persisted == recomputed)
    assert "m5_lineage" in prov
    assert prov["m5_lineage"]["path"] == "PATH_B_AVAILABLE"
    assert prov["m5_lineage"]["persisted_input_snapshot_id"] is not None
    assert prov["m5_lineage"]["persisted_input_snapshot_id"] == prov["m5_lineage"]["recomputed_input_snapshot_id"]

    # 6. theme_index_state_episode_lineage
    assert "theme_index_state_episode_lineage" in prov
    assert "episodes" in prov["theme_index_state_episode_lineage"]
    assert "index_daily" in prov["theme_index_state_episode_lineage"]
    assert "index_state" in prov["theme_index_state_episode_lineage"]

    # 7. popularity_availability source metadata
    assert "popularity_availability" in prov
    assert "dc_hot" in prov["popularity_availability"]
    assert "ths_hot" in prov["popularity_availability"]
    assert prov["popularity_availability"]["dc_hot"]["source_status"] == "AVAILABLE"

    # 8. base_weights & effective_weights covering all 7 leaf components
    assert "base_weights" in prov
    expected_leaves = {
        "episode_return": 0.35,
        "theme_daily_return": 0.10,
        "episode_duration": 0.15,
        "episode_above_ma5_ratio": 0.20,
        "limit_up_diffusion": 0.10,
        "hot_stock_ratio": 0.06,
        "hot_appearance_rate": 0.04,
    }
    assert prov["base_weights"] == expected_leaves
    assert "effective_weights" in prov
    assert set(prov["effective_weights"].keys()) == set(expected_leaves.keys())
    assert sum(prov["base_weights"].values()) == pytest.approx(1.0)

