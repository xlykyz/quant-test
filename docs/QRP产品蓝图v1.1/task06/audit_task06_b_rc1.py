"""Reproduce the Task06-B rc1 design audit against the existing Task04/06-A code.

Run from this checkout with Python. All business data is synthetic and lives in
memory or a TemporaryDirectory; no configured QRP database or network is used.
This is an audit companion, not a Theme Rank implementation or a regression suite.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
import json
from pathlib import Path
import random
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb
import pandas as pd

from qrp_atlas.config import AppSettings
from qrp_atlas.contracts import DC_HOT, THS_HOT, init_database
from qrp_atlas.contracts.schema import (
    SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT,
    SYSTEM_B_ASSET_RANK_SNAPSHOT,
    THEME_CUSTOM_INDEX_STATE,
    THEME_M4_OBSERVATION,
)
from qrp_atlas.indicators.m5.observations import calculate_m5_raw_observations
from qrp_atlas.indicators.system_b.asset_ranking import rank_component
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.orchestration.models import JobDefinition, JobStatus, OverlapPolicy
from qrp_atlas.orchestration.scheduler import JobScheduler
from qrp_atlas.pipeline.contracts import TargetWindow
from qrp_atlas.pipeline.popularity_support import replace_dc_hot_batch, replace_ths_hot_batch
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.system_b_asset_rank.service import _persist
from qrp_atlas.pipeline.testing import ContractTestHarness
from qrp_atlas.pipeline.theme.m5_service import (
    ThemeM5PipelineService,
    validate_complete_popularity_frame,
)
from qrp_atlas.pipeline.theme.service import ThemePipelineService
from qrp_atlas.pipeline.theme_m5_contracts import THEME_M5_PRODUCTION_CONTRACT
from qrp_atlas.stock_collections.service import StockCollectionService


def create_theme(con, key, ticker, target):
    service = StockCollectionService(
        con, clock=lambda: datetime.combine(target - timedelta(days=3), datetime.min.time(), UTC)
    )
    theme, collection = service.create_canonical_theme(
        theme_name=key, source_key=key, effective_from=target, available_trade_date=target
    )
    service.add_member(
        theme_id=theme.theme_id, collection_id=collection.collection_id,
        asset_id=ticker, effective_from=target, available_trade_date=target,
    )
    return theme.theme_id, collection.collection_id


def m4_fixture(prices, suspended_positions=()):
    con = duckdb.connect(":memory:")
    init_database(con)
    days = list(pd.bdate_range("2026-08-03", periods=len(prices)).date)
    prior = list(pd.bdate_range(end="2026-07-31", periods=10).date)
    con.executemany(
        "INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, true)",
        [(day,) for day in prior + days],
    )
    con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('000001.SZ', 'Audit', '2020-01-01')")
    theme_id, collection_id = create_theme(con, "AUDIT", "000001.SZ", days[0])
    previous = 100.0
    for day, price in zip(prior + days, [100.0] * len(prior) + prices):
        pct = (price / previous - 1) * 100
        con.execute(
            "INSERT INTO daily_market_snapshot "
            "(trade_date,ticker,name,open,high,low,close,volume,amount,pct_change,is_limit_up) "
            "VALUES (?, '000001.SZ', 'Audit', ?, ?, ?, ?, 1000, 10000, ?, false)",
            [day, price, price, price, price, pct],
        )
        con.execute(
            "INSERT INTO ths_daily (trade_date,index_code,close,pct_change) VALUES (?, '881101.TI', 100, 0)",
            [day],
        )
        previous = price
    for position in suspended_positions:
        con.execute(
            "INSERT INTO suspend_d (trade_date,ticker,suspend_type) VALUES (?, '000001.SZ', 'S')",
            [days[position]],
        )
        con.execute("DELETE FROM daily_market_snapshot WHERE trade_date=?", [days[position]])
    return con, ThemePipelineService(con), days, theme_id, collection_id


def lifecycle_case():
    prices = [100, 100, 100, 100, 95, 105, 110, 112, 115, 118, 104, 103, 102]
    con, service, days, theme, _ = m4_fixture(prices)
    try:
        for day in days[:10]:
            service.run_m4_daily(day)
        target = days[9]
        before = con.execute("SELECT * FROM theme_m4_observation WHERE trade_date=?", [target]).fetchall()
        episode = con.execute(
            "SELECT episode_id,episode_start_date,episode_confirmed_date,episode_end_date,episode_return "
            "FROM theme_custom_index_episode"
        ).fetchone()
        assert episode[1:4] == (days[5], days[6], None)
        assert episode[4] == 0.0
        aligned = prices[9] / prices[5] - 1
        assert aligned > 0.12
        canonical_before = service._fetch_all_canonical_themes(target)
        for day in days[10:]:
            service.run_m4_daily(day)
        after = con.execute("SELECT * FROM theme_m4_observation WHERE trade_date=?", [target]).fetchall()
        assert before == after
        closed = con.execute(
            "SELECT episode_end_date,episode_return FROM theme_custom_index_episode WHERE episode_id=?",
            [episode[0]],
        ).fetchone()
        assert closed[0] == days[11]
        current_open = con.execute(
            "SELECT COUNT(*) FROM theme_m4_observation m JOIN theme_custom_index_episode e "
            "ON m.custom_index_episode_id=e.episode_id "
            "WHERE m.trade_date=? AND e.episode_end_date IS NULL", [target],
        ).fetchone()[0]
        end_day_pointer = con.execute(
            "SELECT custom_index_episode_id FROM theme_m4_observation WHERE trade_date=?", [days[11]]
        ).fetchone()[0]
        assert current_open == 0 and end_day_pointer == episode[0]
        assert service._fetch_all_canonical_themes(target) == canonical_before
        assert "custom_index_episode_id" not in THEME_CUSTOM_INDEX_STATE.column_names()
        assert "custom_index_episode_id" in THEME_M4_OBSERVATION.column_names()
        return {
            "target": target, "start": episode[1], "confirmed": episode[2], "end": closed[0],
            "stored_open_return": episode[4], "D_aligned_return": aligned,
            "historical_M4_unchanged": before == after,
            "rc1_IS_NULL_eligible_count_after_close": current_open,
            "end_day_keeps_episode_pointer": end_day_pointer == episode[0],
            "episode_pointer_table": THEME_M4_OBSERVATION.name,
        }
    finally:
        con.close()


def null_state_case():
    prices = [100, 100, 100, 100, 95, 105, 110, 110, 110, 110]
    con, service, days, theme, _ = m4_fixture(prices, suspended_positions=(7, 8))
    try:
        for day in days:
            service.run_m4_daily(day)
        states = con.execute(
            "SELECT trade_date,is_above_or_equal_ma5 FROM theme_custom_index_state "
            "WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date", [days[5], days[9]],
        ).fetchall()
        observation = con.execute(
            "SELECT effective_member_count,custom_index_episode_id,custom_index_trend_state "
            "FROM theme_m4_observation WHERE trade_date=?", [days[9]],
        ).fetchone()
        assert len(states) == 5 and [row[1] for row in states] == [True, True, None, None, None]
        assert observation[0] == 1 and observation[1] is not None and observation[2] is None
        assert con.execute("SELECT episode_end_date FROM theme_custom_index_episode").fetchone()[0] is None
        return {
            "target": days[9], "U_D_conditions_satisfied": True, "state_rows": len(states),
            "flags": [row[1] for row in states],
            "exclude_NULL_ratio": 1.0, "include_NULL_in_denominator_ratio": 2 / 5,
            "confirmed_trading_day_duration": 4,
        }
    finally:
        con.close()


def popularity_frame(table, target, hot_tickers):
    source, list_name = ("EASTMONEY", "POPULARITY") if table is DC_HOT else ("THS", "HOT_STOCK")
    fillers = [f"{600000 + number:06d}.SH" for number in range(100)]
    tickers = (list(hot_tickers) + fillers)[:100]
    timestamp = datetime.combine(target, datetime.min.time()) + timedelta(hours=10)
    rows = []
    for rank, ticker in enumerate(tickers, 1):
        row = dict(
            trade_date=target, source=source, list_name=list_name, ticker=ticker, name=ticker,
            rank_position=rank, pct_change=0.0, current_price=10.0,
            source_rank_time=str(timestamp), snapshot_seq=1,
            snapshot_started_at=timestamp, snapshot_completed_at=timestamp,
        )
        if table is THS_HOT:
            row.update(hot=float(101 - rank), concept="audit", rank_reason="audit")
        rows.append(row)
    frame = pd.DataFrame(rows, columns=[column for column in table.column_names() if column != "created_at"])
    return validate_complete_popularity_frame(
        frame, table_name=table.name, expected_source=source, expected_list_name=list_name, trade_date=target
    )


def popularity_cases():
    target = date(2026, 3, 2)
    with TemporaryDirectory(prefix="qrp-task06-b-audit-") as temporary:
        root = Path(temporary)
        settings = AppSettings.load(
            environ={"QRP_HOME": str(root / "home"), "QRP_DATA_DIR": str(root / "data"), "QRP_RUNTIME_ENV": "test"},
            project_root=root / "repo",
        )
        path = settings.paths.duckdb_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(path)) as con:
            init_database(con)
            con.executemany(
                "INSERT INTO stock_info (ticker,name,list_date) VALUES (?, ?, '2020-01-01')",
                [(ticker, ticker) for ticker in ("000001.SZ", "000002.SZ")],
            )
            alpha, _ = create_theme(con, "ALPHA", "000001.SZ", target)
            beta, _ = create_theme(con, "BETA", "000002.SZ", target)

        def context(source, run):
            return SimpleNamespace(
                settings=settings, target_window=TargetWindow.for_date(target),
                execution_control=ExecutionControl(), pipeline_id=f"{source}_ingest", run_id=run,
            )

        replace_dc_hot_batch(context("dc_hot", "AUDIT_DC_V1"), popularity_frame(DC_HOT, target, ["000001.SZ"]), ())
        replace_ths_hot_batch(context("ths_hot", "AUDIT_THS_EMPTY"), pd.DataFrame(), (target,))
        with duckdb.connect(str(path), read_only=True) as con:
            unavailable = con.execute(
                "SELECT source_status,valid_snapshot_count FROM popularity_source_availability WHERE source='ths_hot'"
            ).fetchone()
        assert unavailable == ("UNAVAILABLE", 0)
        result = ContractTestHarness(THEME_M5_PRODUCTION_CONTRACT, settings, registry=default_registry()).run(trade_date=target)
        assert result.status.value == "FAILED"
        assert "THEME_M5_THS_HOT_INPUT_INCOMPLETE" in json.dumps(result.as_dict(), default=str)

        # Exercise the real scheduler gate with the failed M5 outcome. The
        # proposed Task06-B definition and the store read are explicit stubs.
        definition = JobDefinition(
            job_id="audit_theme_rank", name="Audit", enabled=True, schedule="0 18 * * *",
            timezone="Asia/Shanghai", command=(), working_directory=None,
            dependencies=("theme_m5_production",), timeout_seconds=60, max_retries=0,
            overlap_policy=OverlapPolicy.FORBID, resource_locks=(),
        )
        store = SimpleNamespace(latest_run_before=lambda *_: SimpleNamespace(status=JobStatus.FAILED))
        gate, reason = JobScheduler(store, {definition.job_id: definition}).eligibility(
            definition, datetime(2026, 3, 2, 10, tzinfo=UTC)
        )
        assert gate is JobStatus.BLOCKED

        replace_ths_hot_batch(context("ths_hot", "AUDIT_THS_V1"), popularity_frame(THS_HOT, target, []), ())
        with duckdb.connect(str(path)) as con:
            ThemeM5PipelineService(con).run_m5_daily(target, production_run_id="AUDIT_M5_V1")
            stored = con.execute("SELECT * FROM theme_m5_observation ORDER BY theme_id").fetchdf().set_index("theme_id")
        replace_dc_hot_batch(context("dc_hot", "AUDIT_DC_V2"), popularity_frame(DC_HOT, target, ["000002.SZ"]), ())
        with duckdb.connect(str(path), read_only=True) as con:
            availability = con.execute("SELECT * FROM popularity_source_availability ORDER BY source").fetchdf()
            fresh = ThemeM5PipelineService(con).calculate_m5_facts(target)
            recomputed = fresh.observations.set_index("theme_id")
            assert set(availability.source_status) == {"AVAILABLE"}
            assert availability.valid_snapshot_count.tolist() == [1, 1]
            assert availability.snapshot_seqs.tolist() == ["[1]", "[1]"]
        denominator = int(availability.valid_snapshot_count.sum())
        old_rates = {key: int(stored.loc[key, "theme_hot_list_appearance_count"]) / denominator for key in (alpha, beta)}
        fresh_rates = {key: int(recomputed.loc[key, "theme_hot_list_appearance_count"]) / denominator for key in (alpha, beta)}
        assert list(old_rates.values()) == [0.5, 0.0] and list(fresh_rates.values()) == [0.0, 0.5]
        assert stored.loc[alpha, "input_snapshot_id"] != fresh.input_snapshot_id
        return {
            "unavailable": {"THS": unavailable, "M5_status": result.status.value,
                            "M5_diagnostics": [item.code for item in result.diagnostics],
                            "rank_dependency_gate": gate.value, "reason": reason},
            "mixed_versions": {
                "availability_counts": availability.valid_snapshot_count.tolist(),
                "availability_sequences": availability.snapshot_seqs.tolist(),
                "stale_M5_rates": old_rates, "current_source_rates": fresh_rates,
                "M5_input_fingerprint_mismatch": True, "all_rates_within_0_1": True,
            },
        }


def ranking_checks():
    weights = [35, 10, 15, 20, 10, 6, 4]
    dimensions = [(0, 1), (2, 3), (4,), (5, 6)]
    rng = random.Random(606)
    checked = 0
    for size in range(2, 10):
        for _ in range(20):
            leaves = [rank_component([rng.randrange(4) for _ in range(size)]) for _ in weights]
            active = [column for column, leaf in enumerate(leaves) if leaf.status == "OK"]
            if not active:
                continue
            total = sum(weights[column] for column in active)
            for row in range(size):
                score = Fraction(0)
                key = 0
                for column in active:
                    rank = Fraction(float(leaves[column].frame.iloc[row].raw_rank))
                    q = 2 * size - 2 * rank
                    assert q.denominator == 1
                    key += weights[column] * int(q)
                    score += Fraction(weights[column], total) * 100 * (size - rank) / (size - 1)
                assert score == Fraction(100 * key, 2 * (size - 1) * total)
                display_composite = Fraction(0)
                for group in dimensions:
                    group_active = [column for column in group if column in active]
                    budget = sum(weights[column] for column in group_active)
                    if budget:
                        display = sum(
                            Fraction(weights[column], budget) * 100
                            * (size - Fraction(float(leaves[column].frame.iloc[row].raw_rank))) / (size - 1)
                            for column in group_active
                        )
                        display_composite += Fraction(budget, total) * display
                assert display_composite == score
            checked += 1
    # With only 35 versus (15 + 20) active, opposite component orders produce
    # the exact same K and a final average rank of 1.5.
    assert 35 * 2 == 15 * 2 + 20 * 2
    ties = rank_component([70, 70])
    assert ties.frame.raw_rank.tolist() == [1.5, 1.5] and ties.status == "NO_VARIATION"
    missing = rank_component([1, None, 2])
    singleton = rank_component([1])
    assert missing.universe_size == 2
    assert singleton.status == "INSUFFICIENT_UNIVERSE"
    return {"exact_key_and_dimension_cases": checked, "composite_tie_rank": 1.5,
            "helper_finite_only_size_for_three_rows": missing.universe_size,
            "singleton_status": singleton.status}


def appearance_denominator_check():
    target = date(2026, 3, 2)
    tickers = [f"{number:06d}.SZ" for number in range(1, 21)]
    sources = []
    for table, count in ((DC_HOT, 3), (THS_HOT, 2)):
        snapshots = []
        for sequence in range(1, count + 1):
            snapshot = popularity_frame(table, target, tickers[:5])
            snapshot["snapshot_seq"] = sequence
            timestamp = datetime.combine(target, datetime.min.time()) + timedelta(hours=9, minutes=sequence)
            snapshot["snapshot_started_at"] = timestamp
            snapshot["snapshot_completed_at"] = timestamp
            snapshot["source_rank_time"] = str(timestamp)
            snapshots.append(snapshot)
        frame = pd.concat(snapshots, ignore_index=True)
        sources.append(validate_complete_popularity_frame(
            frame, table_name=table.name, expected_source=str(frame.iloc[0].source),
            expected_list_name=str(frame.iloc[0].list_name), trade_date=target,
        ))
    memberships = pd.DataFrame({"collection_id": "COL:AUDIT", "theme_id": "THM:AUDIT", "asset_id": tickers})
    result = calculate_m5_raw_observations(
        memberships, pd.concat(sources, ignore_index=True),
        theme_universe={"THM:AUDIT": "COL:AUDIT"}, trade_date=target,
    ).iloc[0]
    assert result.theme_hot_list_appearance_count == 25
    assert result.theme_member_count == 20 and result.theme_hot_stock_count == 5
    assert result.theme_hot_list_appearance_count / (result.theme_member_count * 5) == 0.25
    return {"appearance_count": 25, "member_count": 20, "source_snapshot_counts": [3, 2], "rate": 0.25}


def atomic_replace_check():
    con = duckdb.connect(":memory:")
    init_database(con)
    target = date(2026, 3, 2)
    common = {
        "trade_date": target, "ticker": "000001.SZ", "production_run_id": "AUDIT_OLD",
        "calculation_version": "audit", "created_at": datetime(2026, 3, 2, 12),
    }
    snapshot_row = {column: None for column in SYSTEM_B_ASSET_RANK_SNAPSHOT.column_names()}
    snapshot_row.update(common, input_provenance="{}", diagnostics="[]", evidence="{}")
    for dimension in ("m1", "m2", "m3"):
        snapshot_row.update({f"{dimension}_status": "OK", f"{dimension}_universe_size": 2})
    audit_row = {column: None for column in SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.column_names()}
    audit_row.update(common, dimension="M1", component="episode_return", direction="HIGHER_IS_BETTER",
                     universe_size=2, tie_count=1, status="OK", source_provenance="{}", metadata_json="{}")
    snapshot, audit = pd.DataFrame([snapshot_row]), pd.DataFrame([audit_row])
    try:
        _persist(con, snapshot, audit, target)
        tables = (SYSTEM_B_ASSET_RANK_SNAPSHOT.name, SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.name)
        before = [con.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() for table in tables]
        replacement = snapshot.copy()
        replacement["production_run_id"] = "AUDIT_NEW"
        try:
            _persist(con, replacement, pd.concat([audit, audit], ignore_index=True), target)
        except duckdb.ConstraintException:
            pass
        else:
            raise AssertionError("Expected duplicate audit key to fail after the snapshot INSERT")
        after = [con.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() for table in tables]
        assert before == after
        return {"existing_Task06_A_persist": "duplicate audit key rolls back both tables"}
    finally:
        con.close()


def main():
    results = {
        "lifecycle": lifecycle_case(), "present_but_NULL_state": null_state_case(),
        "popularity": popularity_cases(), "ranking": ranking_checks(),
        "appearance_denominator": appearance_denominator_check(), "atomicity": atomic_replace_check(),
    }
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    print("AUDIT REPRODUCTIONS PASSED")


if __name__ == "__main__":
    main()
