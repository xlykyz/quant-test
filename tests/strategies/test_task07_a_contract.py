"""Task07-A portfolio target contract and checked-runner tests."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from qrp_atlas.backtest.portfolio import strategy_result_to_target_weights
from qrp_atlas.backtest.runtime.strategy import StrategyBacktestRuntime
from qrp_atlas.backtest.models import BacktestConfig, CostRule, EntryRule, ExitRule, PositionRule
from qrp_atlas.strategies import (
    StrategyDefinition,
    StrategyHoldingState,
    StrategyInput,
    StrategyPortfolioTarget,
    StrategyPortfolioTargetPosition,
    StrategyRunResult,
    StrategyType,
    StrategyValidationError,
    run_strategy_checked,
)
from qrp_atlas.strategies.registry import StrategyRegistry


def _definition(code: str = "target_stub") -> StrategyDefinition:
    return StrategyDefinition(
        code=code,
        name=code,
        version="1.0.0",
        description="Task07-A test strategy",
        strategy_type=StrategyType.BUILTIN,
        required_fields=("ticker", "trade_date"),
        required_indicators=(),
    )


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ticker": "B", "trade_date": "2024-01-03"},
            {"ticker": "A", "trade_date": "2024-01-02"},
        ]
    )


class _StubStrategy:
    def __init__(self, result: StrategyRunResult) -> None:
        self.definition = result.definition
        self.result = result
        self.calls = 0
        self.received: StrategyInput | None = None

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        self.calls += 1
        self.received = strategy_input
        return self.result


def _native_result() -> StrategyRunResult:
    definition = _definition()
    return StrategyRunResult(
        definition=definition,
        parameters={},
        portfolio_targets=(
            StrategyPortfolioTarget(
                trade_date="2024-01-03",
                strategy_code=definition.code,
                strategy_version=definition.version,
                positions=(
                    StrategyPortfolioTargetPosition("B", 0.5, evidence={"z": [1, 2]}),
                ),
            ),
            StrategyPortfolioTarget(
                trade_date="2024-01-02",
                strategy_code=definition.code,
                strategy_version=definition.version,
                positions=(
                    StrategyPortfolioTargetPosition("B", 0.6, evidence={"z": 2, "a": {"b": 1}}),
                    StrategyPortfolioTargetPosition("A", 0.4),
                ),
            ),
            StrategyPortfolioTarget(
                trade_date="2024-01-04",
                strategy_code=definition.code,
                strategy_version=definition.version,
                positions=(),
            ),
        ),
    )


def test_checked_runner_canonicalizes_targets_and_native_full_snapshots() -> None:
    strategy = _StubStrategy(_native_result())

    result = run_strategy_checked(strategy, StrategyInput(prepared_data=_bars()))

    assert strategy.calls == 1
    assert [target.trade_date for target in result.portfolio_targets] == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    assert [position.asset_id for position in result.portfolio_targets[0].positions] == ["A", "B"]
    assert list(result.portfolio_targets[0].positions[1].evidence) == ["a", "z"]
    assert json.dumps(result.to_dict(), sort_keys=True, allow_nan=False)

    targets = strategy_result_to_target_weights(
        result,
        max_positions=1,
        max_weight_per_asset=1.0,
    )
    assert targets.to_dict("records") == [
        {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.4, "priority": 0.0},
        {"trade_date": "2024-01-02", "asset_id": "B", "target_weight": 0.6, "priority": 0.0},
        {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.0, "priority": 0.0},
        {"trade_date": "2024-01-03", "asset_id": "B", "target_weight": 0.5, "priority": 0.0},
        {"trade_date": "2024-01-04", "asset_id": "B", "target_weight": 0.0, "priority": 0.0},
    ]


def test_checked_runner_rejects_invalid_result_and_input_before_execution() -> None:
    definition = _definition()
    invalid = StrategyRunResult(
        definition=definition,
        parameters={},
        portfolio_targets=(
            StrategyPortfolioTarget(
                trade_date="2024-01-02",
                strategy_code=definition.code,
                strategy_version=definition.version,
                positions=(StrategyPortfolioTargetPosition("A", 1.0, evidence={"bad": {1}}),),
            ),
        ),
    )
    strategy = _StubStrategy(invalid)
    with pytest.raises(StrategyValidationError, match="JSON-compatible"):
        run_strategy_checked(strategy, StrategyInput(prepared_data=_bars()))
    assert strategy.calls == 1

    valid = _StubStrategy(StrategyRunResult(definition=definition, parameters={}))
    holdings = {
        "A": StrategyHoldingState(
            asset_id="A",
            current_weight=0.2,
            entry_count=1,
            first_entry_date="2024-01-01",
            last_entry_date="2024-01-01",
        )
    }
    with pytest.raises(StrategyValidationError, match="disagree"):
        run_strategy_checked(
            valid,
            StrategyInput(
                prepared_data=_bars(),
                initial_positions={"A": False},
                holdings=holdings,
                holdings_as_of_date="2024-01-01",
            ),
        )
    assert valid.calls == 0


def test_holdings_are_checked_against_normalized_evaluation_dates() -> None:
    definition = _definition()
    strategy = _StubStrategy(StrategyRunResult(definition=definition, parameters={}))
    holdings = {
        "A": StrategyHoldingState("A", 0.2, 2, "2023-12-20", "2024-01-01")
    }

    result = run_strategy_checked(
        strategy,
        StrategyInput(
            prepared_data=_bars(),
            initial_positions={"A": True},
            holdings=holdings,
            holdings_as_of_date="2024-01-01",
        ),
    )

    assert result.portfolio_targets == ()
    assert strategy.received is not None
    assert strategy.received.prepared_data["trade_date"].tolist() == ["2024-01-02", "2024-01-03"]


def test_event_normalizer_accepts_duplicate_event_identity_without_trade_date() -> None:
    definition = StrategyDefinition(
        code="event_drift_basic",
        name="Event Stub",
        version="1.0.0",
        description="event input test",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(
            "ticker",
            "announcement_date",
            "available_trade_date",
            "forecast_type",
            "profit_change_min",
            "profit_change_max",
            "event_series_id",
            "source_record_id",
        ),
        required_indicators=(),
    )
    strategy = _StubStrategy(StrategyRunResult(definition=definition, parameters={}))
    events = pd.DataFrame(
        [
            {
                "ticker": "A",
                "announcement_date": "2024-01-02",
                "available_trade_date": "2024-01-03",
                "forecast_type": "positive",
                "profit_change_min": 1.0,
                "profit_change_max": 2.0,
                "event_series_id": "series-2",
                "source_record_id": "source-2",
            },
            {
                "ticker": "A",
                "announcement_date": "2024-01-01",
                "available_trade_date": "2024-01-03",
                "forecast_type": "positive",
                "profit_change_min": 1.0,
                "profit_change_max": 2.0,
                "event_series_id": "series-1",
                "source_record_id": "source-1",
            },
        ]
    )

    run_strategy_checked(strategy, StrategyInput(prepared_data=events))

    assert strategy.received is not None
    assert "trade_date" not in strategy.received.prepared_data.columns
    assert strategy.received.prepared_data["source_record_id"].tolist() == ["source-1", "source-2"]


def test_registry_routes_through_checked_runner() -> None:
    definition = _definition("registry_target")
    strategy = _StubStrategy(StrategyRunResult(definition=definition, parameters={}))
    registry = StrategyRegistry()
    registry.register(strategy)

    result = registry.run(definition.code, StrategyInput(prepared_data=_bars()))

    assert result.definition == definition
    assert strategy.calls == 1


def test_legacy_runtime_fails_fast_for_native_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    from qrp_atlas.backtest.runtime import strategy as runtime_module

    result = _native_result()
    strategy = _StubStrategy(result)
    monkeypatch.setattr(runtime_module, "get_strategy", lambda *_args, **_kwargs: strategy)
    prices = pd.DataFrame(
        [
            {
                "asset_id": "A",
                "asset_name": "A",
                "asset_type": "stock",
                "trade_date": "2024-01-02",
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
            }
        ]
    )
    config = BacktestConfig(
        name="legacy-runtime",
        entry=EntryRule(timing="signal_close", price_field="close"),
        exit=ExitRule(type="hold_n_bars", bars=1, price_field="close"),
        position=PositionRule(100_000.0, 1.0, 1, False, False),
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0.0),
    )

    with pytest.raises(ValueError, match="does not support native portfolio targets"):
        StrategyBacktestRuntime().run(result.definition.code, prices, config)
