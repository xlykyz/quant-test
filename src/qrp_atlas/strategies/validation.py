"""Validation for declarations, prepared inputs, and strategy parameters."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.contracts import fields as contract_fields
from qrp_atlas.indicators import (
    IndicatorParameterBinding,
    IndicatorRequestError,
    get_calculation_definition,
    indicator_output_fields,
)
from qrp_atlas.indicators.registry import get_indicator

from .models import (
    ParameterSpec,
    StrategyDefinition,
    StrategyHoldingState,
    StrategyInput,
    StrategyInputScope,
    StrategyPortfolioTarget,
    StrategyPortfolioTargetPosition,
    StrategyRunResult,
)

if TYPE_CHECKING:
    from .protocol import StrategyProtocol



class StrategyValidationError(ValueError):
    """Raised when a strategy declaration or invocation violates its contract."""


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
StrategyInputNormalizer = Callable[[StrategyDefinition, StrategyInput], StrategyInput]


def known_contract_fields() -> frozenset[str]:
    """Return canonical field codes exported by the contracts SSOT."""

    return frozenset(
        value
        for name, value in vars(contract_fields).items()
        if name.isupper() and isinstance(value, str)
    )


def validate_definition(definition: StrategyDefinition) -> None:
    """Validate a strategy declaration against contracts and indicator metadata."""

    if not definition.code or not definition.version:
        raise StrategyValidationError("strategy code and version must be non-empty")
    if not isinstance(definition.input_scope, StrategyInputScope):
        raise StrategyValidationError(f"invalid input_scope: {definition.input_scope!r}")
    if len(set(definition.required_fields)) != len(definition.required_fields):
        raise StrategyValidationError("required_fields must not contain duplicates")
    if len(set(definition.required_indicators)) != len(definition.required_indicators):
        raise StrategyValidationError("required_indicators must not contain duplicates")

    unknown_fields = sorted(set(definition.required_fields) - known_contract_fields())
    if unknown_fields:
        raise StrategyValidationError(f"unknown contract fields: {unknown_fields}")

    for code in definition.required_indicators:
        try:
            get_indicator(code)
        except KeyError as exc:
            raise StrategyValidationError(str(exc)) from exc

    request_aliases: set[str] = set()
    request_outputs: set[str] = set(definition.required_indicators)
    for request in definition.indicator_requests:
        try:
            calculation = get_calculation_definition(request.code)
        except IndicatorRequestError as exc:
            raise StrategyValidationError(str(exc)) from exc
        unknown = sorted(set(request.parameters) - set(calculation.parameter_schema))
        if unknown:
            raise StrategyValidationError(
                f"indicator {request.code!r} has unknown parameters: {unknown}"
            )
        for value in request.parameters.values():
            if isinstance(value, IndicatorParameterBinding) and value.parameter not in definition.parameter_schema:
                raise StrategyValidationError(
                    f"indicator {request.code!r} references unknown strategy parameter {value.parameter!r}"
                )
            if isinstance(value, IndicatorParameterBinding) and request.alias is None:
                raise StrategyValidationError(
                    f"indicator {request.code!r} with parameter bindings requires an explicit alias"
                )
        alias = request.alias
        if alias is not None:
            if alias in request_aliases:
                raise StrategyValidationError(f"duplicate indicator alias: {alias}")
            request_aliases.add(alias)
    try:
        output_columns = indicator_output_fields(definition.indicator_requests)
    except IndicatorRequestError as exc:
        raise StrategyValidationError(str(exc)) from exc
    for output in output_columns:
        if output in request_outputs:
            raise StrategyValidationError(f"duplicate indicator output field: {output}")
        request_outputs.add(output)

    for code, spec in definition.parameter_schema.items():
        if not code:
            raise StrategyValidationError("parameter code must be non-empty")
        _validate_parameter_spec(code, spec)


def _validate_parameter_spec(code: str, spec: ParameterSpec) -> None:
    if spec.type not in {"number", "integer", "string", "boolean"}:
        raise StrategyValidationError(f"parameter {code!r} has unsupported type {spec.type!r}")
    if spec.minimum is not None and spec.maximum is not None and spec.minimum > spec.maximum:
        raise StrategyValidationError(f"parameter {code!r} minimum exceeds maximum")
    if spec.has_default:
        _validate_parameter_value(code, spec.default, spec)


def resolve_parameters(
    definition: StrategyDefinition, parameters: Mapping[str, Any]
) -> dict[str, Any]:
    """Apply defaults and validate caller-supplied strategy parameters."""

    if not isinstance(parameters, Mapping):
        raise StrategyValidationError("parameters must be a mapping")
    unknown = sorted(set(parameters) - set(definition.parameter_schema))
    if unknown:
        raise StrategyValidationError(f"unknown strategy parameters: {unknown}")

    resolved: dict[str, Any] = {}
    for code, spec in definition.parameter_schema.items():
        if code in parameters:
            value = parameters[code]
        elif spec.has_default:
            value = spec.default
        elif spec.required:
            raise StrategyValidationError(f"missing required parameter: {code}")
        else:
            value = None
        if value is not None:
            _validate_parameter_value(code, value, spec)
        resolved[code] = value
    return resolved


def _validate_parameter_value(code: str, value: Any, spec: ParameterSpec) -> None:
    valid_type = {
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "string": lambda v: isinstance(v, str),
        "boolean": lambda v: isinstance(v, bool),
    }[spec.type]
    if not valid_type(value):
        raise StrategyValidationError(
            f"parameter {code!r} must be {spec.type}, got {type(value).__name__}"
        )
    if spec.type in {"number", "integer"}:
        numeric = float(value)
        if not math.isfinite(numeric):
            raise StrategyValidationError(f"parameter {code!r} must be finite")
        if spec.minimum is not None and numeric < spec.minimum:
            raise StrategyValidationError(f"parameter {code!r} is below minimum {spec.minimum}")
        if spec.maximum is not None and numeric > spec.maximum:
            raise StrategyValidationError(f"parameter {code!r} is above maximum {spec.maximum}")


def validate_strategy_input(definition: StrategyDefinition, strategy_input: StrategyInput) -> pd.DataFrame:
    """Validate and canonically order a prepared input frame for deterministic runs."""

    if not isinstance(strategy_input, StrategyInput):
        raise StrategyValidationError("strategy_input must be a StrategyInput instance")
    df = strategy_input.prepared_data
    if not isinstance(df, pd.DataFrame):
        raise StrategyValidationError("prepared_data must be a pandas DataFrame")

    parameterized_outputs = indicator_output_fields(definition.indicator_requests)
    required_columns = tuple(
        dict.fromkeys((*definition.required_fields, *definition.required_indicators, *parameterized_outputs))
    )
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise StrategyValidationError(f"prepared_data missing required columns: {missing}")
    if df.empty:
        # Empty prepared data has no identity rows to validate. Preserve the
        # existing empty-result behavior for product/research callers.
        return df.copy()

    scope = getattr(definition, "input_scope", StrategyInputScope.ASSET)
    if scope is StrategyInputScope.MARKET:
        identity_fields = (TRADE_DATE,)
    else:
        identity_fields = (TICKER, TRADE_DATE)

    identity_missing = [column for column in identity_fields if column not in df.columns]
    if identity_missing:
        raise StrategyValidationError(
            f"prepared_data must include identity fields: {identity_missing}"
        )

    result = df.copy()
    parsed_dates = pd.to_datetime(result[TRADE_DATE], errors="coerce", format="mixed")
    if parsed_dates.isna().any():
        raise StrategyValidationError("prepared_data contains invalid trade_date values")
    result[TRADE_DATE] = parsed_dates.dt.strftime("%Y-%m-%d")


    if scope is StrategyInputScope.MARKET:
        duplicate_count = int(result.duplicated(subset=[TRADE_DATE], keep=False).sum())
        if duplicate_count:
            raise StrategyValidationError(
                f"prepared_data has {duplicate_count} duplicate trade_date rows"
            )
    else:
        if result[TICKER].isna().any() or (result[TICKER].astype(str).str.strip() == "").any():
            raise StrategyValidationError("prepared_data contains missing ticker values")
        result[TICKER] = result[TICKER].astype(str)
        duplicate_count = int(result.duplicated(subset=[TICKER, TRADE_DATE], keep=False).sum())
        if duplicate_count:
            raise StrategyValidationError(
                f"prepared_data has {duplicate_count} duplicate (ticker, trade_date) rows"
            )


    strict_columns = tuple(dict.fromkeys((*definition.required_fields, *definition.required_indicators)))
    for column in strict_columns:
        values = result[column]
        if values.isna().any():
            raise StrategyValidationError(f"prepared_data contains missing values for {column!r}")
        if pd.api.types.is_numeric_dtype(values) and not pd.api.types.is_bool_dtype(values):
            numeric = pd.to_numeric(values, errors="raise")
            if not numeric.map(math.isfinite).all():
                raise StrategyValidationError(
                    f"prepared_data contains non-finite values for {column!r}"
                )

    if not isinstance(strategy_input.initial_positions, Mapping):
        raise StrategyValidationError("initial_positions must be a mapping")
    if any(not isinstance(value, bool) for value in strategy_input.initial_positions.values()):
        raise StrategyValidationError("initial_positions values must be bool")

    if scope is StrategyInputScope.MARKET:
        return result.sort_values([TRADE_DATE], kind="mergesort").reset_index(drop=True)
    return result.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(drop=True)


def validate_and_normalize_strategy_input(
    definition: StrategyDefinition,
    strategy_input: StrategyInput,
) -> StrategyInput:
    """Apply the standard bar/market input contract and common holdings checks."""

    prepared_data = validate_strategy_input(definition, strategy_input)
    return _normalize_input_envelope(
        strategy_input,
        prepared_data,
        evaluation_date_column=TRADE_DATE,
    )


def validate_event_strategy_input(
    definition: StrategyDefinition,
    strategy_input: StrategyInput,
) -> StrategyInput:
    """Normalize the existing EventFrame contract without treating it as bar data."""

    if not isinstance(strategy_input, StrategyInput):
        raise StrategyValidationError("strategy_input must be a StrategyInput instance")
    if not isinstance(strategy_input.prepared_data, pd.DataFrame):
        raise StrategyValidationError("prepared_data must be a pandas DataFrame")

    result = strategy_input.prepared_data.copy()
    missing = [field for field in definition.required_fields if field not in result.columns]
    if missing:
        raise StrategyValidationError(f"prepared_data missing required columns: {missing}")
    for field in definition.required_fields:
        if result[field].isna().any():
            raise StrategyValidationError(f"prepared_data contains missing values for {field!r}")

    required_event_fields = (TICKER, "announcement_date", "available_trade_date")
    missing_event_fields = [field for field in required_event_fields if field not in result.columns]
    if missing_event_fields:
        raise StrategyValidationError(
            f"EventFrame must include fields: {missing_event_fields}"
        )
    if result[TICKER].isna().any() or (result[TICKER].astype(str).str.strip() == "").any():
        raise StrategyValidationError("EventFrame contains missing ticker values")
    result[TICKER] = result[TICKER].astype(str)
    for field in ("announcement_date", "available_trade_date"):
        parsed = pd.to_datetime(result[field], errors="coerce", format="mixed")
        if parsed.isna().any():
            raise StrategyValidationError(f"EventFrame contains invalid {field} values")
        result[field] = parsed.dt.strftime("%Y-%m-%d")

    sort_fields = ["available_trade_date", TICKER, "announcement_date"]
    if "source_record_id" in result.columns:
        result["source_record_id"] = result["source_record_id"].astype(str)
        sort_fields.append("source_record_id")
    result = result.sort_values(sort_fields, kind="mergesort").reset_index(drop=True)
    return _normalize_input_envelope(
        strategy_input,
        result,
        evaluation_date_column="available_trade_date",
    )


def _normalize_input_envelope(
    strategy_input: StrategyInput,
    prepared_data: pd.DataFrame,
    *,
    evaluation_date_column: str,
) -> StrategyInput:
    _validate_initial_positions(strategy_input.initial_positions)
    _validate_holdings(
        strategy_input.holdings,
        strategy_input.holdings_as_of_date,
        prepared_data,
        evaluation_date_column=evaluation_date_column,
    )
    _validate_holding_position_consistency(
        strategy_input.initial_positions,
        strategy_input.holdings,
    )
    return StrategyInput(
        prepared_data=prepared_data,
        parameters=dict(strategy_input.parameters),
        initial_positions=dict(strategy_input.initial_positions),
        runtime_context=dict(strategy_input.runtime_context),
        holdings=dict(strategy_input.holdings),
        holdings_as_of_date=strategy_input.holdings_as_of_date
        if strategy_input.holdings_as_of_date is not None
        else None,
    )


def _validate_initial_positions(initial_positions: Any) -> None:
    if not isinstance(initial_positions, Mapping):
        raise StrategyValidationError("initial_positions must be a mapping")
    for asset_id, held in initial_positions.items():
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise StrategyValidationError("initial_positions keys must be non-empty strings")
        if not isinstance(held, bool):
            raise StrategyValidationError("initial_positions values must be bool")


def _validate_holdings(
    holdings: Any,
    holdings_as_of_date: str | None,
    prepared_data: pd.DataFrame,
    *,
    evaluation_date_column: str,
) -> None:
    if not isinstance(holdings, Mapping):
        raise StrategyValidationError("holdings must be a mapping")
    if not holdings:
        if holdings_as_of_date is not None:
            _require_exact_date(holdings_as_of_date, "holdings_as_of_date")
        return

    as_of = _require_exact_date(holdings_as_of_date, "holdings_as_of_date")
    if evaluation_date_column not in prepared_data.columns or prepared_data.empty:
        raise StrategyValidationError(
            "holdings require at least one strategy evaluation date"
        )
    evaluation_dates = prepared_data[evaluation_date_column]
    if evaluation_dates.isna().any():
        raise StrategyValidationError("prepared_data contains missing evaluation dates")
    first_evaluation_date = min(str(value) for value in evaluation_dates)
    if as_of >= first_evaluation_date:
        raise StrategyValidationError(
            "holdings_as_of_date must be before the first strategy evaluation date"
        )

    for asset_id, state in holdings.items():
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise StrategyValidationError("holdings keys must be non-empty strings")
        if not isinstance(state, StrategyHoldingState):
            raise StrategyValidationError("holdings values must be StrategyHoldingState instances")
        if state.asset_id != asset_id:
            raise StrategyValidationError("holding key must equal StrategyHoldingState.asset_id")
        if (
            isinstance(state.current_weight, bool)
            or not isinstance(state.current_weight, (int, float))
            or not math.isfinite(float(state.current_weight))
            or float(state.current_weight) <= 0
        ):
            raise StrategyValidationError("holding current_weight must be a positive finite number")
        if (
            isinstance(state.entry_count, bool)
            or not isinstance(state.entry_count, int)
            or state.entry_count < 1
        ):
            raise StrategyValidationError("holding entry_count must be an integer >= 1")
        first_entry = (
            _require_exact_date(state.first_entry_date, "first_entry_date")
            if state.first_entry_date is not None
            else None
        )
        last_entry = (
            _require_exact_date(state.last_entry_date, "last_entry_date")
            if state.last_entry_date is not None
            else None
        )
        if first_entry is not None and first_entry > as_of:
            raise StrategyValidationError("first_entry_date must be on or before holdings_as_of_date")
        if last_entry is not None and last_entry > as_of:
            raise StrategyValidationError("last_entry_date must be on or before holdings_as_of_date")
        if first_entry is not None and last_entry is not None and first_entry > last_entry:
            raise StrategyValidationError("first_entry_date must be on or before last_entry_date")


def _validate_holding_position_consistency(
    initial_positions: Mapping[str, bool],
    holdings: Mapping[str, StrategyHoldingState],
) -> None:
    # An empty default holdings mapping means the caller is using the legacy
    # boolean contract only. Compare representations only when typed state was
    # actually supplied.
    if not holdings:
        return
    for asset_id in set(initial_positions) | set(holdings):
        if bool(initial_positions.get(asset_id, False)) != (asset_id in holdings):
            raise StrategyValidationError(
                "initial_positions and holdings disagree for asset " f"{asset_id!r}"
            )


def _require_exact_date(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _ISO_DATE_RE.fullmatch(value):
        raise StrategyValidationError(f"{field} must be an exact YYYY-MM-DD string")
    try:
        pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise StrategyValidationError(f"{field} must be a valid date") from exc
    return value


def run_strategy_checked(
    strategy: "StrategyProtocol",
    strategy_input: StrategyInput,
    *,
    input_normalizer: StrategyInputNormalizer | None = None,
) -> StrategyRunResult:
    """Validate input, execute exactly once, then return a canonical result."""

    normalizer = input_normalizer
    if normalizer is None:
        normalizer = (
            validate_event_strategy_input
            if strategy.definition.code == "event_drift_basic"
            else validate_and_normalize_strategy_input
        )
    normalized_input = normalizer(strategy.definition, strategy_input)
    result = strategy.run(normalized_input)
    return validate_strategy_result(strategy.definition, result)


def validate_strategy_result(
    definition: StrategyDefinition,
    result: StrategyRunResult,
) -> StrategyRunResult:
    """Validate and canonically order native portfolio target output."""

    if not isinstance(result, StrategyRunResult):
        raise StrategyValidationError("strategy.run must return a StrategyRunResult")
    if (
        result.definition.code != definition.code
        or result.definition.version != definition.version
    ):
        raise StrategyValidationError("strategy result definition does not match executed strategy")
    if any(not isinstance(item, str) for item in result.diagnostics):
        raise StrategyValidationError("strategy result diagnostics must contain only strings")
    if not isinstance(result.portfolio_targets, tuple):
        raise StrategyValidationError("portfolio_targets must be a tuple")

    dates: set[str] = set()
    canonical_targets: list[StrategyPortfolioTarget] = []
    for target in result.portfolio_targets:
        if not isinstance(target, StrategyPortfolioTarget):
            raise StrategyValidationError("portfolio_targets must contain StrategyPortfolioTarget values")
        trade_date = _require_exact_date(target.trade_date, "portfolio target trade_date")
        if trade_date in dates:
            raise StrategyValidationError("portfolio target trade_date values must be unique")
        dates.add(trade_date)
        if (
            target.strategy_code != definition.code
            or target.strategy_version != definition.version
        ):
            raise StrategyValidationError("portfolio target strategy code/version does not match result")
        if any(not isinstance(item, str) for item in target.diagnostics):
            raise StrategyValidationError("portfolio target diagnostics must contain only strings")
        positions = _validate_target_positions(target.positions)
        canonical_targets.append(
            StrategyPortfolioTarget(
                trade_date=trade_date,
                strategy_code=target.strategy_code,
                strategy_version=target.strategy_version,
                positions=positions,
                diagnostics=tuple(target.diagnostics),
            )
        )
    canonical_targets.sort(key=lambda item: item.trade_date)
    return replace(result, portfolio_targets=tuple(canonical_targets))


def _validate_target_positions(
    positions: Any,
) -> tuple[StrategyPortfolioTargetPosition, ...]:
    if not isinstance(positions, tuple):
        raise StrategyValidationError("portfolio target positions must be a tuple")
    assets: set[str] = set()
    canonical: list[StrategyPortfolioTargetPosition] = []
    total_weight = 0.0
    for position in positions:
        if not isinstance(position, StrategyPortfolioTargetPosition):
            raise StrategyValidationError(
                "portfolio target positions must contain StrategyPortfolioTargetPosition values"
            )
        if not isinstance(position.asset_id, str) or not position.asset_id.strip():
            raise StrategyValidationError("portfolio target position asset_id must be non-empty")
        if position.asset_id in assets:
            raise StrategyValidationError("portfolio target position asset_id values must be unique")
        assets.add(position.asset_id)
        if (
            isinstance(position.target_weight, bool)
            or not isinstance(position.target_weight, (int, float))
            or not math.isfinite(float(position.target_weight))
            or not 0 <= float(position.target_weight) <= 1
        ):
            raise StrategyValidationError(
                "portfolio target position target_weight must be finite and in [0, 1]"
            )
        if position.reason_code is not None and not isinstance(position.reason_code, str):
            raise StrategyValidationError("portfolio target position reason_code must be a string or None")
        total_weight += float(position.target_weight)
        canonical.append(
            StrategyPortfolioTargetPosition(
                asset_id=position.asset_id,
                target_weight=float(position.target_weight),
                reason_code=position.reason_code,
                evidence=_canonical_json_value(position.evidence),
            )
        )
    if total_weight > 1.0 + 1e-12:
        raise StrategyValidationError("portfolio target weights must sum to <= 1")
    return tuple(sorted(canonical, key=lambda item: item.asset_id))


def _canonical_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise StrategyValidationError("portfolio target evidence must not contain NaN or infinity")
        return value
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise StrategyValidationError("portfolio target evidence mapping keys must be strings")
        return {
            key: _canonical_json_value(value[key])
            for key in sorted(value)
        }
    raise StrategyValidationError("portfolio target evidence must be JSON-compatible")
