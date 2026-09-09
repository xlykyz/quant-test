"""Core models and contract validation for the Research Backtest Harness (Task08)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from enum import StrEnum
import hashlib
import json
from typing import Any
import uuid

import pandas as pd


ALLOWED_UNIVERSE_PRESETS = frozenset({"all_a"})
ALLOWED_FILTER_OPS = frozenset({"eq", "ne", "gt", "ge", "lt", "le", "in", "not_in"})


class HarnessError(Exception):
    """Base exception for backtest harness."""


class HarnessValidationError(HarnessError, ValueError):
    """Raised when request, subject, or configuration validation fails."""


class SubjectType(StrEnum):
    FACTOR = "factor"
    EXPERIMENT = "experiment"
    STRATEGY = "strategy"


@dataclass(frozen=True)
class FactorSpec:
    """Specification for factor evaluation."""

    field: str
    direction: str = "higher_is_better"
    horizons: tuple[int, ...] = (5, 20)
    quantiles: int = 5
    params: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.field or not str(self.field).strip():
            raise HarnessValidationError("FactorSpec.field must not be empty")
        if self.direction not in {"higher_is_better", "lower_is_better"}:
            raise HarnessValidationError(
                f"FactorSpec.direction must be 'higher_is_better' or 'lower_is_better', got: {self.direction!r}"
            )
        if not self.horizons or any(int(h) <= 0 for h in self.horizons):
            raise HarnessValidationError("FactorSpec.horizons must be positive integers")
        if int(self.quantiles) <= 1:
            raise HarnessValidationError("FactorSpec.quantiles must be >= 2")

    @classmethod
    def from_input(cls, raw: FactorSpec | Mapping[str, Any]) -> FactorSpec:
        if isinstance(raw, FactorSpec):
            return raw
        if not isinstance(raw, Mapping):
            raise HarnessValidationError(f"factor must be a FactorSpec or mapping, got: {type(raw).__name__}")
        field_name = raw.get("field")
        if not field_name:
            raise HarnessValidationError("factor specification missing 'field'")
        direction = raw.get("direction", "higher_is_better")
        raw_horizons = raw.get("horizons", (5, 20))
        if isinstance(raw_horizons, int):
            horizons = (raw_horizons,)
        else:
            horizons = tuple(int(h) for h in raw_horizons)
        quantiles = int(raw.get("quantiles", 5))
        params = dict(raw.get("params", {}))
        return cls(
            field=str(field_name).strip(),
            direction=str(direction).strip(),
            horizons=horizons,
            quantiles=quantiles,
            params=params,
        )


@dataclass(frozen=True)
class FilterPredicate:
    """Structured predicate for Experiment filtering without eval()."""

    field: str
    op: str
    value: Any

    def __post_init__(self) -> None:
        if not self.field or not str(self.field).strip():
            raise HarnessValidationError("FilterPredicate.field must not be empty")
        if self.op not in ALLOWED_FILTER_OPS:
            raise HarnessValidationError(
                f"FilterPredicate.op must be one of {sorted(ALLOWED_FILTER_OPS)}, got: {self.op!r}"
            )

    @classmethod
    def from_input(cls, raw: FilterPredicate | Mapping[str, Any] | str) -> FilterPredicate | str:
        if isinstance(raw, (FilterPredicate, str)):
            return raw
        if not isinstance(raw, Mapping):
            raise HarnessValidationError(f"filter must be string or dict predicate, got: {type(raw).__name__}")
        field_name = raw.get("field")
        op = raw.get("op")
        if not field_name or not op:
            raise HarnessValidationError("Filter predicate requires 'field' and 'op'")
        return cls(
            field=str(field_name).strip(),
            op=str(op).strip().lower(),
            value=raw.get("value"),
        )


@dataclass(frozen=True)
class ExperimentSpec:
    """Specification for partial rule experimentation."""

    score: str | Mapping[str, float]
    filter: str | FilterPredicate | tuple[FilterPredicate, ...] | None = None
    rank: Mapping[str, Any] = field(default_factory=lambda: {"by": "score", "order": "desc"})
    portfolio: Mapping[str, Any] = field(default_factory=lambda: {"top_n": 6, "weight_each": 0.125})
    exit: str = "when_not_selected"
    research_assumptions: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Score validation
        if isinstance(self.score, str):
            if not self.score.strip():
                raise HarnessValidationError("ExperimentSpec.score string must not be empty")
        elif isinstance(self.score, Mapping):
            if not self.score:
                raise HarnessValidationError("ExperimentSpec.score linear weights dict must not be empty")
            for k, w in self.score.items():
                if not str(k).strip():
                    raise HarnessValidationError("score linear weight field name cannot be empty")
                try:
                    float(w)
                except (TypeError, ValueError) as exc:
                    raise HarnessValidationError(f"score linear weight for {k} must be numeric, got: {w!r}") from exc
        else:
            raise HarnessValidationError(
                f"ExperimentSpec.score must be str or dict of weights, got: {type(self.score).__name__}"
            )

        # Rank validation
        if not isinstance(self.rank, Mapping):
            raise HarnessValidationError("ExperimentSpec.rank must be a mapping")
        rank_by = str(self.rank.get("by", "")).strip()
        if rank_by != "score":
            raise HarnessValidationError(
                f"ExperimentSpec.rank['by'] must be 'score', got: {self.rank.get('by')!r}. "
                "Task08 v1.1 freezes ranking on the computed score."
            )
        rank_order = str(self.rank.get("order", "")).strip().lower()
        if rank_order not in {"asc", "desc"}:
            raise HarnessValidationError(
                f"ExperimentSpec.rank['order'] must be 'asc' or 'desc', got: {self.rank.get('order')!r}"
            )

        # Portfolio validation
        top_n = self.portfolio.get("top_n")
        if top_n is None or int(top_n) <= 0:
            raise HarnessValidationError("ExperimentSpec.portfolio requires positive integer 'top_n'")
        weight_each = self.portfolio.get("weight_each")
        if weight_each is not None:
            if not 0.0 < float(weight_each) <= 1.0:
                raise HarnessValidationError("ExperimentSpec.portfolio 'weight_each' must be in (0, 1]")

        # Exit validation
        if self.exit != "when_not_selected":
            raise HarnessValidationError(
                f"ExperimentSpec.exit currently only supports 'when_not_selected', got: {self.exit!r}"
            )

    @classmethod
    def from_input(cls, raw: ExperimentSpec | Mapping[str, Any]) -> ExperimentSpec:
        if isinstance(raw, ExperimentSpec):
            return raw
        if not isinstance(raw, Mapping):
            raise HarnessValidationError(f"experiment must be an ExperimentSpec or mapping, got: {type(raw).__name__}")
        raw_score = raw.get("score")
        if raw_score is None:
            raise HarnessValidationError("experiment requires 'score'")
        score: str | dict[str, float]
        if isinstance(raw_score, str):
            score = str(raw_score).strip()
        elif isinstance(raw_score, Mapping):
            score = {str(k).strip(): float(v) for k, v in raw_score.items()}
        else:
            raise HarnessValidationError(f"score must be str or dict, got: {type(raw_score).__name__}")

        raw_filter = raw.get("filter")
        filter_spec: str | FilterPredicate | tuple[FilterPredicate, ...] | None = None
        if raw_filter is not None:
            if isinstance(raw_filter, str):
                filter_spec = raw_filter.strip()
            elif isinstance(raw_filter, (FilterPredicate, Mapping)):
                pred = FilterPredicate.from_input(raw_filter)
                filter_spec = pred if isinstance(pred, FilterPredicate) else pred
            elif isinstance(raw_filter, Sequence):
                preds = []
                for item in raw_filter:
                    parsed = FilterPredicate.from_input(item)
                    if isinstance(parsed, FilterPredicate):
                        preds.append(parsed)
                    else:
                        raise HarnessValidationError("Sequence filters must be structured predicates")
                filter_spec = tuple(preds)
            else:
                raise HarnessValidationError(f"Invalid filter format: {raw_filter!r}")

        rank = dict(raw.get("rank") or {"by": "score", "order": "desc"})
        portfolio = dict(raw.get("portfolio") or {"top_n": 6, "weight_each": 0.125})
        exit_rule = str(raw.get("exit") or "when_not_selected").strip()
        assumptions = dict(raw.get("research_assumptions") or {})

        return cls(
            score=score,
            filter=filter_spec,
            rank=rank,
            portfolio=portfolio,
            exit=exit_rule,
            research_assumptions=assumptions,
        )


@dataclass(frozen=True)
class StrategySpec:
    """Specification for formal strategy replay."""

    code: str
    params: Mapping[str, Any] = field(default_factory=dict)
    version: str | None = None
    runtime_context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.code or not str(self.code).strip():
            raise HarnessValidationError("StrategySpec.code must not be empty")

    @classmethod
    def from_input(cls, raw: StrategySpec | Mapping[str, Any]) -> StrategySpec:
        if isinstance(raw, StrategySpec):
            return raw
        if not isinstance(raw, Mapping):
            raise HarnessValidationError(f"strategy must be a StrategySpec or mapping, got: {type(raw).__name__}")
        code = raw.get("code")
        if not code:
            raise HarnessValidationError("strategy requires 'code'")
        params = dict(raw.get("params") or {})
        version = raw.get("version")
        runtime_context = dict(raw.get("runtime_context") or {})
        return cls(
            code=str(code).strip(),
            params=params,
            version=str(version).strip() if version else None,
            runtime_context=runtime_context,
        )


@dataclass(frozen=True)
class ExecutionSpec:
    """Execution configuration and preset overrides."""

    preset: str = "a_share_daily"
    initial_cash: float | None = None
    max_positions: int | None = None
    max_weight_per_asset: float | None = None
    price_field: str | None = None
    commission_rate: float | None = None
    stamp_tax_rate: float | None = None
    slippage_rate: float | None = None
    overrides: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_input(cls, raw: ExecutionSpec | Mapping[str, Any] | None) -> ExecutionSpec:
        if raw is None:
            return cls()
        if isinstance(raw, ExecutionSpec):
            return raw
        if not isinstance(raw, Mapping):
            raise HarnessValidationError(f"execution must be an ExecutionSpec or mapping, got: {type(raw).__name__}")
        preset = str(raw.get("preset") or "a_share_daily").strip()
        initial_cash = raw.get("initial_cash")
        max_positions = raw.get("max_positions")
        max_weight = raw.get("max_weight_per_asset")
        price_field = raw.get("price_field")
        commission = raw.get("commission_rate")
        stamp_tax = raw.get("stamp_tax_rate")
        slippage = raw.get("slippage_rate")
        known_keys = {
            "preset",
            "initial_cash",
            "max_positions",
            "max_weight_per_asset",
            "price_field",
            "commission_rate",
            "stamp_tax_rate",
            "slippage_rate",
            "overrides",
        }
        extra_overrides = {k: v for k, v in raw.items() if k not in known_keys}
        if "overrides" in raw and isinstance(raw["overrides"], Mapping):
            extra_overrides.update(raw["overrides"])

        return cls(
            preset=preset,
            initial_cash=float(initial_cash) if initial_cash is not None else None,
            max_positions=int(max_positions) if max_positions is not None else None,
            max_weight_per_asset=float(max_weight) if max_weight is not None else None,
            price_field=str(price_field).strip() if price_field else None,
            commission_rate=float(commission) if commission is not None else None,
            stamp_tax_rate=float(stamp_tax) if stamp_tax is not None else None,
            slippage_rate=float(slippage) if slippage is not None else None,
            overrides=extra_overrides,
        )


def _normalize_date_str(val: Any) -> str:
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m-%d")
    text = str(val).strip()
    try:
        ts = pd.Timestamp(text)
        return ts.strftime("%Y-%m-%d")
    except Exception as exc:
        raise HarnessValidationError(f"Invalid date format: {val!r}") from exc


@dataclass(frozen=True)
class BackRequest:
    """Canonical, normalized backtest request."""

    period: tuple[str, str]
    universe: str | tuple[str, ...] | None
    subject_type: SubjectType
    factor: FactorSpec | None = None
    experiment: ExperimentSpec | None = None
    strategy: StrategySpec | None = None
    execution: ExecutionSpec = field(default_factory=ExecutionSpec)
    output: Mapping[str, Any] = field(default_factory=dict)
    data: Mapping[str, pd.DataFrame] | None = None

    def __post_init__(self) -> None:
        # Validate period
        if not isinstance(self.period, (tuple, list)) or len(self.period) != 2:
            raise HarnessValidationError(f"period must be a 2-tuple (start, end), got: {self.period!r}")
        start, end = _normalize_date_str(self.period[0]), _normalize_date_str(self.period[1])
        if start > end:
            raise HarnessValidationError(f"period start ({start}) must be <= end ({end})")

        # Validate subject exactly-one
        subjects_set = [
            (SubjectType.FACTOR, self.factor is not None),
            (SubjectType.EXPERIMENT, self.experiment is not None),
            (SubjectType.STRATEGY, self.strategy is not None),
        ]
        provided = [st for st, present in subjects_set if present]
        if len(provided) != 1:
            raise HarnessValidationError(
                f"Exactly one of factor, experiment, or strategy must be provided, got: {provided}"
            )
        if provided[0] != self.subject_type:
            raise HarnessValidationError(f"subject_type {self.subject_type} does not match provided {provided[0]}")

        # Validate universe
        if self.universe is not None:
            if isinstance(self.universe, str):
                if self.universe == "system_b_active_pools":
                    raise HarnessValidationError(
                        "Universe preset 'system_b_active_pools' is disabled in Task08 to prevent PIT universe leakage "
                        "(daily dynamic membership resolver pending). Use an explicit ticker list or 'all_a'."
                    )
                if self.universe not in ALLOWED_UNIVERSE_PRESETS:
                    raise HarnessValidationError(
                        f"Unknown universe preset {self.universe!r}; allowed: {sorted(ALLOWED_UNIVERSE_PRESETS)}"
                    )
            elif isinstance(self.universe, (tuple, list)):
                if not self.universe:
                    raise HarnessValidationError("universe sequence cannot be empty")
                for t in self.universe:
                    if not str(t).strip():
                        raise HarnessValidationError("universe ticker cannot be blank")
            else:
                raise HarnessValidationError(f"universe must be str or sequence of str, got: {type(self.universe).__name__}")
        else:
            # Universe omitted is allowed only for strategy mode or when data is injected with universe
            if self.subject_type in {SubjectType.FACTOR, SubjectType.EXPERIMENT} and (
                self.data is None or "prices" not in self.data
            ):
                raise HarnessValidationError(f"universe is required for {self.subject_type} mode when data is not injected")

        # Validate data if provided
        if self.data is not None:
            if not isinstance(self.data, Mapping):
                raise HarnessValidationError(f"data must be a mapping of DataFrame panels, got: {type(self.data).__name__}")
            for k, df in self.data.items():
                if not isinstance(df, pd.DataFrame):
                    raise HarnessValidationError(f"data[{k!r}] must be a pandas DataFrame, got: {type(df).__name__}")

    @property
    def subject(self) -> FactorSpec | ExperimentSpec | StrategySpec:
        if self.factor is not None:
            return self.factor
        if self.experiment is not None:
            return self.experiment
        if self.strategy is not None:
            return self.strategy
        raise RuntimeError("No subject configured")

    def to_canonical_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary with stable key sorting."""
        subject_dict: dict[str, Any]
        if self.factor is not None:
            subject_dict = {"factor": asdict(self.factor)}
        elif self.experiment is not None:
            exp_dict = asdict(self.experiment)
            subject_dict = {"experiment": exp_dict}
        else:
            subject_dict = {"strategy": asdict(self.strategy)}

        univ: Any = self.universe
        if isinstance(univ, tuple):
            univ = list(univ)

        return {
            "period": [self.period[0], self.period[1]],
            "universe": univ,
            "subject_type": self.subject_type.value,
            **subject_dict,
            "execution": asdict(self.execution),
            "output": dict(self.output),
            "data_injected": self.data is not None,
            "data_keys": sorted(self.data.keys()) if self.data is not None else [],
        }

    def compute_config_hash(self) -> str:
        canonical_json = json.dumps(self.to_canonical_dict(), sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _make_json_safe(val: Any) -> Any:
    if isinstance(val, (datetime, date, pd.Timestamp)):
        return val.isoformat()
    if isinstance(val, Mapping):
        return {str(k): _make_json_safe(v) for k, v in val.items()}
    if isinstance(val, (list, tuple, set)):
        return [_make_json_safe(x) for x in val]
    if isinstance(val, (int,)):
        return int(val)
    if isinstance(val, (float,)):
        import math
        return None if (pd.isna(val) or not math.isfinite(val)) else float(val)
    if isinstance(val, (bool,)):
        return bool(val)
    try:
        import numpy as np
        if isinstance(val, (np.integer,)):
            return int(val)
        if isinstance(val, (np.floating,)):
            import math
            return None if (pd.isna(val) or not math.isfinite(val)) else float(val)
        if isinstance(val, (np.bool_,)):
            return bool(val)
    except ImportError:
        pass
    if pd.isna(val):
        return None
    return val


@dataclass(frozen=True)
class BackResult:
    """Unified container for all backtest and historical replay results."""

    status: str
    run_id: str
    subject_type: SubjectType
    request_snapshot: Mapping[str, Any]
    provenance: Mapping[str, Any]
    summary_metrics: Mapping[str, Any]
    factor: Mapping[str, Any] | None = None
    portfolio: Mapping[str, Any] | None = None
    replay: Mapping[str, Any] | None = None
    warnings: tuple[str, ...] = ()
    artifacts: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert BackResult to serializable dictionary."""
        payload = {
            "status": self.status,
            "run_id": self.run_id,
            "subject_type": self.subject_type.value,
            "request_snapshot": dict(self.request_snapshot),
            "provenance": dict(self.provenance),
            "summary_metrics": dict(self.summary_metrics),
            "factor": dict(self.factor) if self.factor is not None else None,
            "portfolio": dict(self.portfolio) if self.portfolio is not None else None,
            "replay": dict(self.replay) if self.replay is not None else None,
            "warnings": list(self.warnings),
            "artifacts": dict(self.artifacts),
        }
        return _make_json_safe(payload)

    def to_json(self, indent: int | None = None) -> str:
        """Serialize BackResult to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def summary(self) -> str:
        """Produce human-readable text summary of the run."""
        lines = [
            f"=== Backtest Run Summary [{self.run_id[:8]}] ===",
            f"Status: {self.status}",
            f"Subject Type: {self.subject_type.value}",
            f"Period: {self.request_snapshot.get('period')}",
            f"Config Hash: {self.provenance.get('config_hash', 'N/A')[:16]}...",
        ]
        if self.summary_metrics:
            lines.append("Metrics:")
            for k, v in self.summary_metrics.items():
                lines.append(f"  - {k}: {v}")
        if self.warnings:
            lines.append("Warnings:")
            for w in self.warnings:
                lines.append(f"  ! {w}")
        return "\n".join(lines)
