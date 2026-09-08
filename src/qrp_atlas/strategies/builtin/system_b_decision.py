"""System B-local holding / entry / exit decision policy.

This module deliberately stays below the registered Product Strategy boundary.
It consumes already-prepared System B facts and emits per-asset
``StrategyDecision`` objects for Task07-C to resolve into a full portfolio target.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import TICKER, TRADE_DATE

from ..models import StrategyAction, StrategyDecision, StrategyHoldingState
from ..validation import StrategyValidationError


COMPARISON_SCORE = "comparison_score"
ENTRY_ELIGIBLE = "entry_eligible"
SYSTEM_B_EXIT_TRIGGERED = "system_b_exit_triggered"
SEVERE_ABNORMAL_SUPERVISION_STATUS = "severe_abnormal_supervision_status"

_SCORE_PROVENANCE_FIELDS = (
    TRADE_DATE,
    "score_calculation_version",
    "rule_version_set_id",
    "parameter_set_id",
    "input_snapshot_id",
)


class SystemBExitStatus(str, Enum):
    TRIGGERED = "TRIGGERED"
    NOT_TRIGGERED = "NOT_TRIGGERED"
    UNAVAILABLE = "UNAVAILABLE"


class SystemBEntryEligibilityStatus(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    INELIGIBLE = "INELIGIBLE"
    UNAVAILABLE = "UNAVAILABLE"


class SystemBAuthorizationStatus(str, Enum):
    AUTHORIZED = "AUTHORIZED"
    DENIED = "DENIED"
    UNAVAILABLE = "UNAVAILABLE"


class SystemBSupervisionStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    UNAVAILABLE = "UNAVAILABLE"


class SystemBScoreProvenanceStatus(str, Enum):
    VALID = "VALID"
    UNAVAILABLE = "UNAVAILABLE"
    MISMATCH = "MISMATCH"


@dataclass(frozen=True)
class SystemBComparisonScoreProvenance:
    trade_date: str | None
    score_calculation_version: str | None
    rule_version_set_id: str | None
    parameter_set_id: str | None
    input_snapshot_id: str | None
    status: SystemBScoreProvenanceStatus

    def to_evidence(self) -> dict[str, str | None]:
        return {
            "score_calculation_version": self.score_calculation_version,
            "rule_version_set_id": self.rule_version_set_id,
            "parameter_set_id": self.parameter_set_id,
            "input_snapshot_id": self.input_snapshot_id,
        }


@dataclass(frozen=True)
class SystemBAssetDecisionFact:
    asset_id: str
    comparison_score: float | None
    exit_status: SystemBExitStatus
    entry_eligibility_status: SystemBEntryEligibilityStatus
    supervision_status: SystemBSupervisionStatus
    manual_handling_required: bool | None = None
    manual_handling_status: str | None = None
    manual_handling_record_id: str | None = None


@dataclass(frozen=True)
class SystemBDecisionInput:
    trade_date: str
    initial_holdings: Mapping[str, StrategyHoldingState]
    candidate_asset_ids: frozenset[str]
    authorization_status: SystemBAuthorizationStatus
    comparison_score_provenance: SystemBComparisonScoreProvenance
    asset_facts: Mapping[str, SystemBAssetDecisionFact]


def normalize_system_b_decision_input(
    prepared_facts: pd.DataFrame,
    *,
    holdings: Mapping[str, StrategyHoldingState],
    candidate_asset_ids: Sequence[str] | set[str] | frozenset[str],
    authorization: bool | np.bool_ | str | SystemBAuthorizationStatus | None,
    comparison_score_provenance: Mapping[str, Any] | SystemBComparisonScoreProvenance | None,
) -> SystemBDecisionInput:
    """Normalize the private Task07-B input envelope without using Common strict NA rules."""

    if not isinstance(prepared_facts, pd.DataFrame):
        raise StrategyValidationError("System B prepared facts must be a pandas DataFrame")
    required = (
        TICKER,
        TRADE_DATE,
        COMPARISON_SCORE,
        ENTRY_ELIGIBLE,
        SYSTEM_B_EXIT_TRIGGERED,
        SEVERE_ABNORMAL_SUPERVISION_STATUS,
    )
    missing_columns = [column for column in required if column not in prepared_facts.columns]
    if missing_columns:
        raise StrategyValidationError(
            f"System B prepared facts missing required columns: {missing_columns}"
        )

    normalized_holdings = _normalize_holdings(holdings)
    normalized_candidates = _normalize_asset_ids(candidate_asset_ids, "candidate_asset_ids")
    expected_domain = set(normalized_holdings) | set(normalized_candidates)

    if prepared_facts.empty:
        if expected_domain:
            raise StrategyValidationError("System B prepared facts do not cover the required asset domain")
        raise StrategyValidationError("System B decision input requires exactly one trade_date")

    result = prepared_facts.copy()
    parsed_dates = pd.to_datetime(result[TRADE_DATE], errors="coerce", format="mixed")
    if parsed_dates.isna().any():
        raise StrategyValidationError("System B prepared facts contain invalid trade_date values")
    result[TRADE_DATE] = parsed_dates.dt.strftime("%Y-%m-%d")
    dates = tuple(sorted(set(result[TRADE_DATE].astype(str))))
    if len(dates) != 1:
        raise StrategyValidationError("System B decision input must contain exactly one trade_date")
    trade_date = dates[0]

    if result[TICKER].isna().any():
        raise StrategyValidationError("System B prepared facts contain missing ticker values")
    result[TICKER] = result[TICKER].astype(str).str.strip()
    if (result[TICKER] == "").any():
        raise StrategyValidationError("System B prepared facts contain blank ticker values")
    duplicate_count = int(result.duplicated(subset=[TICKER, TRADE_DATE], keep=False).sum())
    if duplicate_count:
        raise StrategyValidationError(
            f"System B prepared facts have {duplicate_count} duplicate (ticker, trade_date) rows"
        )

    actual_domain = set(result[TICKER])
    if actual_domain != expected_domain:
        missing_assets = sorted(expected_domain - actual_domain)
        extra_assets = sorted(actual_domain - expected_domain)
        raise StrategyValidationError(
            "System B prepared facts asset domain mismatch: "
            f"missing={missing_assets}, extra={extra_assets}"
        )

    authorization_status = _normalize_authorization(authorization)
    provenance = _normalize_score_provenance(
        trade_date,
        comparison_score_provenance,
        result,
    )

    facts: dict[str, SystemBAssetDecisionFact] = {}
    for row in result.sort_values([TICKER], kind="mergesort").to_dict("records"):
        asset_id = str(row[TICKER])
        facts[asset_id] = SystemBAssetDecisionFact(
            asset_id=asset_id,
            comparison_score=_normalize_score(row.get(COMPARISON_SCORE)),
            exit_status=_normalize_exit_status(row.get(SYSTEM_B_EXIT_TRIGGERED)),
            entry_eligibility_status=_normalize_entry_eligibility(row.get(ENTRY_ELIGIBLE)),
            supervision_status=_normalize_supervision_status(
                row.get(SEVERE_ABNORMAL_SUPERVISION_STATUS)
            ),
            manual_handling_required=_normalize_optional_bool(
                row.get("manual_handling_required")
            ),
            manual_handling_status=_normalize_optional_string(
                row.get("manual_handling_status")
            ),
            manual_handling_record_id=_normalize_optional_string(
                row.get("manual_handling_record_id")
            ),
        )

    return SystemBDecisionInput(
        trade_date=trade_date,
        initial_holdings=MappingProxyType(dict(normalized_holdings)),
        candidate_asset_ids=normalized_candidates,
        authorization_status=authorization_status,
        comparison_score_provenance=provenance,
        asset_facts=MappingProxyType(dict(facts)),
    )


def evaluate_system_b_holding_entry_exit(
    decision_input: SystemBDecisionInput,
    *,
    strategy_code: str,
    strategy_version: str,
) -> tuple[StrategyDecision, ...]:
    """Evaluate deterministic System B HOLD / ENTER / EXIT decisions for one trade date."""

    if not isinstance(decision_input, SystemBDecisionInput):
        raise StrategyValidationError("decision_input must be SystemBDecisionInput")
    if not strategy_code or not strategy_version:
        raise StrategyValidationError("strategy_code and strategy_version must be non-empty")

    initial_ids = set(decision_input.initial_holdings)
    facts = decision_input.asset_facts
    terminal_exit_ids = {
        asset_id
        for asset_id in initial_ids
        if facts[asset_id].exit_status is SystemBExitStatus.TRIGGERED
    }
    retained_ids = initial_ids - terminal_exit_ids

    provenance = decision_input.comparison_score_provenance
    provenance_valid = provenance.status is SystemBScoreProvenanceStatus.VALID

    retained_scores_complete = all(
        facts[asset_id].comparison_score is not None for asset_id in retained_ids
    )
    retained_scores = [
        facts[asset_id].comparison_score
        for asset_id in retained_ids
        if facts[asset_id].comparison_score is not None
    ]

    new_ids = sorted(decision_input.candidate_asset_ids - initial_ids)
    new_gate_reasons = {
        asset_id: _entry_gate_failure_reason(
            decision_input.authorization_status,
            facts[asset_id],
            provenance.status,
        )
        for asset_id in new_ids
    }

    add_base_ids = {
        asset_id
        for asset_id in retained_ids
        if decision_input.initial_holdings[asset_id].entry_count == 1
        and facts[asset_id].exit_status is SystemBExitStatus.NOT_TRIGGERED
        and _entry_gate_failure_reason(
            decision_input.authorization_status,
            facts[asset_id],
            provenance.status,
        )
        is None
    }
    score_ready_new_ids = {
        asset_id for asset_id in new_ids if new_gate_reasons[asset_id] is None
    }
    top_universe = sorted(score_ready_new_ids | add_base_ids)
    top_score = (
        max(float(facts[asset_id].comparison_score) for asset_id in top_universe)
        if top_universe
        else None
    )

    decisions: list[StrategyDecision] = []
    for asset_id in sorted(facts):
        fact = facts[asset_id]
        holding = decision_input.initial_holdings.get(asset_id)
        was_initially_held = asset_id in initial_ids
        was_retained = asset_id in retained_ids
        same_day_exit_terminal = asset_id in terminal_exit_ids
        entry_kind = "NONE"
        threshold: float | None = None
        threshold_passed: bool | None = None

        if same_day_exit_terminal:
            action = StrategyAction.EXIT
            reason_code = "MA5_TWO_ACTUAL_TRADING_DAYS_EXIT"
        elif was_initially_held and fact.exit_status is SystemBExitStatus.UNAVAILABLE:
            action = StrategyAction.NO_ACTION
            reason_code = "EXIT_STATUS_UNAVAILABLE"
        elif was_initially_held:
            assert holding is not None
            if holding.entry_count >= 2:
                action = StrategyAction.HOLD
                reason_code = "ADD_ENTRY_LIMIT_REACHED"
            else:
                add_gate_reason = _entry_gate_failure_reason(
                    decision_input.authorization_status,
                    fact,
                    provenance.status,
                )
                if add_gate_reason is not None:
                    action = StrategyAction.HOLD
                    reason_code = add_gate_reason
                elif top_score is not None and fact.comparison_score == top_score:
                    action = StrategyAction.ENTER
                    reason_code = "ADD_ENTRY_ELIGIBLE_TOP_SCORE"
                    entry_kind = "ADD"
                else:
                    action = StrategyAction.HOLD
                    reason_code = "ADD_ENTRY_NOT_TOP_SCORE"
        else:
            gate_reason = new_gate_reasons[asset_id]
            if gate_reason is not None:
                action = StrategyAction.NO_ACTION
                reason_code = gate_reason
            else:
                retained_count = len(retained_ids)
                if retained_count >= 6:
                    action = StrategyAction.NO_ACTION
                    reason_code = "DISTINCT_HOLDING_CAP_REACHED"
                elif retained_count == 0:
                    action = StrategyAction.ENTER
                    reason_code = "NEW_ENTRY_ELIGIBLE_NO_HOLDING_THRESHOLD"
                    entry_kind = "NEW"
                    threshold_passed = True
                elif not provenance_valid:
                    action = StrategyAction.NO_ACTION
                    reason_code = _provenance_reason(provenance.status)
                elif not retained_scores_complete:
                    action = StrategyAction.NO_ACTION
                    reason_code = "RETAINED_HOLDING_SCORE_UNAVAILABLE"
                elif retained_count <= 3:
                    threshold = float(min(retained_scores))
                    threshold_passed = bool(fact.comparison_score > threshold)
                    if threshold_passed:
                        action = StrategyAction.ENTER
                        reason_code = "NEW_ENTRY_ABOVE_MIN_HOLDING_SCORE"
                        entry_kind = "NEW"
                    else:
                        action = StrategyAction.NO_ACTION
                        reason_code = "NEW_ENTRY_SCORE_THRESHOLD_NOT_MET"
                else:
                    threshold = float(max(retained_scores))
                    threshold_passed = bool(fact.comparison_score > threshold)
                    if threshold_passed:
                        action = StrategyAction.ENTER
                        reason_code = "NEW_ENTRY_ABOVE_ALL_HOLDING_SCORES"
                        entry_kind = "NEW"
                    else:
                        action = StrategyAction.NO_ACTION
                        reason_code = "NEW_ENTRY_SCORE_THRESHOLD_NOT_MET"

        evidence: dict[str, Any] = {
            "was_initially_held": was_initially_held,
            "was_retained": was_retained,
            "entry_count": holding.entry_count if holding is not None else 0,
            "exit_status": fact.exit_status.value,
            "entry_eligibility_status": fact.entry_eligibility_status.value,
            "authorization_status": decision_input.authorization_status.value,
            "supervision_status": fact.supervision_status.value,
            "comparison_score": fact.comparison_score,
            "comparison_score_provenance_valid": provenance_valid,
            **provenance.to_evidence(),
            "retained_holding_count": len(retained_ids),
            "relative_score_threshold": threshold,
            "relative_score_passed": threshold_passed,
            "entry_kind": entry_kind,
            "same_day_exit_terminal": same_day_exit_terminal,
        }
        if fact.manual_handling_required is not None:
            evidence["manual_handling_required"] = fact.manual_handling_required
        if fact.manual_handling_status is not None:
            evidence["manual_handling_status"] = fact.manual_handling_status
        if fact.manual_handling_record_id is not None:
            evidence["manual_handling_record_id"] = fact.manual_handling_record_id

        decisions.append(
            StrategyDecision(
                trade_date=decision_input.trade_date,
                asset_id=asset_id,
                action=action,
                direction="long",
                strategy_code=strategy_code,
                strategy_version=strategy_version,
                reason_code=reason_code,
                score=fact.comparison_score,
                weight=None,
                evidence=evidence,
            )
        )

    return tuple(decisions)


def _normalize_holdings(
    holdings: Mapping[str, StrategyHoldingState],
) -> dict[str, StrategyHoldingState]:
    if not isinstance(holdings, Mapping):
        raise StrategyValidationError("System B holdings must be a mapping")
    normalized: dict[str, StrategyHoldingState] = {}
    for asset_id, state in holdings.items():
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise StrategyValidationError("System B holding asset ids must be non-empty strings")
        if not isinstance(state, StrategyHoldingState):
            raise StrategyValidationError("System B holdings must contain StrategyHoldingState values")
        if state.asset_id != asset_id:
            raise StrategyValidationError("System B holding key must equal state.asset_id")
        if (
            isinstance(state.current_weight, bool)
            or not isinstance(state.current_weight, (int, float))
            or not math.isfinite(float(state.current_weight))
            or float(state.current_weight) <= 0
        ):
            raise StrategyValidationError(
                "System B holding current_weight must be a positive finite number"
            )
        if (
            isinstance(state.entry_count, bool)
            or not isinstance(state.entry_count, int)
            or state.entry_count < 1
        ):
            raise StrategyValidationError(
                "System B holding entry_count must be an integer >= 1"
            )
        first_entry = _normalize_holding_date(state.first_entry_date, "first_entry_date")
        last_entry = _normalize_holding_date(state.last_entry_date, "last_entry_date")
        if first_entry is not None and last_entry is not None and first_entry > last_entry:
            raise StrategyValidationError(
                "System B holding first_entry_date must be on or before last_entry_date"
            )
        normalized[asset_id] = state
    return normalized


def _normalize_holding_date(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 10:
        raise StrategyValidationError(
            f"System B holding {field_name} must be an exact YYYY-MM-DD string"
        )
    parsed = pd.to_datetime(value, errors="coerce", format="%Y-%m-%d")
    if pd.isna(parsed) or parsed.strftime("%Y-%m-%d") != value:
        raise StrategyValidationError(
            f"System B holding {field_name} must be an exact YYYY-MM-DD string"
        )
    return value


def _normalize_asset_ids(
    asset_ids: Sequence[str] | set[str] | frozenset[str],
    field_name: str,
) -> frozenset[str]:
    if isinstance(asset_ids, str) or not isinstance(asset_ids, (Sequence, set, frozenset)):
        raise StrategyValidationError(f"{field_name} must be a collection of asset ids")
    normalized: set[str] = set()
    for asset_id in asset_ids:
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise StrategyValidationError(f"{field_name} must contain non-empty strings")
        normalized.add(asset_id.strip())
    return frozenset(normalized)


def _normalize_authorization(
    value: bool | np.bool_ | str | SystemBAuthorizationStatus | None,
) -> SystemBAuthorizationStatus:
    if value is None or _is_missing(value):
        return SystemBAuthorizationStatus.UNAVAILABLE
    if isinstance(value, (bool, np.bool_)):
        return SystemBAuthorizationStatus.AUTHORIZED if bool(value) else SystemBAuthorizationStatus.DENIED
    if isinstance(value, SystemBAuthorizationStatus):
        return value
    if isinstance(value, str):
        try:
            return SystemBAuthorizationStatus(value.strip().upper())
        except ValueError as exc:
            raise StrategyValidationError(f"invalid System B authorization status: {value!r}") from exc
    raise StrategyValidationError(f"invalid System B authorization value: {value!r}")


def _normalize_exit_status(value: Any) -> SystemBExitStatus:
    if _is_missing(value):
        return SystemBExitStatus.UNAVAILABLE
    if isinstance(value, (bool, np.bool_)):
        return SystemBExitStatus.TRIGGERED if bool(value) else SystemBExitStatus.NOT_TRIGGERED
    if isinstance(value, SystemBExitStatus):
        return value
    if isinstance(value, str):
        try:
            return SystemBExitStatus(value.strip().upper())
        except ValueError as exc:
            raise StrategyValidationError(f"invalid System B exit status: {value!r}") from exc
    raise StrategyValidationError(f"invalid System B exit status: {value!r}")


def _normalize_entry_eligibility(value: Any) -> SystemBEntryEligibilityStatus:
    if _is_missing(value):
        return SystemBEntryEligibilityStatus.UNAVAILABLE
    if isinstance(value, (bool, np.bool_)):
        return (
            SystemBEntryEligibilityStatus.ELIGIBLE
            if bool(value)
            else SystemBEntryEligibilityStatus.INELIGIBLE
        )
    if isinstance(value, SystemBEntryEligibilityStatus):
        return value
    if isinstance(value, str):
        try:
            return SystemBEntryEligibilityStatus(value.strip().upper())
        except ValueError as exc:
            raise StrategyValidationError(f"invalid System B eligibility status: {value!r}") from exc
    raise StrategyValidationError(f"invalid System B eligibility status: {value!r}")


def _normalize_supervision_status(value: Any) -> SystemBSupervisionStatus:
    if _is_missing(value):
        return SystemBSupervisionStatus.UNAVAILABLE
    if isinstance(value, SystemBSupervisionStatus):
        return value
    if isinstance(value, str):
        try:
            return SystemBSupervisionStatus(value.strip().upper())
        except ValueError as exc:
            raise StrategyValidationError(f"invalid System B supervision status: {value!r}") from exc
    raise StrategyValidationError(f"invalid System B supervision status: {value!r}")


def _normalize_score(value: Any) -> float | None:
    if _is_missing(value):
        return None
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.integer, np.floating)):
        raise StrategyValidationError(f"comparison_score must be numeric or unavailable, got {value!r}")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise StrategyValidationError("comparison_score must be finite or unavailable")
    return numeric


def _normalize_score_provenance(
    trade_date: str,
    raw: Mapping[str, Any] | SystemBComparisonScoreProvenance | None,
    prepared: pd.DataFrame,
) -> SystemBComparisonScoreProvenance:
    if isinstance(raw, SystemBComparisonScoreProvenance):
        required_values = (
            raw.trade_date,
            raw.score_calculation_version,
            raw.rule_version_set_id,
            raw.parameter_set_id,
            raw.input_snapshot_id,
        )
        if raw.status is SystemBScoreProvenanceStatus.VALID and any(
            value is None or (isinstance(value, str) and not value.strip())
            for value in required_values
        ):
            base = SystemBComparisonScoreProvenance(
                trade_date=raw.trade_date,
                score_calculation_version=raw.score_calculation_version,
                rule_version_set_id=raw.rule_version_set_id,
                parameter_set_id=raw.parameter_set_id,
                input_snapshot_id=raw.input_snapshot_id,
                status=SystemBScoreProvenanceStatus.UNAVAILABLE,
            )
        else:
            base = raw
    elif raw is None:
        base = SystemBComparisonScoreProvenance(
            trade_date=None,
            score_calculation_version=None,
            rule_version_set_id=None,
            parameter_set_id=None,
            input_snapshot_id=None,
            status=SystemBScoreProvenanceStatus.UNAVAILABLE,
        )
    elif isinstance(raw, Mapping):
        values = {field: _normalize_optional_string(raw.get(field)) for field in _SCORE_PROVENANCE_FIELDS}
        raw_trade_date = values[TRADE_DATE]
        if raw_trade_date is not None:
            parsed = pd.to_datetime(raw_trade_date, errors="coerce", format="mixed")
            if pd.isna(parsed):
                raise StrategyValidationError("comparison-score provenance has invalid trade_date")
            raw_trade_date = parsed.strftime("%Y-%m-%d")
        missing = [field for field in _SCORE_PROVENANCE_FIELDS if values[field] is None]
        status = (
            SystemBScoreProvenanceStatus.UNAVAILABLE
            if missing
            else SystemBScoreProvenanceStatus.VALID
        )
        if status is SystemBScoreProvenanceStatus.VALID and raw_trade_date != trade_date:
            status = SystemBScoreProvenanceStatus.MISMATCH
        base = SystemBComparisonScoreProvenance(
            trade_date=raw_trade_date,
            score_calculation_version=values["score_calculation_version"],
            rule_version_set_id=values["rule_version_set_id"],
            parameter_set_id=values["parameter_set_id"],
            input_snapshot_id=values["input_snapshot_id"],
            status=status,
        )
    else:
        raise StrategyValidationError("comparison_score_provenance must be a mapping or provenance object")

    if base.status is SystemBScoreProvenanceStatus.VALID and base.trade_date != trade_date:
        base = SystemBComparisonScoreProvenance(
            trade_date=base.trade_date,
            score_calculation_version=base.score_calculation_version,
            rule_version_set_id=base.rule_version_set_id,
            parameter_set_id=base.parameter_set_id,
            input_snapshot_id=base.input_snapshot_id,
            status=SystemBScoreProvenanceStatus.MISMATCH,
        )

    row_fields = [field for field in _SCORE_PROVENANCE_FIELDS[1:] if field in prepared.columns]
    if base.status is SystemBScoreProvenanceStatus.VALID and row_fields:
        expected = {
            "score_calculation_version": base.score_calculation_version,
            "rule_version_set_id": base.rule_version_set_id,
            "parameter_set_id": base.parameter_set_id,
            "input_snapshot_id": base.input_snapshot_id,
        }
        mismatch = False
        for field in row_fields:
            values = prepared[field].map(_normalize_optional_string)
            if values.isna().any() or any(value != expected[field] for value in values):
                mismatch = True
                break
        if mismatch:
            base = SystemBComparisonScoreProvenance(
                trade_date=base.trade_date,
                score_calculation_version=base.score_calculation_version,
                rule_version_set_id=base.rule_version_set_id,
                parameter_set_id=base.parameter_set_id,
                input_snapshot_id=base.input_snapshot_id,
                status=SystemBScoreProvenanceStatus.MISMATCH,
            )
    return base


def _entry_gate_failure_reason(
    authorization_status: SystemBAuthorizationStatus,
    fact: SystemBAssetDecisionFact,
    provenance_status: SystemBScoreProvenanceStatus,
) -> str | None:
    if authorization_status is SystemBAuthorizationStatus.DENIED:
        return "NEW_POSITION_AUTHORIZATION_DENIED"
    if authorization_status is SystemBAuthorizationStatus.UNAVAILABLE:
        return "NEW_POSITION_AUTHORIZATION_UNAVAILABLE"
    if fact.entry_eligibility_status is SystemBEntryEligibilityStatus.INELIGIBLE:
        return "ENTRY_ELIGIBILITY_DENIED"
    if fact.entry_eligibility_status is SystemBEntryEligibilityStatus.UNAVAILABLE:
        return "ENTRY_ELIGIBILITY_UNAVAILABLE"
    if fact.supervision_status is SystemBSupervisionStatus.ACTIVE:
        return "SEVERE_ABNORMAL_SUPERVISION_BLOCKED"
    if fact.supervision_status is SystemBSupervisionStatus.UNAVAILABLE:
        return "SEVERE_ABNORMAL_STATUS_UNAVAILABLE"
    if fact.comparison_score is None:
        return "COMPARISON_SCORE_UNAVAILABLE"
    if provenance_status is not SystemBScoreProvenanceStatus.VALID:
        return _provenance_reason(provenance_status)
    return None


def _provenance_reason(status: SystemBScoreProvenanceStatus) -> str:
    if status is SystemBScoreProvenanceStatus.MISMATCH:
        return "COMPARISON_SCORE_PROVENANCE_MISMATCH"
    return "COMPARISON_SCORE_PROVENANCE_UNAVAILABLE"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(missing) if isinstance(missing, (bool, np.bool_)) else False


def _normalize_optional_bool(value: Any) -> bool | None:
    if _is_missing(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    raise StrategyValidationError(f"optional boolean field has invalid value: {value!r}")


def _normalize_optional_string(value: Any) -> str | None:
    if _is_missing(value):
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized = value.strip()
    return normalized or None
