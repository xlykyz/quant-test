"""System B portfolio constraint resolution and final target strategy.

This module implements Task07-C: resolving asset-level Holding / Entry / Exit
decisions into a complete, deterministic, full-snapshot native portfolio target.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import TICKER, TRADE_DATE

from ..models import (
    StrategyAction,
    StrategyDecision,
    StrategyDefinition,
    StrategyHoldingState,
    StrategyInput,
    StrategyPortfolioTarget,
    StrategyPortfolioTargetPosition,
    StrategyRunResult,
    StrategyType,
)
from ..validation import (
    StrategyValidationError,
    resolve_parameters,
    validate_definition,
)
from .system_b_decision import (
    COMPARISON_SCORE,
    ENTRY_ELIGIBLE,
    SEVERE_ABNORMAL_SUPERVISION_STATUS,
    SYSTEM_B_EXIT_TRIGGERED,
    _normalize_holdings,
    evaluate_system_b_holding_entry_exit,
    normalize_system_b_decision_input,
)


PORTFOLIO_WEIGHT_TOLERANCE: float = 1e-12
ENTRY_INCREMENT: float = 0.125
MAX_DISTINCT_HOLDINGS: int = 6
SINGLE_ASSET_CAP: float = 0.30

# Diagnostic reason codes
ADD_SINGLE_ASSET_CAP_EXCEEDED = "ADD_SINGLE_ASSET_CAP_EXCEEDED"
NEW_DISTINCT_CAPACITY_INSUFFICIENT = "NEW_DISTINCT_CAPACITY_INSUFFICIENT"
NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED = "NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED"
PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT = "PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT"
ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED = "ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED"

# Target position reason codes
POSITION_PRESERVED = "POSITION_PRESERVED"
NEW_ENTRY_SELECTED = "NEW_ENTRY_SELECTED"
ADD_ENTRY_SELECTED = "ADD_ENTRY_SELECTED"

_PROVENANCE_KEYS = (
    "score_calculation_version",
    "rule_version_set_id",
    "parameter_set_id",
    "input_snapshot_id",
)


def resolve_system_b_portfolio_target(
    *,
    trade_date: str,
    holdings: Mapping[str, StrategyHoldingState],
    decisions: Sequence[StrategyDecision],
    strategy_code: str,
    strategy_version: str,
) -> StrategyPortfolioTarget:
    """Resolve asset decisions into a deterministic full-snapshot portfolio target."""

    # 1. Validate scalar inputs
    if not isinstance(trade_date, str) or len(trade_date) != 10:
        raise StrategyValidationError(f"invalid trade_date: {trade_date!r}")
    parsed_date = pd.to_datetime(trade_date, errors="coerce", format="%Y-%m-%d")
    if pd.isna(parsed_date) or parsed_date.strftime("%Y-%m-%d") != trade_date:
        raise StrategyValidationError(f"invalid trade_date: {trade_date!r}")

    if not isinstance(strategy_code, str) or not strategy_code.strip():
        raise StrategyValidationError("strategy_code must be a non-empty string")
    if not isinstance(strategy_version, str) or not strategy_version.strip():
        raise StrategyValidationError("strategy_version must be a non-empty string")

    # 2. Validate and normalize holdings
    normalized_holdings = _normalize_holdings(holdings)
    initial_gross = sum(state.current_weight for state in normalized_holdings.values())
    if initial_gross > 1.0 + PORTFOLIO_WEIGHT_TOLERANCE:
        raise StrategyValidationError(
            f"initial holdings gross weight {initial_gross} exceeds 1.0"
        )

    # 3. Validate decisions sequence and domain
    if isinstance(decisions, (str, bytes)) or not isinstance(decisions, Sequence):
        raise StrategyValidationError("decisions must be a sequence of StrategyDecision instances")

    decision_map: dict[str, StrategyDecision] = {}
    for decision in decisions:
        if not isinstance(decision, StrategyDecision):
            raise StrategyValidationError("decisions must contain StrategyDecision instances")
        if decision.trade_date != trade_date:
            raise StrategyValidationError(
                f"decision trade_date {decision.trade_date!r} does not match {trade_date!r}"
            )
        if decision.strategy_code != strategy_code or decision.strategy_version != strategy_version:
            raise StrategyValidationError(
                f"decision strategy identity does not match resolver ({strategy_code}@{strategy_version})"
            )
        if decision.asset_id in decision_map:
            raise StrategyValidationError(f"duplicate decision for asset_id: {decision.asset_id}")
        decision_map[decision.asset_id] = decision

    for asset_id in normalized_holdings:
        if asset_id not in decision_map:
            raise StrategyValidationError(f"decisions missing initial holding asset: {asset_id}")

    # 4. Validate ENTER structural invariants and finite scores
    for decision in decisions:
        if decision.action is StrategyAction.ENTER:
            if (
                decision.score is None
                or isinstance(decision.score, (bool, np.bool_))
                or not isinstance(decision.score, (int, float, np.integer, np.floating))
                or not math.isfinite(float(decision.score))
            ):
                raise StrategyValidationError(
                    f"ENTER decision for {decision.asset_id} has non-finite score: {decision.score!r}"
                )

            evidence = decision.evidence if isinstance(decision.evidence, Mapping) else {}
            entry_kind = evidence.get("entry_kind")
            if entry_kind not in ("NEW", "ADD"):
                raise StrategyValidationError(
                    f"ENTER decision for {decision.asset_id} has invalid entry_kind: {entry_kind!r}"
                )

            if entry_kind == "NEW":
                if decision.asset_id in normalized_holdings:
                    raise StrategyValidationError(
                        f"NEW entry asset {decision.asset_id} is in initial holdings"
                    )
            elif entry_kind == "ADD":
                if decision.asset_id not in normalized_holdings:
                    raise StrategyValidationError(
                        f"ADD entry asset {decision.asset_id} is not in initial holdings"
                    )
                holding = normalized_holdings[decision.asset_id]
                if holding.entry_count != 1:
                    raise StrategyValidationError(
                        f"ADD entry asset {decision.asset_id} has invalid entry_count {holding.entry_count}, expected 1"
                    )

    # 5. Identify same-day EXIT terminal assets
    terminal_exit_ids: set[str] = set()
    for asset_id in normalized_holdings:
        if decision_map[asset_id].action is StrategyAction.EXIT:
            terminal_exit_ids.add(asset_id)

    # Confirm no same-day EXIT asset has ENTER decision
    for decision in decisions:
        if decision.action is StrategyAction.ENTER and decision.asset_id in terminal_exit_ids:
            raise StrategyValidationError(
                f"same-day EXIT asset {decision.asset_id} cannot have ENTER decision"
            )

    retained_ids = set(normalized_holdings) - terminal_exit_ids

    # 6. Validate retained count and base gross
    retained_count = len(retained_ids)
    if retained_count > MAX_DISTINCT_HOLDINGS:
        raise StrategyValidationError(
            f"retained holdings count {retained_count} exceeds {MAX_DISTINCT_HOLDINGS}"
        )

    base_weights = {asset_id: normalized_holdings[asset_id].current_weight for asset_id in retained_ids}
    base_gross = sum(base_weights.values())
    if base_gross > 1.0 + PORTFOLIO_WEIGHT_TOLERANCE:
        raise StrategyValidationError(
            f"retained holdings base gross weight {base_gross} exceeds 1.0"
        )

    # 7. Extract ENTER NEW / ADD intents and check single-asset ADD cap
    diagnostics: list[str] = []
    feasible_add_candidates: list[StrategyDecision] = []
    new_candidates: list[StrategyDecision] = []

    for decision in decisions:
        if decision.action is StrategyAction.ENTER:
            entry_kind = decision.evidence.get("entry_kind")
            if entry_kind == "NEW":
                new_candidates.append(decision)
            elif entry_kind == "ADD":
                current_weight = normalized_holdings[decision.asset_id].current_weight
                if current_weight + ENTRY_INCREMENT <= SINGLE_ASSET_CAP + PORTFOLIO_WEIGHT_TOLERANCE:
                    feasible_add_candidates.append(decision)
                else:
                    diagnostics.append(
                        f"SYSTEM_B_TARGET_REJECTION|asset_id={decision.asset_id}|reason={ADD_SINGLE_ASSET_CAP_EXCEEDED}"
                    )

    # 8. Resolve NEW distinct slots by exact-score groups
    available_new_slots = MAX_DISTINCT_HOLDINGS - retained_count
    slot_admitted_new: list[StrategyDecision] = []

    unique_new_scores = sorted({float(d.score) for d in new_candidates}, reverse=True)
    remaining_new_slots = available_new_slots
    new_tie_occurred = False

    for score in unique_new_scores:
        group = [d for d in new_candidates if float(d.score) == score]
        group_size = len(group)

        if new_tie_occurred:
            # Cutoff tie occurred at a higher score. Lower score NEW cannot leapfrog!
            for d in group:
                diagnostics.append(
                    f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED}"
                )
            continue

        if remaining_new_slots == 0:
            # Case A: Ordinary capacity exhausted
            for d in group:
                diagnostics.append(
                    f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={NEW_DISTINCT_CAPACITY_INSUFFICIENT}"
                )
            continue

        if group_size <= remaining_new_slots:
            # Case B: Whole group can be accommodated
            slot_admitted_new.extend(group)
            remaining_new_slots -= group_size
        else:
            # Case C: 0 < remaining_new_slots < group_size (true cutoff tie)
            new_tie_occurred = True
            for d in group:
                diagnostics.append(
                    f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED}"
                )

    # 9. Resolve shared desired gross-weight budget by exact-score groups
    enter_pool = slot_admitted_new + feasible_add_candidates
    selected_enter: list[StrategyDecision] = []

    unique_gross_scores = sorted({float(d.score) for d in enter_pool}, reverse=True)
    current_gross = base_gross
    gross_budget_exhausted = False
    gross_tie_occurred = False

    for score in unique_gross_scores:
        group = [d for d in enter_pool if float(d.score) == score]
        group_size = len(group)
        needed_weight = group_size * ENTRY_INCREMENT
        remaining_budget = 1.0 - current_gross

        if gross_tie_occurred:
            # Higher score tied. Lower score cannot leapfrog!
            for d in group:
                diagnostics.append(
                    f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED}"
                )
            continue

        if gross_budget_exhausted:
            # Budget exhausted
            for d in group:
                diagnostics.append(
                    f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT}"
                )
            continue

        if needed_weight <= remaining_budget + PORTFOLIO_WEIGHT_TOLERANCE:
            selected_enter.extend(group)
            current_gross += needed_weight
            if current_gross >= 1.0 - PORTFOLIO_WEIGHT_TOLERANCE:
                gross_budget_exhausted = True
        else:
            if remaining_budget + PORTFOLIO_WEIGHT_TOLERANCE < ENTRY_INCREMENT:
                # Not even one fixed increment can fit
                gross_budget_exhausted = True
                for d in group:
                    diagnostics.append(
                        f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT}"
                    )
            else:
                # Can partially fit, but group_size > 1 -> true cutoff tie
                gross_tie_occurred = True
                for d in group:
                    diagnostics.append(
                        f"SYSTEM_B_TARGET_REJECTION|asset_id={d.asset_id}|reason={ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED}"
                    )

    # 10. Build final target positions
    selected_add_ids = {
        d.asset_id for d in selected_enter if d.evidence.get("entry_kind") == "ADD"
    }
    selected_new = [
        d for d in selected_enter if d.evidence.get("entry_kind") == "NEW"
    ]

    positions: list[StrategyPortfolioTargetPosition] = []

    # 10.1 Retained positions
    for asset_id in retained_ids:
        holding = normalized_holdings[asset_id]
        decision = decision_map[asset_id]
        prior_weight = holding.current_weight
        if asset_id in selected_add_ids:
            target_weight = prior_weight + ENTRY_INCREMENT
            reason_code = ADD_ENTRY_SELECTED
            entry_increment = ENTRY_INCREMENT
            entry_count_after = 2
        else:
            target_weight = prior_weight
            reason_code = POSITION_PRESERVED
            entry_increment = 0.0
            entry_count_after = holding.entry_count

        evidence: dict[str, Any] = {
            "source_decision_action": decision.action.value,
            "source_decision_reason_code": decision.reason_code,
            "source_entry_kind": (
                decision.evidence.get("entry_kind", "NONE")
                if isinstance(decision.evidence, Mapping)
                else "NONE"
            ),
            "comparison_score": decision.score,
            "prior_weight": prior_weight,
            "entry_increment": entry_increment,
            "final_target_weight": target_weight,
            "entry_count_before": holding.entry_count,
            "entry_count_after_if_selected": entry_count_after,
            "was_initially_held": True,
            "was_retained": True,
        }
        for key in _PROVENANCE_KEYS:
            evidence[key] = (
                decision.evidence.get(key)
                if isinstance(decision.evidence, Mapping)
                else None
            )

        positions.append(
            StrategyPortfolioTargetPosition(
                asset_id=asset_id,
                target_weight=target_weight,
                reason_code=reason_code,
                evidence=evidence,
            )
        )

    # 10.2 Selected NEW positions
    for decision in selected_new:
        target_weight = ENTRY_INCREMENT
        reason_code = NEW_ENTRY_SELECTED
        evidence = {
            "source_decision_action": decision.action.value,
            "source_decision_reason_code": decision.reason_code,
            "source_entry_kind": "NEW",
            "comparison_score": decision.score,
            "prior_weight": 0.0,
            "entry_increment": ENTRY_INCREMENT,
            "final_target_weight": target_weight,
            "entry_count_before": 0,
            "entry_count_after_if_selected": 1,
            "was_initially_held": False,
            "was_retained": False,
        }
        for key in _PROVENANCE_KEYS:
            evidence[key] = (
                decision.evidence.get(key)
                if isinstance(decision.evidence, Mapping)
                else None
            )

        positions.append(
            StrategyPortfolioTargetPosition(
                asset_id=decision.asset_id,
                target_weight=target_weight,
                reason_code=reason_code,
                evidence=evidence,
            )
        )

    sorted_positions = tuple(sorted(positions, key=lambda p: p.asset_id))

    # 11. Final invariants check
    if len(sorted_positions) > MAX_DISTINCT_HOLDINGS:
        raise StrategyValidationError(
            f"final distinct positions {len(sorted_positions)} exceeds {MAX_DISTINCT_HOLDINGS}"
        )
    total_target_weight = sum(p.target_weight for p in sorted_positions)
    if total_target_weight > 1.0 + PORTFOLIO_WEIGHT_TOLERANCE:
        raise StrategyValidationError(
            f"final target gross weight {total_target_weight} exceeds 1.0"
        )
    for p in sorted_positions:
        if p.reason_code == ADD_ENTRY_SELECTED and p.target_weight > SINGLE_ASSET_CAP + PORTFOLIO_WEIGHT_TOLERANCE:
            raise StrategyValidationError(
                f"ADD target position {p.asset_id} weight {p.target_weight} exceeds {SINGLE_ASSET_CAP}"
            )

    sorted_diagnostics = tuple(sorted(diagnostics))

    return StrategyPortfolioTarget(
        trade_date=trade_date,
        strategy_code=strategy_code,
        strategy_version=strategy_version,
        positions=sorted_positions,
        diagnostics=sorted_diagnostics,
    )


class SystemBPortfolioStrategy:
    """Deterministic System B portfolio strategy resolving decisions into native full-snapshot targets."""

    definition = StrategyDefinition(
        code="system_b_portfolio",
        name="System B Portfolio",
        version="1.0.0",
        description="Deterministic System B portfolio strategy resolving decisions into native full-snapshot targets.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(
            TICKER,
            TRADE_DATE,
        ),
        required_indicators=(),
    )

    def __init__(self) -> None:
        validate_definition(self.definition)

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        if not isinstance(strategy_input, StrategyInput):
            raise StrategyValidationError("strategy_input must be a StrategyInput instance")
        if not isinstance(strategy_input.prepared_data, pd.DataFrame):
            raise StrategyValidationError("prepared_data must be a pandas DataFrame")

        raw_params = dict(strategy_input.parameters)
        authorization = strategy_input.runtime_context.get(
            "authorization", raw_params.pop("authorization", None)
        )
        provenance = strategy_input.runtime_context.get(
            "comparison_score_provenance", raw_params.pop("comparison_score_provenance", None)
        )
        candidate_ids = strategy_input.runtime_context.get(
            "candidate_asset_ids", raw_params.pop("candidate_asset_ids", None)
        )
        parameters = resolve_parameters(self.definition, raw_params)

        if candidate_ids is None:
            if TICKER in strategy_input.prepared_data.columns:
                all_tickers = {
                    str(t).strip()
                    for t in strategy_input.prepared_data[TICKER].dropna()
                    if str(t).strip()
                }
                candidate_ids = frozenset(all_tickers - set(strategy_input.holdings))
            else:
                candidate_ids = frozenset()

        decision_input = normalize_system_b_decision_input(
            strategy_input.prepared_data,
            holdings=strategy_input.holdings,
            candidate_asset_ids=candidate_ids,
            authorization=authorization,
            comparison_score_provenance=provenance,
        )

        decisions = evaluate_system_b_holding_entry_exit(
            decision_input,
            strategy_code=self.definition.code,
            strategy_version=self.definition.version,
        )

        target = resolve_system_b_portfolio_target(
            trade_date=decision_input.trade_date,
            holdings=decision_input.initial_holdings,
            decisions=decisions,
            strategy_code=self.definition.code,
            strategy_version=self.definition.version,
        )

        return StrategyRunResult(
            definition=self.definition,
            parameters=parameters,
            decisions=decisions,
            portfolio_targets=(target,),
            diagnostics=target.diagnostics,
        )
