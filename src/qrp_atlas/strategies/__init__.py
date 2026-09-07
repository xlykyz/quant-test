"""QRP strategy definitions, validation, registry, and built-in implementations."""

from .models import (
    ParameterSpec,
    StrategyAction,
    StrategyAuthorization,
    StrategyDecision,
    StrategyDefinition,
    StrategyInput,
    StrategyHoldingState,
    StrategyInputScope,
    StrategyPortfolioTarget,
    StrategyPortfolioTargetPosition,
    StrategyRunResult,
    StrategyType,
)
from .protocol import StrategyProtocol
from .registry import (
    DEFAULT_REGISTRY,
    DuplicateStrategyError,
    StrategyNotFoundError,
    StrategyRegistry,
    get_strategy,
    list_strategies,
    run_strategy,
)
from .validation import (
    StrategyValidationError,
    run_strategy_checked,
    validate_and_normalize_strategy_input,
    validate_event_strategy_input,
    validate_strategy_result,
)
from .selection import (
    EligibilityError,
    RebalanceScheduleError,
    SelectionError,
    WeightConstructionError,
    apply_eligibility,
    build_rebalance_schedule,
    equal_weight_targets,
    select_top_n,
    selection_to_target_weights,
)
from .builtin.cross_section import compute_composite_score
from .builtin.system_b_authorization import SystemBAuthorizationStrategy


__all__ = [
    "DEFAULT_REGISTRY",
    "DuplicateStrategyError",
    "ParameterSpec",
    "StrategyAction",
    "StrategyAuthorization",
    "StrategyDecision",
    "StrategyDefinition",
    "StrategyInput",
    "StrategyHoldingState",
    "StrategyInputScope",
    "StrategyNotFoundError",
    "StrategyProtocol",
    "StrategyRegistry",
    "StrategyRunResult",
    "StrategyPortfolioTarget",
    "StrategyPortfolioTargetPosition",
    "StrategyType",
    "StrategyValidationError",
    "run_strategy_checked",
    "validate_and_normalize_strategy_input",
    "validate_event_strategy_input",
    "validate_strategy_result",
    "SystemBAuthorizationStrategy",
    "get_strategy",
    "list_strategies",
    "run_strategy",
    "EligibilityError",
    "RebalanceScheduleError",
    "SelectionError",
    "WeightConstructionError",
    "apply_eligibility",
    "build_rebalance_schedule",
    "compute_composite_score",
    "equal_weight_targets",
    "select_top_n",
    "selection_to_target_weights",
]
