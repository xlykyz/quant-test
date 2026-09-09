"""Safe, zero-AST partial rule evaluator for Experiment mode (Task08)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TICKER, TRADE_DATE
from .models import ExperimentSpec, FilterPredicate, HarnessValidationError


def _normalize_date_str(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def evaluate_experiment_rules(
    factor_df: pd.DataFrame,
    spec: ExperimentSpec,
    *,
    cash_buffer: float = 0.0,
    trading_days: Sequence[Any] | None = None,
) -> pd.DataFrame:
    """Evaluate partial experiment rules into a canonical target weights DataFrame.

    Parameters
    ----------
    factor_df:
        DataFrame containing trade_date, asset_id (or ticker), plus score/filter fields.
    spec:
        Validated ExperimentSpec defining score, filter, rank, portfolio, and exit.
    cash_buffer:
        Buffer fraction in [0, 1) to withhold as cash.
    trading_days:
        Optional sequence of trading days for alignment.
    """
    if not isinstance(factor_df, pd.DataFrame):
        raise HarnessValidationError("factor_df must be a pandas DataFrame")
    if factor_df.empty:
        return pd.DataFrame(columns=["trade_date", "asset_id", "target_weight", "priority"])

    df = factor_df.copy()

    # Normalize identity columns
    if TRADE_DATE not in df.columns:
        raise HarnessValidationError("factor_df must contain 'trade_date'")
    df[TRADE_DATE] = df[TRADE_DATE].apply(_normalize_date_str)

    if ASSET_ID not in df.columns:
        if TICKER in df.columns:
            df[ASSET_ID] = df[TICKER].astype(str).str.strip()
        else:
            raise HarnessValidationError("factor_df requires 'asset_id' or 'ticker'")
    else:
        df[ASSET_ID] = df[ASSET_ID].astype(str).str.strip()

    # 1. Compute Score
    if isinstance(spec.score, str):
        if spec.score not in df.columns:
            raise HarnessValidationError(f"score column {spec.score!r} missing from factor_df")
        df["_computed_score"] = pd.to_numeric(df[spec.score], errors="coerce")
    elif isinstance(spec.score, Mapping):
        total_score = pd.Series(0.0, index=df.index, dtype=float)
        for col, weight in spec.score.items():
            if col not in df.columns:
                raise HarnessValidationError(f"score component column {col!r} missing from factor_df")
            numeric_col = pd.to_numeric(df[col], errors="coerce")
            total_score += numeric_col * float(weight)
        df["_computed_score"] = total_score
    else:
        raise HarnessValidationError(f"unsupported score spec type: {type(spec.score)}")

    # Drop rows where computed score is NaN
    df = df[df["_computed_score"].notna()].copy()

    # 2. Evaluate Filter
    if spec.filter is not None:
        mask = pd.Series(True, index=df.index)
        predicates: list[FilterPredicate] = []

        if isinstance(spec.filter, str):
            if spec.filter not in df.columns:
                raise HarnessValidationError(f"filter column {spec.filter!r} missing from factor_df")
            mask = mask & df[spec.filter].astype(bool)
        elif isinstance(spec.filter, FilterPredicate):
            predicates.append(spec.filter)
        elif isinstance(spec.filter, Sequence):
            for p in spec.filter:
                if isinstance(p, FilterPredicate):
                    predicates.append(p)
                else:
                    raise HarnessValidationError(f"Invalid filter predicate in sequence: {p!r}")
        else:
            raise HarnessValidationError(f"Invalid filter format: {spec.filter!r}")

        for pred in predicates:
            if pred.field not in df.columns:
                raise HarnessValidationError(f"filter predicate field {pred.field!r} missing from factor_df")
            col = df[pred.field]
            op = pred.op
            val = pred.value
            if op == "eq":
                pred_mask = col == val
            elif op == "ne":
                pred_mask = col != val
            elif op == "gt":
                pred_mask = pd.to_numeric(col, errors="coerce") > float(val)
            elif op == "ge":
                pred_mask = pd.to_numeric(col, errors="coerce") >= float(val)
            elif op == "lt":
                pred_mask = pd.to_numeric(col, errors="coerce") < float(val)
            elif op == "le":
                pred_mask = pd.to_numeric(col, errors="coerce") <= float(val)
            elif op == "in":
                pred_mask = col.isin(val)
            elif op == "not_in":
                pred_mask = ~col.isin(val)
            else:
                raise HarnessValidationError(f"Unsupported filter op {op!r}")
            mask = mask & pred_mask.fillna(False)

        df = df[mask].copy()

    # 3. Rank & Select Portfolio per trade_date
    rank_spec = spec.rank or {"by": "score", "order": "desc"}
    order = str(rank_spec.get("order", "desc")).lower()
    ascending = order == "asc"

    portfolio_spec = spec.portfolio or {"top_n": 6, "weight_each": 0.125}
    top_n = int(portfolio_spec["top_n"])
    weight_each = portfolio_spec.get("weight_each")
    fixed_weight = float(weight_each) if weight_each is not None else None

    # Group by trade_date
    grouped = df.groupby(TRADE_DATE, sort=True)
    all_dates = sorted(df[TRADE_DATE].unique())
    if trading_days:
        normalized_days = [_normalize_date_str(d) for d in trading_days]
        all_dates = sorted(set(all_dates) | {d for d in normalized_days if d in set(df[TRADE_DATE])})

    rows: list[dict[str, Any]] = []
    previously_selected: set[str] = set()

    for date_str in all_dates:
        if date_str in grouped.groups:
            day_df = grouped.get_group(date_str).copy()
            # Sort by computed_score then asset_id for determinism
            day_df = day_df.sort_values(
                by=["_computed_score", ASSET_ID],
                ascending=[ascending, True],
                kind="mergesort",
            )
            selected_df = day_df.head(top_n)
            selected_assets = selected_df[ASSET_ID].tolist()
            selected_scores = selected_df["_computed_score"].tolist()
        else:
            selected_assets = []
            selected_scores = []

        currently_selected = set(selected_assets)
        target_gross = 1.0 - float(cash_buffer)

        # Determine weights
        assigned_weights: dict[str, float] = {}
        if selected_assets:
            if fixed_weight is not None:
                single_weight = min(fixed_weight, target_gross)
                for a in selected_assets:
                    assigned_weights[a] = single_weight
            else:
                equal_w = target_gross / len(selected_assets)
                for a in selected_assets:
                    assigned_weights[a] = equal_w

            # Scale down if gross target exceeded
            total_w = sum(assigned_weights.values())
            if total_w > target_gross + 1e-12:
                scale = target_gross / total_w
                assigned_weights = {a: w * scale for a, w in assigned_weights.items()}

        priority_map = {a: float(s) for a, s in zip(selected_assets, selected_scores)}

        # Combine selected assets and previously held assets that need exit (0.0 weight)
        all_active_assets = sorted(currently_selected | previously_selected)
        for a in all_active_assets:
            w = assigned_weights.get(a, 0.0)
            p = priority_map.get(a, 0.0)
            rows.append({
                "trade_date": date_str,
                "asset_id": a,
                "target_weight": w,
                "priority": p,
            })

        previously_selected = currently_selected

    return pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "target_weight", "priority"],
    )
