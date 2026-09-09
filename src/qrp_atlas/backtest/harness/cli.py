"""CLI wrapper for Task08 research backtest harness."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .runner import back


def main(args: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="qrp-atlas-backtest",
        description="QRP Atlas Research Backtest & Historical Replay Harness (Task08)",
    )
    parser.add_argument("--config", "-c", type=str, help="Path to JSON request configuration file")
    parser.add_argument("--period", "-p", nargs=2, metavar=("START", "END"), help="Period start and end dates")
    parser.add_argument("--universe", "-u", type=str, default="all_a", help="Universe preset or comma-separated tickers")
    parser.add_argument("--factor-field", type=str, help="Factor field name to evaluate in Factor mode")
    parser.add_argument("--experiment-score", type=str, help="Score field name or JSON for Experiment mode")
    parser.add_argument("--strategy-code", type=str, help="Strategy code for Strategy mode")
    parser.add_argument("--preset", type=str, default="a_share_daily", help="Execution preset")

    parsed = parser.parse_args(args)

    if parsed.config:
        with open(parsed.config, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        result = back(**cfg)
        print(result.summary())
        return 0

    if not parsed.period:
        parser.error("Either --config or --period is required")

    period = (parsed.period[0], parsed.period[1])
    universe: Any = parsed.universe
    if "," in universe:
        universe = [t.strip() for t in universe.split(",") if t.strip()]

    kwargs: dict[str, Any] = {
        "period": period,
        "universe": universe,
        "execution": {"preset": parsed.preset},
    }

    if parsed.factor_field:
        kwargs["factor"] = {"field": parsed.factor_field}
    elif parsed.experiment_score:
        try:
            score = json.loads(parsed.experiment_score)
        except Exception:
            score = parsed.experiment_score
        kwargs["experiment"] = {"score": score}
    elif parsed.strategy_code:
        kwargs["strategy"] = {"code": parsed.strategy_code}
    else:
        parser.error("One of --factor-field, --experiment-score, or --strategy-code is required")

    result = back(**kwargs)
    print(result.summary())
    return 0


if __name__ == "__main__":
    sys.exit(main())
