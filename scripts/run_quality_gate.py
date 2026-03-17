#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from de_home_assignment.quality_gate import run_quality_gate


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Run assignment data quality checks on generated outputs.",
    )
    parser.add_argument(
        "--outputs-dir",
        default="outputs",
        help=(
            "Directory containing cleaned_events.jsonl, "
            "metrics_per_service_per_minute.csv, and cleaning_report.json"
        ),
    )
    parser.add_argument(
        "--max-drop-rate",
        type=float,
        default=0.10,
        help="Maximum allowed dropped_events/raw_events ratio",
    )
    args = parser.parse_args()

    result = run_quality_gate(
        outputs_dir=Path(args.outputs_dir),
        max_drop_rate=args.max_drop_rate,
    )

    if result.passed:
        print(f"Quality gate passed ({result.checks_run} checks).")
        return

    print("Quality gate failed with the following issues:")
    for error in result.errors:
        print(f"- {error}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
