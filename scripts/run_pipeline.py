#!/usr/bin/env python3
from __future__ import annotations

import logging
from pathlib import Path

from de_home_assignment.pipeline import (
    PipelineConfig,
    run_pipeline,
    write_cleaned_events_jsonl,
    write_metrics_csv,
    write_report_json,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / "HomeAssignmentEvents.jsonl"
    output_dir = project_root / "outputs"

    cleaned_events, metrics, report = run_pipeline(
        input_path=input_path,
        config=PipelineConfig(deduplicate_by_event_id=True),
    )

    write_cleaned_events_jsonl(cleaned_events, output_dir / "cleaned_events.jsonl")
    write_metrics_csv(metrics, output_dir / "metrics_per_service_per_minute.csv")
    write_report_json(report, output_dir / "cleaning_report.json")

    print("Pipeline completed.")
    print(f"Raw events: {report.raw_events}")
    print(f"Cleaned events: {report.cleaned_events}")
    print(f"Dropped events: {report.dropped_events}")
    print(f"Metrics rows: {len(metrics)}")


if __name__ == "__main__":
    main()
