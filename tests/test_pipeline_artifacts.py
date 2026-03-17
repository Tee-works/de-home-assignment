from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

EXPECTED_REPORT = {
    "raw_events": 2020,
    "cleaned_events": 1945,
    "dropped_events": 75,
    "dropped_by_reason": {
        "duplicate_event_id": 20,
        "invalid_status_code": 10,
        "invalid_timestamp": 6,
        "missing_service": 6,
        "missing_status_code": 9,
        "missing_timestamp": 24,
    },
    "normalized_timestamps": 6,
    "normalized_status_codes": 2,
    "normalized_latency": 0,
    "duplicates_dropped": 20,
}


def test_pipeline_script_generates_expected_artifacts() -> None:
    project_root = Path(__file__).resolve().parents[1]

    env = dict(os.environ)
    env["PYTHONPATH"] = str(project_root / "src")

    subprocess.run(
        [sys.executable, str(project_root / "scripts" / "run_pipeline.py")],
        cwd=project_root,
        check=True,
        env=env,
    )

    outputs_dir = project_root / "outputs"
    report_path = outputs_dir / "cleaning_report.json"
    metrics_path = outputs_dir / "metrics_per_service_per_minute.csv"
    cleaned_path = outputs_dir / "cleaned_events.jsonl"

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report == EXPECTED_REPORT

    with metrics_path.open("r", encoding="utf-8", newline="") as metrics_file:
        reader = csv.DictReader(metrics_file)
        assert reader.fieldnames == [
            "service",
            "minute",
            "request_count",
            "average_latency_ms",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
            "error_rate",
        ]
        rows = list(reader)

    assert len(rows) == 789
    assert sum(int(row["request_count"]) for row in rows) == 1945

    cleaned_lines = cleaned_path.read_text(encoding="utf-8").splitlines()
    assert len(cleaned_lines) == 1945
    first_event = json.loads(cleaned_lines[0])
    assert sorted(first_event.keys()) == [
        "event_id",
        "event_type",
        "latency_ms",
        "service",
        "status_code",
        "timestamp",
        "user_id",
    ]
