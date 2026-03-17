from __future__ import annotations

import json
from pathlib import Path

from de_home_assignment.pipeline import (
    PipelineConfig,
    aggregate_metrics,
    run_pipeline,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


def test_pipeline_cleans_and_deduplicates(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:10Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 200,
            "user_id": "u1",
        },
        {
            "event_id": "e2",
            "timestamp": "12/01/2025 10:00:20",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": "140",
            "status_code": "500",
            "user_id": "u2",
        },
        {
            "event_id": "e2",
            "timestamp": "2025-01-12T10:00:30Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 111,
            "status_code": 200,
            "user_id": "u2",
        },
        {
            "event_id": "e3",
            "timestamp": "not-a-timestamp",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 200,
            "user_id": "u1",
        },
    ]

    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    cleaned, metrics, report = run_pipeline(
        input_path,
        PipelineConfig(deduplicate_by_event_id=True),
    )

    assert len(cleaned) == 2
    assert report.raw_events == 4
    assert report.cleaned_events == 2
    assert report.dropped_by_reason["invalid_timestamp"] == 1
    assert report.dropped_by_reason["duplicate_event_id"] == 1
    assert report.normalized_timestamps == 1
    assert report.normalized_status_codes == 1
    assert report.normalized_latency == 1

    assert len(metrics) == 1
    metric = metrics[0]
    assert metric.request_count == 2
    assert metric.average_latency_ms == 120.0
    assert metric.p50_latency_ms == 120.0
    assert metric.p95_latency_ms == 138.0
    assert metric.p99_latency_ms == 139.6
    assert metric.error_rate == 0.5


def test_aggregate_metrics_handles_missing_latency(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:00Z",
            "service": "auth",
            "event_type": "request_started",
            "latency_ms": None,
            "status_code": 200,
            "user_id": "u1",
        },
        {
            "event_id": "e2",
            "timestamp": "2025-01-12T10:00:30Z",
            "service": "auth",
            "event_type": "request_completed",
            "latency_ms": 80,
            "status_code": 503,
            "user_id": "u2",
        },
    ]

    path = tmp_path / "events_aggregate.jsonl"
    _write_jsonl(path, rows)
    cleaned, _, _ = run_pipeline(path)

    metrics = aggregate_metrics(cleaned)
    assert len(metrics) == 1
    assert metrics[0].request_count == 2
    assert metrics[0].average_latency_ms == 80.0
    assert metrics[0].p50_latency_ms == 80.0
    assert metrics[0].p95_latency_ms == 80.0
    assert metrics[0].p99_latency_ms == 80.0
    assert metrics[0].error_rate == 0.5
