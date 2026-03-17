from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

import pytest

from de_home_assignment.pipeline import run_pipeline

EXPECTED_DATASET_SHA256 = "f7b6595981b3b8135f8e1db8a2dc8c083e03ef8bbff9890748a02db780d64ef6"


def test_full_dataset_regression_counts() -> None:
    project_root = Path(__file__).resolve().parents[1]
    input_path = project_root / "HomeAssignmentEvents.jsonl"

    if _sha256_file(input_path) != EXPECTED_DATASET_SHA256:
        pytest.skip(
            "Dataset content differs from the pinned regression fixture; "
            "skipping exact-count assertions."
        )

    cleaned, metrics, report = run_pipeline(input_path)

    assert report.raw_events == 2020
    assert report.cleaned_events == 1945
    assert report.dropped_events == 75
    assert report.dropped_by_reason == {
        "duplicate_event_id": 20,
        "invalid_status_code": 10,
        "invalid_timestamp": 6,
        "missing_service": 6,
        "missing_status_code": 9,
        "missing_timestamp": 24,
    }
    assert report.normalized_timestamps == 6
    assert report.normalized_status_codes == 2
    assert report.normalized_latency == 0
    assert report.duplicates_dropped == 20

    assert len(metrics) == 789
    assert sum(metric.request_count for metric in metrics) == 1945

    by_service = Counter(event.service for event in cleaned)
    assert dict(sorted(by_service.items())) == {
        "auth": 387,
        "catalog": 384,
        "checkout": 396,
        "payments": 397,
        "search": 381,
    }

    assert min(metric.minute for metric in metrics) == "2025-01-12T09:00:00Z"
    assert max(metric.minute for metric in metrics) == "2025-01-12T11:59:00Z"

    for metric in metrics:
        if metric.average_latency_ms is None:
            assert metric.p50_latency_ms is None
            assert metric.p95_latency_ms is None
            assert metric.p99_latency_ms is None
        else:
            assert metric.p50_latency_ms is not None
            assert metric.p95_latency_ms is not None
            assert metric.p99_latency_ms is not None

    direct_error_events = sum(1 for event in cleaned if not (200 <= event.status_code <= 299))
    metric_error_events = sum(round(metric.error_rate * metric.request_count) for metric in metrics)
    assert metric_error_events == direct_error_events == 757


def test_full_dataset_invariants() -> None:
    project_root = Path(__file__).resolve().parents[1]
    input_path = project_root / "HomeAssignmentEvents.jsonl"

    cleaned, metrics, report = run_pipeline(input_path)

    assert report.raw_events > 0
    assert report.cleaned_events >= 0
    assert report.dropped_events == report.raw_events - report.cleaned_events
    assert sum(metric.request_count for metric in metrics) == report.cleaned_events
    assert all(0.0 <= metric.error_rate <= 1.0 for metric in metrics)

    for metric in metrics:
        if metric.average_latency_ms is None:
            assert metric.p50_latency_ms is None
            assert metric.p95_latency_ms is None
            assert metric.p99_latency_ms is None
        else:
            assert metric.p50_latency_ms is not None
            assert metric.p95_latency_ms is not None
            assert metric.p99_latency_ms is not None
            assert metric.p50_latency_ms <= metric.p95_latency_ms <= metric.p99_latency_ms


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
