from __future__ import annotations

import json
from pathlib import Path

import pytest

from de_home_assignment.pipeline import (
    PipelineConfig,
    aggregate_metrics,
    load_events,
    parse_latency_ms,
    parse_status_code,
    parse_timestamp,
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


# --- parse_timestamp ---

@pytest.mark.parametrize("raw, expected_normalized", [
    ("2025-01-12T10:00:00Z", False),
    ("2025-01-12T10:00:00+00:00", False),
    ("2025-01-12T10:00:00+05:30", False),   # timezone offset — valid ISO-8601, not normalized
    ("12/01/2025 10:00:00", True),           # legacy DD/MM/YYYY
    ("2025/01/12 10:00:00", True),           # legacy YYYY/MM/DD
])
def test_parse_timestamp_valid(raw: str, expected_normalized: bool) -> None:
    result, normalized = parse_timestamp(raw)
    assert result is not None
    assert normalized == expected_normalized


@pytest.mark.parametrize("raw", [
    "not-a-date",
    "",
    "   ",
    None,
    12345,
    "2025-13-01T00:00:00Z",   # invalid month
    "2025-01-32T00:00:00Z",   # invalid day
])
def test_parse_timestamp_invalid(raw) -> None:
    result, _ = parse_timestamp(raw)
    assert result is None


# --- parse_status_code ---

@pytest.mark.parametrize("raw, expected_code, expected_normalized", [
    (200, 200, False),
    (500, 500, False),
    (100, 100, False),
    (599, 599, False),
    ("200", 200, True),
    ("404", 404, True),
])
def test_parse_status_code_valid(raw, expected_code: int, expected_normalized: bool) -> None:
    code, normalized = parse_status_code(raw)
    assert code == expected_code
    assert normalized == expected_normalized


@pytest.mark.parametrize("raw", [
    99,      # below range
    600,     # above range
    0,
    -1,
    "abc",
    None,
    "",
    3.14,
])
def test_parse_status_code_invalid(raw) -> None:
    code, _ = parse_status_code(raw)
    assert code is None


# --- parse_latency_ms ---

@pytest.mark.parametrize("raw, expected_value, expected_normalized", [
    (0, 0, False),
    (100, 100, False),
    ("200", 200, True),
    ("0", 0, True),
])
def test_parse_latency_ms_valid(raw, expected_value: int, expected_normalized: bool) -> None:
    value, normalized = parse_latency_ms(raw)
    assert value == expected_value
    assert normalized == expected_normalized


@pytest.mark.parametrize("raw", [
    -1,       # negative
    "",       # empty string
    None,     # missing
    "abc",    # non-numeric
    "-5",     # negative string
    3.14,     # float
])
def test_parse_latency_ms_invalid(raw) -> None:
    value, _ = parse_latency_ms(raw)
    assert value is None


# --- run_pipeline drop reasons ---

@pytest.mark.parametrize("missing_field", [
    "event_id", "timestamp", "service", "event_type", "status_code"
])
def test_pipeline_drops_missing_required_field(tmp_path: Path, missing_field: str) -> None:
    row = {
        "event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
        "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1",
    }
    del row[missing_field]
    path = tmp_path / "events.jsonl"
    _write_jsonl(path, [row])
    _, _, report = run_pipeline(path)
    assert report.dropped_events == 1
    assert f"missing_{missing_field}" in report.dropped_by_reason


def test_pipeline_keeps_event_with_null_latency(tmp_path: Path) -> None:
    row = {
        "event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
        "event_type": "request_completed", "latency_ms": None, "status_code": 200, "user_id": "u1",
    }
    path = tmp_path / "events.jsonl"
    _write_jsonl(path, [row])
    cleaned, _, report = run_pipeline(path)
    assert report.cleaned_events == 1
    assert cleaned[0].latency_ms is None


def test_pipeline_drops_negative_latency(tmp_path: Path) -> None:
    row = {
        "event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
        "event_type": "request_completed", "latency_ms": -50, "status_code": 200, "user_id": "u1",
    }
    path = tmp_path / "events.jsonl"
    _write_jsonl(path, [row])
    cleaned, _, report = run_pipeline(path)
    assert report.cleaned_events == 1
    assert cleaned[0].latency_ms is None  # dropped silently, event kept


def test_pipeline_dedup_disabled(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:10Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 110, "status_code": 200, "user_id": "u1"},
    ]
    path = tmp_path / "events.jsonl"
    _write_jsonl(path, rows)
    _, _, report = run_pipeline(path, PipelineConfig(deduplicate_by_event_id=False))
    assert report.cleaned_events == 2
    assert report.duplicates_dropped == 0


def test_pipeline_all_events_dropped(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "bad", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
        {"event_id": "e2", "timestamp": "bad", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
    ]
    path = tmp_path / "events.jsonl"
    _write_jsonl(path, rows)
    cleaned, metrics, report = run_pipeline(path)
    assert cleaned == []
    assert metrics == []
    assert report.cleaned_events == 0
    assert report.dropped_events == 2


# --- load_events ---

def test_load_events_raises_on_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text('{"valid": true}\nnot valid json\n', encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid JSON at line 2"):
        load_events(path)


def test_load_events_skips_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"event_id": "e1"}\n\n{"event_id": "e2"}\n', encoding="utf-8")
    events = load_events(path)
    assert len(events) == 2


# --- aggregate_metrics ---

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
