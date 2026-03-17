from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from de_home_assignment.api import create_app


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


def test_metrics_endpoint_filters_by_service_and_time(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:00Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 200,
            "user_id": "u1",
        },
        {
            "event_id": "e2",
            "timestamp": "2025-01-12T10:01:00Z",
            "service": "payments",
            "event_type": "request_completed",
            "latency_ms": 120,
            "status_code": 500,
            "user_id": "u2",
        },
    ]

    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get(
            "/metrics",
            params={
                "service": "checkout",
                "from": "2025-01-12T10:00:00Z",
                "to": "2025-01-12T10:00:59Z",
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert len(payload["metrics"]) == 1
        assert payload["metrics"][0]["service"] == "checkout"
        assert "p50_latency_ms" in payload["metrics"][0]
        assert "p95_latency_ms" in payload["metrics"][0]
        assert "p99_latency_ms" in payload["metrics"][0]


def test_metrics_endpoint_rejects_bad_timestamp(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:00Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 200,
            "user_id": "u1",
        }
    ]

    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"from": "not-a-date"})
        assert response.status_code == 400
        assert "Invalid ISO-8601 timestamp" in response.json()["detail"]


def test_metrics_endpoint_rejects_inverted_time_window(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:00Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 200,
            "user_id": "u1",
        }
    ]

    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get(
            "/metrics",
            params={
                "from": "2025-01-12T10:01:00Z",
                "to": "2025-01-12T10:00:00Z",
            },
        )
        assert response.status_code == 400
        assert "'from' must be <= 'to'" == response.json()["detail"]


def test_summary_endpoint_returns_pipeline_stats(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:00Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 200,
            "user_id": "u1",
        },
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:10Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 101,
            "status_code": 200,
            "user_id": "u1",
        },
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/summary")
        payload = response.json()

        assert response.status_code == 200
        assert payload["raw_events"] == 2
        assert payload["cleaned_events"] == 1
        assert payload["dropped_events"] == 1
        assert payload["dropped_by_reason"] == {"duplicate_event_id": 1}
        assert payload["metrics_rows"] == 1
