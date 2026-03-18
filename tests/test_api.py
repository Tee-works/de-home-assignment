from __future__ import annotations

import json
from pathlib import Path

import pytest
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


def test_metrics_endpoint_pagination(tmp_path: Path) -> None:
    rows = [
        {"event_id": f"e{i}", "timestamp": f"2025-01-12T10:{i:02d}:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"}
        for i in range(5)
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"limit": 2, "offset": 0})
        payload = response.json()
        assert response.status_code == 200
        assert payload["total"] == 5
        assert payload["limit"] == 2
        assert payload["offset"] == 0
        assert payload["returned"] == 2
        assert payload["has_more"] is True
        assert payload["next_offset"] == 2
        assert len(payload["metrics"]) == 2

        response2 = client.get("/metrics", params={"limit": 2, "offset": 2})
        payload2 = response2.json()
        assert len(payload2["metrics"]) == 2
        assert payload2["has_more"] is True
        assert payload2["next_offset"] == 4

        response3 = client.get("/metrics", params={"limit": 2, "offset": 4})
        payload3 = response3.json()
        assert len(payload3["metrics"]) == 1
        assert payload3["has_more"] is False
        assert payload3["next_offset"] is None

        # offset beyond total returns empty with has_more=False
        response4 = client.get("/metrics", params={"limit": 10, "offset": 100})
        payload4 = response4.json()
        assert len(payload4["metrics"]) == 0
        assert payload4["has_more"] is False
        assert payload4["next_offset"] is None


def test_metrics_endpoint_multi_service_filter(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
        {"event_id": "e2", "timestamp": "2025-01-12T10:01:00Z", "service": "payments",
         "event_type": "request_completed", "latency_ms": 120, "status_code": 200, "user_id": "u2"},
        {"event_id": "e3", "timestamp": "2025-01-12T10:02:00Z", "service": "auth",
         "event_type": "request_completed", "latency_ms": 80, "status_code": 200, "user_id": "u3"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"service": "checkout,payments"})
        payload = response.json()
        assert response.status_code == 200
        services = {m["service"] for m in payload["metrics"]}
        assert services == {"checkout", "payments"}
        assert "auth" not in services


def test_metrics_endpoint_sort_by_error_rate_desc(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 500, "user_id": "u1"},
        {"event_id": "e2", "timestamp": "2025-01-12T10:01:00Z", "service": "payments",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u2"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"sort_by": "error_rate", "order": "desc"})
        payload = response.json()
        assert response.status_code == 200
        rates = [m["error_rate"] for m in payload["metrics"]]
        assert rates == sorted(rates, reverse=True)


def test_metrics_endpoint_rejects_invalid_sort_field(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"sort_by": "invalid_field"})
        assert response.status_code == 400
        assert "Invalid sort_by" in response.json()["detail"]


def test_metrics_endpoint_rejects_invalid_order(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"order": "random"})
        assert response.status_code == 400
        assert "order must be" in response.json()["detail"]


def test_metrics_endpoint_rejects_limit_out_of_range(tmp_path: Path) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        assert client.get("/metrics", params={"limit": 0}).status_code == 422
        assert client.get("/metrics", params={"limit": 1001}).status_code == 422
        assert client.get("/metrics", params={"offset": -1}).status_code == 422


def test_metrics_endpoint_supports_repeated_service_params(tmp_path: Path) -> None:
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
            "status_code": 200,
            "user_id": "u2",
        },
        {
            "event_id": "e3",
            "timestamp": "2025-01-12T10:02:00Z",
            "service": "auth",
            "event_type": "request_completed",
            "latency_ms": 90,
            "status_code": 200,
            "user_id": "u3",
        },
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics?service=checkout&service=payments")
        payload = response.json()

        assert response.status_code == 200
        services = {row["service"] for row in payload["metrics"]}
        assert services == {"checkout", "payments"}
        assert payload["total"] == 2


def test_metrics_endpoint_filters_by_rate_and_count_ranges(tmp_path: Path) -> None:
    rows = [
        {
            "event_id": "e1",
            "timestamp": "2025-01-12T10:00:00Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 100,
            "status_code": 500,
            "user_id": "u1",
        },
        {
            "event_id": "e2",
            "timestamp": "2025-01-12T10:00:05Z",
            "service": "checkout",
            "event_type": "request_completed",
            "latency_ms": 110,
            "status_code": 200,
            "user_id": "u2",
        },
        {
            "event_id": "e3",
            "timestamp": "2025-01-12T10:01:00Z",
            "service": "payments",
            "event_type": "request_completed",
            "latency_ms": 120,
            "status_code": 200,
            "user_id": "u3",
        },
        {
            "event_id": "e4",
            "timestamp": "2025-01-12T10:01:05Z",
            "service": "payments",
            "event_type": "request_completed",
            "latency_ms": 130,
            "status_code": 200,
            "user_id": "u4",
        },
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get(
            "/metrics",
            params={
                "min_error_rate": 0.5,
                "max_error_rate": 1.0,
                "min_request_count": 2,
                "max_request_count": 2,
            },
        )
        payload = response.json()

        assert response.status_code == 200
        assert payload["total"] == 1
        assert payload["metrics"][0]["service"] == "checkout"
        assert payload["metrics"][0]["request_count"] == 2
        assert payload["metrics"][0]["error_rate"] == 0.5


def test_metrics_endpoint_rejects_inverted_filter_ranges(tmp_path: Path) -> None:
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
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        bad_error_range = client.get(
            "/metrics",
            params={"min_error_rate": 0.9, "max_error_rate": 0.2},
        )
        assert bad_error_range.status_code == 400
        assert "min_error_rate must be <= max_error_rate" in bad_error_range.json()["detail"]

        bad_count_range = client.get(
            "/metrics",
            params={"min_request_count": 5, "max_request_count": 1},
        )
        assert bad_count_range.status_code == 400
        assert "min_request_count must be <= max_request_count" in bad_count_range.json()["detail"]


def test_metrics_endpoint_rejects_naive_timestamp_filters(tmp_path: Path) -> None:
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
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/metrics", params={"from": "2025-01-12T10:00:00"})
        assert response.status_code == 400
        assert "must include timezone" in response.json()["detail"]


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


@pytest.mark.parametrize("sort_field", [
    "request_count",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "average_latency_ms",
    "service",
    "minute",
])
def test_metrics_endpoint_all_sort_fields_accepted(tmp_path: Path, sort_field: str) -> None:
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"},
        {"event_id": "e2", "timestamp": "2025-01-12T10:01:00Z", "service": "payments",
         "event_type": "request_completed", "latency_ms": 200, "status_code": 200, "user_id": "u2"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        for order in ("asc", "desc"):
            response = client.get("/metrics", params={"sort_by": sort_field, "order": order})
            assert response.status_code == 200, f"Failed for sort_by={sort_field} order={order}"


def test_metrics_endpoint_sort_by_nullable_field_puts_nulls_last(tmp_path: Path) -> None:
    # One event has latency, one does not — None-latency row must always sort last.
    rows = [
        {"event_id": "e1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout",
         "event_type": "request_completed", "latency_ms": None, "status_code": 200, "user_id": "u1"},  # noqa: E501
        {"event_id": "e2", "timestamp": "2025-01-12T10:01:00Z", "service": "payments",
         "event_type": "request_completed", "latency_ms": 50, "status_code": 200, "user_id": "u2"},
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        for order in ("asc", "desc"):
            response = client.get(
                "/metrics", params={"sort_by": "average_latency_ms", "order": order}
            )
            payload = response.json()
            assert response.status_code == 200
            # Row with None latency must always be last regardless of order
            last = payload["metrics"][-1]
            assert last["average_latency_ms"] is None, f"Expected None last for order={order}"


def test_openapi_metrics_response_schema_is_typed(tmp_path: Path) -> None:
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
    ]
    input_path = tmp_path / "events.jsonl"
    _write_jsonl(input_path, rows)

    app = create_app(input_path)
    with TestClient(app) as client:
        response = client.get("/openapi.json")
        assert response.status_code == 200
        spec = response.json()
        metrics_response_schema = spec["paths"]["/metrics"]["get"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
        assert "$ref" in metrics_response_schema
