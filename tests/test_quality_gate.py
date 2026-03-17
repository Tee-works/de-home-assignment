from __future__ import annotations

import csv
import json
from pathlib import Path

from de_home_assignment.quality_gate import QUALITY_CHECKS, run_quality_gate, validate_outputs


def _write_valid_artifacts(outputs_dir: Path) -> None:
    outputs_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "raw_events": 3,
        "cleaned_events": 2,
        "dropped_events": 1,
        "dropped_by_reason": {"duplicate_event_id": 1},
        "normalized_timestamps": 0,
        "normalized_status_codes": 0,
        "normalized_latency": 0,
        "duplicates_dropped": 1,
    }
    (outputs_dir / "cleaning_report.json").write_text(json.dumps(report), encoding="utf-8")

    with (outputs_dir / "metrics_per_service_per_minute.csv").open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "service",
                "minute",
                "request_count",
                "average_latency_ms",
                "p50_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
                "error_rate",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "service": "checkout",
                "minute": "2025-01-12T10:00:00Z",
                "request_count": 2,
                "average_latency_ms": 120.0,
                "p50_latency_ms": 120.0,
                "p95_latency_ms": 120.0,
                "p99_latency_ms": 120.0,
                "error_rate": 0.5,
            }
        )

    with (outputs_dir / "cleaned_events.jsonl").open("w", encoding="utf-8") as file:
        file.write(
            json.dumps(
                {
                    "event_id": "e1",
                    "timestamp": "2025-01-12T10:00:00Z",
                    "service": "checkout",
                    "event_type": "request_completed",
                    "latency_ms": 100,
                    "status_code": 200,
                    "user_id": "u1",
                }
            )
            + "\n"
        )
        file.write(
            json.dumps(
                {
                    "event_id": "e2",
                    "timestamp": "2025-01-12T10:00:30Z",
                    "service": "checkout",
                    "event_type": "request_completed",
                    "latency_ms": 140,
                    "status_code": 500,
                    "user_id": "u2",
                }
            )
            + "\n"
        )


def test_validate_outputs_accepts_valid_artifacts(tmp_path: Path) -> None:
    outputs_dir = tmp_path / "outputs"
    _write_valid_artifacts(outputs_dir)

    errors = validate_outputs(outputs_dir, max_drop_rate=0.5)
    assert errors == []

    result = run_quality_gate(outputs_dir, max_drop_rate=0.5)
    assert result.passed is True
    assert result.checks_run == len(QUALITY_CHECKS)


def test_validate_outputs_catches_mismatch(tmp_path: Path) -> None:
    outputs_dir = tmp_path / "outputs"
    _write_valid_artifacts(outputs_dir)

    bad_report = json.loads((outputs_dir / "cleaning_report.json").read_text(encoding="utf-8"))
    bad_report["cleaned_events"] = 999
    (outputs_dir / "cleaning_report.json").write_text(json.dumps(bad_report), encoding="utf-8")

    errors = validate_outputs(outputs_dir, max_drop_rate=0.5)
    assert any("request_count" in error for error in errors)


def test_validate_outputs_handles_non_numeric_report_values(tmp_path: Path) -> None:
    outputs_dir = tmp_path / "outputs"
    _write_valid_artifacts(outputs_dir)

    bad_report = json.loads((outputs_dir / "cleaning_report.json").read_text(encoding="utf-8"))
    bad_report["raw_events"] = "not-an-int"
    (outputs_dir / "cleaning_report.json").write_text(json.dumps(bad_report), encoding="utf-8")

    errors = validate_outputs(outputs_dir, max_drop_rate=0.5)
    assert any("raw_events must be an integer" in error for error in errors)


def test_validate_outputs_rejects_float_like_integer_fields(tmp_path: Path) -> None:
    outputs_dir = tmp_path / "outputs"
    _write_valid_artifacts(outputs_dir)

    bad_report = json.loads((outputs_dir / "cleaning_report.json").read_text(encoding="utf-8"))
    bad_report["raw_events"] = 3.0
    (outputs_dir / "cleaning_report.json").write_text(json.dumps(bad_report), encoding="utf-8")

    errors = validate_outputs(outputs_dir, max_drop_rate=0.5)
    assert any("raw_events must be an integer" in error for error in errors)


def test_validate_outputs_catches_invalid_percentile_order(tmp_path: Path) -> None:
    outputs_dir = tmp_path / "outputs"
    _write_valid_artifacts(outputs_dir)

    csv_path = outputs_dir / "metrics_per_service_per_minute.csv"
    with csv_path.open("r", encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))
    rows[0]["p50_latency_ms"] = "120"
    rows[0]["p95_latency_ms"] = "110"
    rows[0]["p99_latency_ms"] = "100"
    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    errors = validate_outputs(outputs_dir, max_drop_rate=0.5)
    assert any("expected p50 <= p95 <= p99" in error for error in errors)


def test_validate_outputs_rejects_negative_drop_reason_count(tmp_path: Path) -> None:
    outputs_dir = tmp_path / "outputs"
    _write_valid_artifacts(outputs_dir)

    bad_report = json.loads((outputs_dir / "cleaning_report.json").read_text(encoding="utf-8"))
    bad_report["dropped_by_reason"]["duplicate_event_id"] = -1
    (outputs_dir / "cleaning_report.json").write_text(json.dumps(bad_report), encoding="utf-8")

    errors = validate_outputs(outputs_dir, max_drop_rate=0.5)
    assert any("must be >= 0" in error for error in errors)
