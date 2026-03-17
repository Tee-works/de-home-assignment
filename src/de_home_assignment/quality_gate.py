from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

EXPECTED_REPORT_KEYS = {
    "raw_events",
    "cleaned_events",
    "dropped_events",
    "dropped_by_reason",
    "normalized_timestamps",
    "normalized_status_codes",
    "normalized_latency",
    "duplicates_dropped",
}

EXPECTED_METRICS_FIELDS = [
    "service",
    "minute",
    "request_count",
    "average_latency_ms",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "error_rate",
]

QUALITY_CHECKS = [
    "required_artifacts_exist",
    "report_required_keys",
    "report_numeric_fields",
    "report_internal_consistency",
    "drop_rate_threshold",
    "metrics_schema",
    "metrics_row_validation",
    "percentile_consistency",
    "metrics_request_count_total",
    "cleaned_events_line_count",
]


@dataclass(frozen=True)
class QualityGateResult:
    passed: bool
    errors: list[str]
    checks_run: int


def validate_outputs(outputs_dir: Path, max_drop_rate: float = 0.10) -> list[str]:
    """Validate output artifacts against schema and cross-file consistency contracts."""
    logger.info("Running quality checks on %s", outputs_dir)
    errors: list[str] = []

    cleaned_path = outputs_dir / "cleaned_events.jsonl"
    metrics_path = outputs_dir / "metrics_per_service_per_minute.csv"
    report_path = outputs_dir / "cleaning_report.json"

    for path in (cleaned_path, metrics_path, report_path):
        if not path.exists():
            errors.append(f"Missing required artifact: {path.name}")

    # Stop early if core artifacts are missing; downstream checks depend on them.
    if errors:
        return errors

    report = json.loads(report_path.read_text(encoding="utf-8"))

    missing_report_keys = EXPECTED_REPORT_KEYS - set(report.keys())
    if missing_report_keys:
        errors.append(f"cleaning_report.json missing keys: {sorted(missing_report_keys)}")

    raw_events = _parse_int(report.get("raw_events"), "raw_events", errors)
    cleaned_events = _parse_int(report.get("cleaned_events"), "cleaned_events", errors)
    dropped_events = _parse_int(report.get("dropped_events"), "dropped_events", errors)
    dropped_by_reason = report.get("dropped_by_reason", {})
    duplicates_dropped = _parse_int(report.get("duplicates_dropped"), "duplicates_dropped", errors)

    if raw_events is not None and raw_events <= 0:
        errors.append("raw_events must be > 0")
    if cleaned_events is not None and cleaned_events < 0:
        errors.append("cleaned_events must be >= 0")
    if dropped_events is not None and dropped_events < 0:
        errors.append("dropped_events must be >= 0")

    if raw_events is not None and cleaned_events is not None and cleaned_events > raw_events:
        errors.append("cleaned_events must be between 0 and raw_events")

    if raw_events is not None and cleaned_events is not None and dropped_events is not None:
        if dropped_events != raw_events - cleaned_events:
            errors.append("dropped_events must equal raw_events - cleaned_events")

    dropped_reasons_total: int | None = None
    duplicate_reason_count: int | None = None
    if not isinstance(dropped_by_reason, dict):
        errors.append("dropped_by_reason must be a dictionary")
    else:
        dropped_reasons_total = 0
        for reason, value in dropped_by_reason.items():
            parsed_value = _parse_int(value, f"dropped_by_reason['{reason}']", errors)
            if parsed_value is None:
                dropped_reasons_total = None
                break
            if parsed_value < 0:
                errors.append(f"dropped_by_reason['{reason}'] must be >= 0")
            dropped_reasons_total += parsed_value
        duplicate_reason_count = _parse_int(
            dropped_by_reason.get("duplicate_event_id", 0),
            "duplicate_event_id",
            errors,
        )

    if (
        dropped_reasons_total is not None
        and dropped_events is not None
        and dropped_reasons_total != dropped_events
    ):
        errors.append("Sum of dropped_by_reason values must equal dropped_events")

    if (
        duplicate_reason_count is not None
        and duplicates_dropped is not None
        and duplicate_reason_count != duplicates_dropped
    ):
        errors.append("duplicate_event_id count must equal duplicates_dropped")

    if raw_events is not None and dropped_events is not None and raw_events > 0:
        drop_rate = dropped_events / raw_events
        if drop_rate > max_drop_rate:
            errors.append(f"Drop rate {drop_rate:.4f} exceeds threshold {max_drop_rate:.4f}")

    with metrics_path.open("r", encoding="utf-8", newline="") as metrics_file:
        reader = csv.DictReader(metrics_file)
        if reader.fieldnames != EXPECTED_METRICS_FIELDS:
            errors.append(
                "metrics_per_service_per_minute.csv has unexpected columns: "
                f"{reader.fieldnames} (expected {EXPECTED_METRICS_FIELDS})"
            )

        request_count_total = 0
        for idx, row in enumerate(reader, start=2):
            service = row.get("service", "")
            minute_raw = row.get("minute")
            minute = minute_raw if isinstance(minute_raw, str) else ""
            if not service:
                errors.append(f"Row {idx}: service is empty")
            if not isinstance(minute_raw, str):
                errors.append(f"Row {idx}: minute is missing")

            try:
                datetime.fromisoformat(minute.replace("Z", "+00:00"))
            except ValueError:
                errors.append(f"Row {idx}: invalid minute timestamp '{minute}'")

            request_count = _parse_int(
                row.get("request_count"),
                f"Row {idx}: request_count",
                errors,
            )
            if request_count is not None:
                if request_count < 0:
                    errors.append(f"Row {idx}: request_count is negative")
                request_count_total += request_count

            average_latency = _parse_optional_float(
                row.get("average_latency_ms"), f"Row {idx}: average_latency_ms", errors
            )
            if average_latency is not None and average_latency < 0:
                errors.append(f"Row {idx}: average_latency_ms is negative")

            p50 = _parse_optional_float(
                row.get("p50_latency_ms"),
                f"Row {idx}: p50_latency_ms",
                errors,
            )
            p95 = _parse_optional_float(
                row.get("p95_latency_ms"),
                f"Row {idx}: p95_latency_ms",
                errors,
            )
            p99 = _parse_optional_float(
                row.get("p99_latency_ms"),
                f"Row {idx}: p99_latency_ms",
                errors,
            )

            for field_name, percentile_value in (
                ("p50_latency_ms", p50),
                ("p95_latency_ms", p95),
                ("p99_latency_ms", p99),
            ):
                if percentile_value is not None and percentile_value < 0:
                    errors.append(f"Row {idx}: {field_name} is negative")

            percentiles = [p50, p95, p99]
            any_percentile_present = any(value is not None for value in percentiles)
            all_percentiles_present = all(value is not None for value in percentiles)
            # Require percentile triplet atomicity to avoid partially-populated rows.
            if any_percentile_present and not all_percentiles_present:
                errors.append(f"Row {idx}: percentile fields must all be present or all be empty")
            if all_percentiles_present:
                assert p50 is not None and p95 is not None and p99 is not None
                if not (p50 <= p95 <= p99):
                    errors.append(f"Row {idx}: expected p50 <= p95 <= p99")
            if average_latency is None and any_percentile_present:
                errors.append(
                    f"Row {idx}: percentile values present but average_latency_ms is empty",
                )
            if average_latency is not None and not all_percentiles_present:
                errors.append(
                    f"Row {idx}: average_latency_ms present but percentile values are incomplete",
                )

            error_rate = _parse_float(row.get("error_rate"), f"Row {idx}: error_rate", errors)
            if error_rate is not None and not (0.0 <= error_rate <= 1.0):
                errors.append(f"Row {idx}: error_rate must be between 0 and 1")

        if cleaned_events is not None and request_count_total != cleaned_events:
            errors.append(
                "Sum of request_count values in metrics does not match cleaned_events "
                f"({request_count_total} != {cleaned_events})"
            )

    cleaned_lines = cleaned_path.read_text(encoding="utf-8").splitlines()
    if cleaned_events is not None and len(cleaned_lines) != cleaned_events:
        errors.append(
            "cleaned_events.jsonl line count does not match cleaned_events "
            f"({len(cleaned_lines)} != {cleaned_events})"
        )

    logger.info("Quality checks completed (errors=%s)", len(errors))
    return errors


def run_quality_gate(outputs_dir: Path, max_drop_rate: float = 0.10) -> QualityGateResult:
    errors = validate_outputs(outputs_dir=outputs_dir, max_drop_rate=max_drop_rate)
    if errors:
        logger.warning("Quality gate failed with %s issues", len(errors))
    else:
        logger.info("Quality gate passed")
    return QualityGateResult(
        passed=not errors,
        errors=errors,
        checks_run=len(QUALITY_CHECKS),
    )


def _parse_int(value: Any, field_name: str, errors: list[str]) -> int | None:
    if isinstance(value, bool):
        errors.append(f"{field_name} must be an integer")
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        # Reject float coercion (e.g. 3.0 -> 3) to keep contracts strict.
        errors.append(f"{field_name} must be an integer")
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if stripped and (
            stripped.isdigit()
            or (stripped.startswith("-") and stripped[1:].isdigit())
        ):
            return int(stripped)
        errors.append(f"{field_name} must be an integer")
        return None

    errors.append(f"{field_name} must be an integer")
    return None


def _parse_float(value: Any, field_name: str, errors: list[str]) -> float | None:
    try:
        if isinstance(value, bool):
            raise ValueError
        return float(value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} must be numeric")
        return None


def _parse_optional_float(value: Any, field_name: str, errors: list[str]) -> float | None:
    if value in (None, ""):
        return None
    return _parse_float(value, field_name, errors)
