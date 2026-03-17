from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil, floor
from pathlib import Path
from typing import Any, TypedDict

REQUIRED_FIELDS = ("event_id", "timestamp", "service", "event_type", "status_code")
logger = logging.getLogger(__name__)


class CleanEventMeta(TypedDict):
    drop_reason: str | None
    timestamp_normalized: bool
    status_normalized: bool
    latency_normalized: bool


@dataclass(frozen=True)
class PipelineConfig:
    deduplicate_by_event_id: bool = True


@dataclass(frozen=True)
class CleaningReport:
    raw_events: int
    cleaned_events: int
    dropped_events: int
    dropped_by_reason: dict[str, int]
    normalized_timestamps: int
    normalized_status_codes: int
    normalized_latency: int
    duplicates_dropped: int


@dataclass(frozen=True)
class CleanedEvent:
    event_id: str
    timestamp: datetime
    service: str
    event_type: str
    latency_ms: int | None
    status_code: int
    user_id: str | None

    def as_output_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "service": self.service,
            "event_type": self.event_type,
            "latency_ms": self.latency_ms,
            "status_code": self.status_code,
            "user_id": self.user_id,
        }


@dataclass(frozen=True)
class ServiceMinuteMetric:
    service: str
    minute: str
    request_count: int
    average_latency_ms: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    p99_latency_ms: float | None
    error_rate: float

    def as_output_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "minute": self.minute,
            "request_count": self.request_count,
            "average_latency_ms": self.average_latency_ms,
            "p50_latency_ms": self.p50_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "error_rate": self.error_rate,
        }


def parse_timestamp(raw: Any) -> tuple[datetime | None, bool]:
    """Parse supported timestamp formats and indicate if normalization was applied."""
    if not isinstance(raw, str) or not raw.strip():
        return None, False

    value = raw.strip()

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _to_utc(parsed), False
    except ValueError:
        pass

    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt).replace(tzinfo=UTC)
            return parsed, True
        except ValueError:
            continue

    return None, False


def parse_status_code(raw: Any) -> tuple[int | None, bool]:
    if isinstance(raw, int):
        code = raw
        normalized = False
    elif isinstance(raw, str) and raw.strip().isdigit():
        code = int(raw.strip())
        normalized = True
    else:
        return None, False

    if 100 <= code <= 599:
        return code, normalized
    return None, normalized


def parse_latency_ms(raw: Any) -> tuple[int | None, bool]:
    if raw is None or raw == "":
        return None, False

    if isinstance(raw, int):
        if raw < 0:
            return None, False
        return raw, False

    if isinstance(raw, str):
        value = raw.strip()
        if value.isdigit():
            return int(value), True

    return None, False


def clean_event(raw_event: dict[str, Any]) -> tuple[CleanedEvent | None, CleanEventMeta]:
    meta: CleanEventMeta = {
        "drop_reason": None,
        "timestamp_normalized": False,
        "status_normalized": False,
        "latency_normalized": False,
    }

    for field in REQUIRED_FIELDS:
        if field not in raw_event or raw_event[field] in (None, ""):
            meta["drop_reason"] = f"missing_{field}"
            return None, meta

    event_id_raw = raw_event["event_id"]
    if not isinstance(event_id_raw, str) or not event_id_raw.strip():
        meta["drop_reason"] = "invalid_event_id"
        return None, meta

    service_raw = raw_event["service"]
    if not isinstance(service_raw, str) or not service_raw.strip():
        meta["drop_reason"] = "invalid_service"
        return None, meta

    event_type_raw = raw_event["event_type"]
    if not isinstance(event_type_raw, str) or not event_type_raw.strip():
        meta["drop_reason"] = "invalid_event_type"
        return None, meta

    timestamp, ts_normalized = parse_timestamp(raw_event["timestamp"])
    if timestamp is None:
        meta["drop_reason"] = "invalid_timestamp"
        return None, meta

    status_code, status_normalized = parse_status_code(raw_event["status_code"])
    if status_code is None:
        meta["drop_reason"] = "invalid_status_code"
        return None, meta

    latency_ms, latency_normalized = parse_latency_ms(raw_event.get("latency_ms"))

    user_id_raw = raw_event.get("user_id")
    user_id = user_id_raw.strip() if isinstance(user_id_raw, str) and user_id_raw.strip() else None

    cleaned = CleanedEvent(
        event_id=event_id_raw.strip(),
        timestamp=timestamp,
        service=service_raw.strip(),
        event_type=event_type_raw.strip(),
        latency_ms=latency_ms,
        status_code=status_code,
        user_id=user_id,
    )

    meta["timestamp_normalized"] = ts_normalized
    meta["status_normalized"] = status_normalized
    meta["latency_normalized"] = latency_normalized
    return cleaned, meta


def load_events(input_path: Path) -> list[dict[str, Any]]:
    logger.info("Loading events from %s", input_path)
    events: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as infile:
        for line_number, line in enumerate(infile, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                events.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {line_number}: {exc}") from exc
    logger.info("Loaded %s raw events", len(events))
    return events


def run_pipeline(
    input_path: Path,
    config: PipelineConfig | None = None,
) -> tuple[list[CleanedEvent], list[ServiceMinuteMetric], CleaningReport]:
    """Run full batch pipeline: load, clean/dedupe, aggregate, and return report."""
    cfg = config or PipelineConfig()
    logger.info("Starting pipeline run (deduplicate_by_event_id=%s)", cfg.deduplicate_by_event_id)
    raw_events = load_events(input_path)

    dropped_by_reason: dict[str, int] = {}
    cleaned_events: list[CleanedEvent] = []

    normalized_timestamps = 0
    normalized_status_codes = 0
    normalized_latency = 0
    duplicates_dropped = 0

    seen_event_ids: set[str] = set()

    for raw_event in raw_events:
        cleaned, meta = clean_event(raw_event)
        if cleaned is None:
            reason = str(meta["drop_reason"])
            dropped_by_reason[reason] = dropped_by_reason.get(reason, 0) + 1
            continue

        if cfg.deduplicate_by_event_id and cleaned.event_id in seen_event_ids:
            # First occurrence wins for deterministic counts across reruns.
            duplicates_dropped += 1
            dropped_by_reason["duplicate_event_id"] = (
                dropped_by_reason.get("duplicate_event_id", 0) + 1
            )
            continue

        seen_event_ids.add(cleaned.event_id)
        cleaned_events.append(cleaned)

        if meta["timestamp_normalized"]:
            normalized_timestamps += 1
        if meta["status_normalized"]:
            normalized_status_codes += 1
        if meta["latency_normalized"]:
            normalized_latency += 1

    metrics = aggregate_metrics(cleaned_events)
    logger.info(
        "Cleaning completed (raw=%s, cleaned=%s, dropped=%s, duplicates_dropped=%s)",
        len(raw_events),
        len(cleaned_events),
        len(raw_events) - len(cleaned_events),
        duplicates_dropped,
    )
    if dropped_by_reason:
        logger.info("Dropped-by-reason: %s", dict(sorted(dropped_by_reason.items())))

    report = CleaningReport(
        raw_events=len(raw_events),
        cleaned_events=len(cleaned_events),
        dropped_events=len(raw_events) - len(cleaned_events),
        dropped_by_reason=dict(sorted(dropped_by_reason.items())),
        normalized_timestamps=normalized_timestamps,
        normalized_status_codes=normalized_status_codes,
        normalized_latency=normalized_latency,
        duplicates_dropped=duplicates_dropped,
    )

    logger.info("Aggregation completed (metric_rows=%s)", len(metrics))
    return cleaned_events, metrics, report


def aggregate_metrics(cleaned_events: list[CleanedEvent]) -> list[ServiceMinuteMetric]:
    """Aggregate per (service, minute) with latency percentiles and error rate."""
    grouped: dict[tuple[str, str], dict[str, Any]] = {}

    for event in cleaned_events:
        minute_bucket = event.timestamp.astimezone(UTC).replace(second=0, microsecond=0)
        minute_key = minute_bucket.isoformat().replace("+00:00", "Z")
        key = (event.service, minute_key)

        if key not in grouped:
            grouped[key] = {
                "request_count": 0,
                "error_count": 0,
                "latency_sum": 0,
                "latency_count": 0,
                "latencies": [],
            }

        bucket = grouped[key]
        bucket["request_count"] += 1
        # Treat any non-2xx status as an error for assignment-level SLI reporting.
        if not (200 <= event.status_code <= 299):
            bucket["error_count"] += 1

        if event.latency_ms is not None:
            bucket["latency_sum"] += event.latency_ms
            bucket["latency_count"] += 1
            bucket["latencies"].append(event.latency_ms)

    metrics: list[ServiceMinuteMetric] = []
    for (service, minute), values in sorted(
        grouped.items(),
        key=lambda item: (item[0][1], item[0][0]),
    ):
        latency_count = values["latency_count"]
        average_latency = round(values["latency_sum"] / latency_count, 2) if latency_count else None
        p50_latency = _percentile(values["latencies"], 50)
        p95_latency = _percentile(values["latencies"], 95)
        p99_latency = _percentile(values["latencies"], 99)
        error_rate = round(values["error_count"] / values["request_count"], 4)

        metrics.append(
            ServiceMinuteMetric(
                service=service,
                minute=minute,
                request_count=values["request_count"],
                average_latency_ms=average_latency,
                p50_latency_ms=p50_latency,
                p95_latency_ms=p95_latency,
                p99_latency_ms=p99_latency,
                error_rate=error_rate,
            )
        )

    return metrics


def write_cleaned_events_jsonl(events: list[CleanedEvent], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as outfile:
        for event in events:
            outfile.write(json.dumps(event.as_output_dict(), separators=(",", ":")))
            outfile.write("\n")
    logger.info("Wrote cleaned events JSONL to %s (rows=%s)", output_path, len(events))


def write_metrics_csv(metrics: list[ServiceMinuteMetric], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(
            outfile,
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
        for metric in metrics:
            writer.writerow(metric.as_output_dict())
    logger.info("Wrote metrics CSV to %s (rows=%s)", output_path, len(metrics))


def write_report_json(report: CleaningReport, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "raw_events": report.raw_events,
        "cleaned_events": report.cleaned_events,
        "dropped_events": report.dropped_events,
        "dropped_by_reason": report.dropped_by_reason,
        "normalized_timestamps": report.normalized_timestamps,
        "normalized_status_codes": report.normalized_status_codes,
        "normalized_latency": report.normalized_latency,
        "duplicates_dropped": report.duplicates_dropped,
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Wrote cleaning report JSON to %s", output_path)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _percentile(values: list[int], percentile: float) -> float | None:
    """Compute percentile via linear interpolation on the sorted sample."""
    if not values:
        return None

    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    position = (len(sorted_values) - 1) * (percentile / 100)
    lower_index = floor(position)
    upper_index = ceil(position)

    if lower_index == upper_index:
        return float(sorted_values[lower_index])

    weight = position - lower_index
    interpolated = sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight
    return round(interpolated, 2)
