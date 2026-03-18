from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from .pipeline import CleaningReport, PipelineConfig, ServiceMinuteMetric, run_pipeline

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    status: str


class MetricResponseItem(BaseModel):
    service: str
    minute: str
    request_count: int
    average_latency_ms: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    p99_latency_ms: float | None
    error_rate: float


class MetricsResponse(BaseModel):
    total: int
    limit: int
    offset: int
    returned: int
    has_more: bool
    next_offset: int | None
    metrics: list[MetricResponseItem]


class SummaryResponse(BaseModel):
    raw_events: int
    cleaned_events: int
    dropped_events: int
    dropped_by_reason: dict[str, int]
    metrics_rows: int


def create_app(input_path: Path | None = None) -> FastAPI:
    """Create API app and load computed pipeline data once during startup lifespan."""
    source_path = input_path or Path(__file__).resolve().parents[2] / "HomeAssignmentEvents.jsonl"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Keep startup work in lifespan (not import time) for safer runtime behavior.
        cleaned_events, metrics, report = run_pipeline(
            input_path=source_path,
            config=PipelineConfig(deduplicate_by_event_id=True),
        )
        app.state.cleaned_events = cleaned_events
        app.state.metrics = metrics
        app.state.report = report
        logger.info(
            "API initialized (cleaned_events=%s, metrics_rows=%s, source=%s)",
            len(cleaned_events),
            len(metrics),
            source_path,
        )
        yield

    app = FastAPI(title="Home Assignment Metrics API", version="1.0.0", lifespan=lifespan)
    app.state.cleaned_events = None
    app.state.metrics = None
    app.state.report = None

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/metrics", response_model=MetricsResponse)
    def get_metrics(
        service: Annotated[
            list[str] | None,
            Query(
                description=(
                    "Filter by service. Supports repeated query params and/or comma-separated "
                    "values (e.g. ?service=checkout&service=payments or "
                    "?service=checkout,payments)."
                ),
            ),
        ] = None,
        from_ts: Annotated[
            str | None,
            Query(
                alias="from",
                description="ISO-8601 start timestamp (inclusive, timezone required)",
            ),
        ] = None,
        to_ts: Annotated[
            str | None,
            Query(
                alias="to",
                description="ISO-8601 end timestamp (inclusive, timezone required)",
            ),
        ] = None,
        min_error_rate: Annotated[
            float | None,
            Query(
                ge=0.0,
                le=1.0,
                description="Filter rows with error_rate >= value",
            ),
        ] = None,
        max_error_rate: Annotated[
            float | None,
            Query(
                ge=0.0,
                le=1.0,
                description="Filter rows with error_rate <= value",
            ),
        ] = None,
        min_request_count: Annotated[
            int | None,
            Query(
                ge=0,
                description="Filter rows with request_count >= value",
            ),
        ] = None,
        max_request_count: Annotated[
            int | None,
            Query(
                ge=0,
                description="Filter rows with request_count <= value",
            ),
        ] = None,
        sort_by: Annotated[
            str,
            Query(
                description=(
                    "Sort field: minute, service, request_count, error_rate, "
                    "average_latency_ms, p50_latency_ms, p95_latency_ms, p99_latency_ms"
                ),
            ),
        ] = "minute",
        order: Annotated[str, Query(description="Sort order: asc or desc")] = "asc",
        limit: Annotated[
            int,
            Query(
                ge=1,
                le=1000,
                description="Max rows to return (1-1000)",
            ),
        ] = 100,
        offset: Annotated[int, Query(ge=0, description="Number of rows to skip")] = 0,
    ) -> MetricsResponse:
        _validate_sort_params(sort_by, order)
        _validate_filter_ranges(
            min_error_rate=min_error_rate,
            max_error_rate=max_error_rate,
            min_request_count=min_request_count,
            max_request_count=max_request_count,
        )

        from_dt = _parse_optional_iso_ts(from_ts, "from")
        to_dt = _parse_optional_iso_ts(to_ts, "to")

        if from_dt and to_dt and from_dt > to_dt:
            raise HTTPException(status_code=400, detail="'from' must be <= 'to'")

        metrics = _get_metrics_or_raise(app)
        services = _normalize_services(service)
        filtered = _filter_metrics(
            metrics=metrics,
            services=services,
            from_dt=from_dt,
            to_dt=to_dt,
            min_error_rate=min_error_rate,
            max_error_rate=max_error_rate,
            min_request_count=min_request_count,
            max_request_count=max_request_count,
        )
        sorted_metrics = _sort_metrics(filtered, sort_by, order == "desc")
        page = sorted_metrics[offset : offset + limit]
        has_more = offset + len(page) < len(filtered)
        next_offset = offset + len(page) if has_more else None

        logger.info(
            (
                "GET /metrics services=%s from=%s to=%s min_error_rate=%s max_error_rate=%s "
                "min_request_count=%s max_request_count=%s sort=%s order=%s limit=%s offset=%s "
                "-> total=%s returned=%s"
            ),
            services,
            from_ts,
            to_ts,
            min_error_rate,
            max_error_rate,
            min_request_count,
            max_request_count,
            sort_by,
            order,
            limit,
            offset,
            len(filtered),
            len(page),
        )
        return MetricsResponse(
            total=len(filtered),
            limit=limit,
            offset=offset,
            returned=len(page),
            has_more=has_more,
            next_offset=next_offset,
            metrics=[MetricResponseItem(**metric.as_output_dict()) for metric in page],
        )

    @app.get("/summary", response_model=SummaryResponse)
    def summary() -> SummaryResponse:
        report = _get_report_or_raise(app)
        metrics = _get_metrics_or_raise(app)
        return SummaryResponse(
            raw_events=report.raw_events,
            cleaned_events=report.cleaned_events,
            dropped_events=report.dropped_events,
            dropped_by_reason=report.dropped_by_reason,
            metrics_rows=len(metrics),
        )

    return app


SORT_ACCESSORS: dict[str, Callable[[ServiceMinuteMetric], object]] = {
    "minute": lambda metric: metric.minute,
    "service": lambda metric: metric.service,
    "request_count": lambda metric: metric.request_count,
    "error_rate": lambda metric: metric.error_rate,
    "average_latency_ms": lambda metric: metric.average_latency_ms,
    "p50_latency_ms": lambda metric: metric.p50_latency_ms,
    "p95_latency_ms": lambda metric: metric.p95_latency_ms,
    "p99_latency_ms": lambda metric: metric.p99_latency_ms,
}

VALID_SORT_FIELDS = set(SORT_ACCESSORS.keys())


def _validate_sort_params(sort_by: str, order: str) -> None:
    if sort_by not in VALID_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by '{sort_by}'. Must be one of: {sorted(VALID_SORT_FIELDS)}",
        )
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")


def _validate_filter_ranges(
    min_error_rate: float | None,
    max_error_rate: float | None,
    min_request_count: int | None,
    max_request_count: int | None,
) -> None:
    if (
        min_error_rate is not None
        and max_error_rate is not None
        and min_error_rate > max_error_rate
    ):
        raise HTTPException(status_code=400, detail="min_error_rate must be <= max_error_rate")
    if (
        min_request_count is not None
        and max_request_count is not None
        and min_request_count > max_request_count
    ):
        raise HTTPException(
            status_code=400,
            detail="min_request_count must be <= max_request_count",
        )


def _normalize_services(raw_services: list[str] | None) -> set[str] | None:
    if not raw_services:
        return None
    normalized: set[str] = set()
    for service in raw_services:
        for part in service.split(","):
            candidate = part.strip()
            if candidate:
                normalized.add(candidate)
    return normalized if normalized else None


def _filter_metrics(
    metrics: list[ServiceMinuteMetric],
    services: set[str] | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
    min_error_rate: float | None,
    max_error_rate: float | None,
    min_request_count: int | None,
    max_request_count: int | None,
) -> list[ServiceMinuteMetric]:
    out: list[ServiceMinuteMetric] = []

    for metric in metrics:
        if services and metric.service not in services:
            continue

        minute_dt = datetime.fromisoformat(metric.minute.replace("Z", "+00:00"))

        if from_dt and minute_dt < from_dt:
            continue
        if to_dt and minute_dt > to_dt:
            continue
        if min_error_rate is not None and metric.error_rate < min_error_rate:
            continue
        if max_error_rate is not None and metric.error_rate > max_error_rate:
            continue
        if min_request_count is not None and metric.request_count < min_request_count:
            continue
        if max_request_count is not None and metric.request_count > max_request_count:
            continue

        out.append(metric)

    return out


def _sort_metrics(
    metrics: list[ServiceMinuteMetric],
    sort_by: str,
    descending: bool,
) -> list[ServiceMinuteMetric]:
    accessor = SORT_ACCESSORS[sort_by]
    with_value = [metric for metric in metrics if accessor(metric) is not None]
    without_value = [metric for metric in metrics if accessor(metric) is None]

    sorted_with_value = sorted(
        with_value,
        key=lambda metric: (
            accessor(metric),
            metric.minute,
            metric.service,
        ),
        reverse=descending,
    )
    # Keep None-valued rows consistently at the end in both asc and desc orders.
    return sorted_with_value + without_value


def _parse_optional_iso_ts(raw: str | None, field_name: str) -> datetime | None:
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ISO-8601 timestamp for '{field_name}'",
        ) from exc
    if parsed.tzinfo is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Timestamp for '{field_name}' must include timezone "
                "(for example, trailing 'Z')"
            ),
        )
    return parsed.astimezone(UTC)


def _get_metrics_or_raise(app: FastAPI) -> list[ServiceMinuteMetric]:
    metrics = app.state.metrics
    if metrics is None:
        raise HTTPException(status_code=503, detail="Metrics data not loaded")
    return metrics


def _get_report_or_raise(app: FastAPI) -> CleaningReport:
    report = app.state.report
    if report is None:
        raise HTTPException(status_code=503, detail="Summary data not loaded")
    return report
