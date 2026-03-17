from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from .pipeline import CleaningReport, PipelineConfig, ServiceMinuteMetric, run_pipeline

logger = logging.getLogger(__name__)


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

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    def get_metrics(
        service: str | None = Query(default=None),
        from_ts: str | None = Query(default=None, alias="from"),
        to_ts: str | None = Query(default=None, alias="to"),
    ) -> dict[str, list[dict[str, object]]]:
        from_dt = _parse_optional_iso_ts(from_ts, "from")
        to_dt = _parse_optional_iso_ts(to_ts, "to")

        if from_dt and to_dt and from_dt > to_dt:
            raise HTTPException(status_code=400, detail="'from' must be <= 'to'")

        metrics = _get_metrics_or_raise(app)
        filtered = _filter_metrics(metrics, service, from_dt, to_dt)
        logger.info(
            "GET /metrics service=%s from=%s to=%s -> rows=%s",
            service,
            from_ts,
            to_ts,
            len(filtered),
        )
        return {"metrics": [metric.as_output_dict() for metric in filtered]}

    @app.get("/summary")
    def summary() -> dict[str, object]:
        report = _get_report_or_raise(app)
        metrics = _get_metrics_or_raise(app)
        return {
            "raw_events": report.raw_events,
            "cleaned_events": report.cleaned_events,
            "dropped_events": report.dropped_events,
            "dropped_by_reason": report.dropped_by_reason,
            "metrics_rows": len(metrics),
        }

    return app


def _filter_metrics(
    metrics: list[ServiceMinuteMetric],
    service: str | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
) -> list[ServiceMinuteMetric]:
    out: list[ServiceMinuteMetric] = []

    for metric in metrics:
        if service and metric.service != service:
            continue

        minute_dt = datetime.fromisoformat(metric.minute.replace("Z", "+00:00"))

        if from_dt and minute_dt < from_dt:
            continue
        if to_dt and minute_dt > to_dt:
            continue

        out.append(metric)

    return out


def _parse_optional_iso_ts(raw: str | None, field_name: str) -> datetime | None:
    if raw is None:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ISO-8601 timestamp for '{field_name}'",
        ) from exc


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
