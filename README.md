# Data Engineering Home Assignment

This repository contains an end-to-end Python solution for the assignment in `HomeAssignment.pdf`.

- Input dataset: `HomeAssignmentEvents.jsonl`
- Pipeline outputs:
  - `outputs/cleaned_events.jsonl`
  - `outputs/metrics_per_service_per_minute.csv`
  - `outputs/cleaning_report.json`
- API: FastAPI app exposing processed metrics
- Tests: pytest unit/integration coverage for pipeline + API

## 1. How to run

### Prerequisites
- Python 3.11+

### Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Run the transformation pipeline
```bash
PYTHONPATH=src python3 scripts/run_pipeline.py
```

### Run API
```bash
PYTHONPATH=src python3 scripts/run_api.py
```

### Run tests
```bash
PYTHONPATH=src python3 -m pytest
```

### Run quality gate checks
```bash
PYTHONPATH=src python3 scripts/run_quality_gate.py
```

### Optional lint/type checks
```bash
PYTHONPATH=src python3 -m ruff check .
PYTHONPATH=src python3 -m mypy src
```

## 2. Part 1 - Data ingestion and transformation

### Cleaning and validation rules

I made the following explicit decisions for imperfect records:

1. **Drop event if any critical field is missing/invalid**:
   - Required fields: `event_id`, `timestamp`, `service`, `event_type`, `status_code`
   - Reasons:
     - Without `timestamp` we cannot assign to a minute bucket
     - Without `service` we cannot compute service-level metrics
     - Without valid `status_code` we cannot compute error-rate correctly

2. **Normalize when safe**:
   - `timestamp`:
     - Accept ISO-8601 (`...Z` or `+00:00`)
     - Also accept a fallback legacy format (`%d/%m/%Y %H:%M:%S` and `%Y/%m/%d %H:%M:%S`)
     - Normalize all accepted timestamps to UTC ISO-8601 with `Z`
   - `status_code`:
     - Convert digit strings like `"500"` to integer `500`
     - Reject non-numeric values (e.g., `"ERR"`) and out-of-range values (not 100-599)
   - `latency_ms`:
     - Keep non-negative integers
     - Convert numeric strings (e.g., `"140"`) to int
     - Invalid/missing latency is set to `null` rather than dropping the full event

3. **Deduplication policy**:
   - Deduplicate by `event_id`, keep first valid occurrence, drop later duplicates
   - Rationale: `event_id` should represent one logical event and duplicate IDs cause double counting in aggregate metrics

4. **Extra fields**:
   - Unknown fields are ignored in the cleaned output schema to keep downstream contracts stable

### Aggregated metrics produced

Per **service + minute**:
- `request_count`
- `average_latency_ms` (computed from events with non-null latency)
- `p50_latency_ms`
- `p95_latency_ms`
- `p99_latency_ms`
- `error_rate` = `error_count / request_count`, where error means status code outside 200-299

### Results on provided dataset

From `outputs/cleaning_report.json`:

- Raw events: **2020**
- Cleaned events: **1945**
- Dropped events: **75**
- Dropped by reason:
  - `missing_timestamp`: 24
  - `missing_status_code`: 9
  - `missing_service`: 6
  - `invalid_timestamp`: 6
  - `invalid_status_code`: 10
  - `duplicate_event_id`: 20
- Normalized:
  - timestamps: 6
  - status codes: 2
  - latency: 0

Metrics output rows: **789**

## 3. Part 2 - Storage and modeling design (BigQuery / Bigtable)

### What goes where

1. **Raw events**
   - Primary store: **BigQuery raw table** (partition by event date, cluster by `service`, `event_type`)
   - Why:
     - Strong fit for analytics, replay/backfill workflows, and ad-hoc debugging
     - Simple SQL-based quality checks and downstream consumption by analytics/ML teams

2. **Aggregated metrics**
   - Primary analytical store: **BigQuery metrics table** (partition by minute/date, cluster by `service`)
   - Optional low-latency serving store: **Bigtable** for API-read patterns
   - Why:
     - BigQuery is best for historical analysis and batch-style consumers
     - Bigtable is better for very high-QPS point/range reads by key/time if API traffic grows

### Suggested Bigtable model for serving

- Row key: `service#YYYYMMDDHHMM`
- Columns:
  - `request_count`
  - `average_latency_ms`
  - `p50_latency_ms`
  - `p95_latency_ms`
  - `p99_latency_ms`
  - `error_rate`
- Supports efficient reads for `service + time range` endpoint shape

### Schema evolution approach

1. Prefer backward-compatible changes (add nullable columns, avoid destructive renames)
2. Versioned schemas/contracts in repo (with compatibility checks in CI)
3. Store raw payload plus parsed columns to preserve unknown/new fields
4. Track null-rate/type-rate drifts for new/changed fields and alert on sudden shifts
5. Use explicit transformation versions so backfills are reproducible

### Production mapping artifacts

- `docs/dataflow_design.md`: Beam/Dataflow stage design, windowing, dedupe, and observability plan
- `docs/bigquery_schema.sql`: BigQuery DDL for cleaned events, metrics, and quality audit tables

### Storage cost and retention trade-offs

1. Keep partition pruning strict (`DATE(event_ts)` / `DATE(minute_ts)`) so queries scan only relevant partitions.
2. Cluster by high-cardinality filters actually used by consumers (`service`, `event_type`) to reduce scanned bytes.
3. Use short retention or tiered retention for hot metrics tables (for example 30-90 days hot, long-term in cheaper storage/backups).
4. Avoid repeated full-table metric recomputation by writing incremental windows and backfilling only affected ranges.
5. Use Bigtable only when low-latency serving justifies operational cost; keep BigQuery as the analytical source of truth.

## 4. Part 3 - Simple data API

Implemented with FastAPI:

1. `GET /health`
   - Basic liveness endpoint

2. `GET /metrics?service=...&from=...&to=...`
   - Returns processed per-minute metrics
   - Query params:
     - `service` optional exact match
     - `from` optional ISO-8601
     - `to` optional ISO-8601
   - Validates timestamp input and returns HTTP 400 for invalid values

3. `GET /summary`
   - Returns cleaning statistics and metrics row count

### API examples

```bash
curl "http://localhost:8000/metrics?service=checkout&from=2025-01-12T10:00:00Z&to=2025-01-12T10:10:00Z"
```

Example response shape:

```json
{
  "metrics": [
    {
      "service": "checkout",
      "minute": "2025-01-12T10:00:00Z",
      "request_count": 2,
      "average_latency_ms": 137.5,
      "p50_latency_ms": 137.5,
      "p95_latency_ms": 137.5,
      "p99_latency_ms": 137.5,
      "error_rate": 0.5
    }
  ]
}
```

```bash
curl "http://localhost:8000/summary"
```

Example response shape:

```json
{
  "raw_events": 2020,
  "cleaned_events": 1945,
  "dropped_events": 75,
  "dropped_by_reason": {
    "duplicate_event_id": 20,
    "invalid_status_code": 10,
    "invalid_timestamp": 6,
    "missing_service": 6,
    "missing_status_code": 9,
    "missing_timestamp": 24
  },
  "metrics_rows": 789
}
```

## 5. Part 4 - Code quality and reliability

### Implemented in this submission

1. Lightweight data quality gate:
   - Validates required output artifacts exist
   - Validates report internal consistency (`raw`, `cleaned`, `dropped`, reason totals)
   - Validates metrics schema and value constraints
   - Validates `sum(request_count) == cleaned_events`
2. Minimal CI workflow:
   - Runs pipeline generation
   - Runs pytest suite
   - Runs ruff lint checks
   - Runs mypy type checks
   - Runs quality gate script
3. Structured logging in runners/modules:
   - Pipeline run logs input/output counts and drop reason summaries
   - API startup and query logs include filter parameters and returned row counts
   - Quality gate logs pass/fail and issue counts

### What I would monitor in production

1. **Ingestion quality**:
   - Invalid JSON rate
   - Drop rate by reason (missing timestamp/status/service, invalid types)
2. **Freshness and completeness**:
   - End-to-end lag (event time to availability)
   - Expected events/minute vs observed
3. **Metric quality**:
   - Error-rate and latency anomalies by service
   - Sudden null-rate spikes for critical fields
4. **API behavior**:
   - p95/p99 latency, error rate, request volume by endpoint/filter pattern

### Operations and monitoring plan

1. Define SLOs:
   - Data freshness SLO (event time to metric availability)
   - Pipeline success SLO (no failed production runs)
   - API availability/latency SLO
2. Alerting thresholds:
   - Drop-rate spike above baseline
   - Freshness lag above threshold
   - Error-rate anomaly per service
   - API 5xx or latency regression
3. Dashboard groups:
   - Ingestion quality dashboard
   - Pipeline throughput/freshness dashboard
   - API reliability dashboard
4. Incident response:
   - Route malformed payloads to DLQ with reason codes
   - Runbook for replay/backfill from retained raw history
   - Post-incident review updating quality gates and alerts

These operational patterns align with the production mapping in `docs/dataflow_design.md`.

### What could break at higher data volume

1. Single-process in-memory aggregation becomes a bottleneck
2. Full reload at API startup becomes too expensive
3. Local file outputs are not suitable for concurrent readers/writers or large history
4. Dedupe state by `event_id` in memory does not scale without bounded state/store

### Next improvements with more time

1. Move to incremental/streaming windowed aggregation with watermarking and late-event handling
2. Add idempotency/dedupe strategy with bounded state and TTL
3. Evolve the current quality gate into formal contracts (for example Great Expectations, schema registry compatibility checks, and producer-side contract tests)
4. Expand CI with lint/type checks and multi-version test matrix; add containerization for repeatable local/runtime parity
5. Adopt dbt in BigQuery to build/test/document Silver/Gold models and enforce transformation contracts.
6. If API moves to live datastore reads, adopt async I/O + pooling to scale concurrent requests.

## 6. Project layout

```text
src/de_home_assignment/pipeline.py   # Validation, normalization, aggregation, output writers
src/de_home_assignment/api.py        # FastAPI endpoints over processed metrics
src/de_home_assignment/quality_gate.py  # Output contract and quality checks
scripts/run_pipeline.py              # Pipeline runner (writes outputs/)
scripts/run_api.py                   # API runner
scripts/run_quality_gate.py          # Quality gate runner
tests/test_pipeline.py               # Pipeline tests
tests/test_api.py                    # API tests
tests/test_quality_gate.py           # Quality gate tests
tests/test_assignment_dataset_regression.py  # Full-dataset regression checks
tests/test_pipeline_artifacts.py     # Golden artifact contract checks
docs/dataflow_design.md              # Production design mapping to Beam/Dataflow
docs/bigquery_schema.sql             # BigQuery table definitions
.github/workflows/ci.yml             # CI: pipeline + tests + ruff + mypy + quality gate
outputs/*                            # Generated artifacts from assignment dataset
```

