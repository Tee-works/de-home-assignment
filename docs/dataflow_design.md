# Production Design: Apache Beam / Dataflow

This document maps the local batch pipeline to a production-grade streaming architecture using
Apache Beam on Google Cloud Dataflow.

---

## Architecture Overview

```
Pub/Sub (raw-events)
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│  Dataflow Streaming Job                                 │
│                                                         │
│  1. Read from Pub/Sub                                   │
│  2. Parse JSON  ──── malformed ──► DLQ topic            │
│  3. Validate + Normalize fields                         │
│  4. Deduplicate by event_id (stateful, TTL)             │
│  5. Branch:                                             │
│     ├── Cleaned events ────────► BigQuery (raw table)   │
│     └── Windowed aggregation ──► BigQuery (metrics)     │
│                                 BigTable (hot path)     │
│  6. Quality counters ──────────► Cloud Monitoring       │
└─────────────────────────────────────────────────────────┘
```

## Pipeline stages

1. Read events from Pub/Sub topic (`raw-events`).
2. Parse JSON and route malformed payloads to a dead-letter topic (`raw-events-dlq`).
3. Validate and normalize fields:
   - required fields: `event_id`, `timestamp`, `service`, `event_type`, `status_code`
   - timestamp normalization to UTC
   - status code normalization (`"500"` -> `500`)
   - optional latency normalization when numeric
4. Deduplicate by `event_id` using stateful processing with TTL.
5. Windowed aggregation by `service` and 1-minute event-time windows:
   - `request_count`
   - `average_latency_ms`
   - `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`
   - `error_rate`
6. Write outputs:
   - cleaned events -> BigQuery raw_clean table
   - per-minute metrics -> BigQuery metrics table
   - optional low-latency serving copy -> Bigtable

---

## Stage-by-Stage Breakdown

### Stage 1: Read from Pub/Sub

```python
events = (
    pipeline
    | "ReadPubSub" >> beam.io.ReadFromPubSub(
        topic="projects/{project}/topics/raw-events",
        with_attributes=True,
    )
)
```

- Messages carry a `publish_time` attribute used as the event-time watermark seed if the payload
  timestamp is missing or unparseable.
- `with_attributes=True` preserves Pub/Sub metadata (message ID, publish time) for deduplication
  and observability.

### Stage 2: Parse JSON and Route Failures

```python
parsed, failures = (
    events
    | "ParseJSON" >> beam.ParDo(ParseJsonFn()).with_outputs("failures", main="parsed")
)

failures | "WriteDLQ" >> beam.io.WriteToPubSub(
    topic="projects/{project}/topics/raw-events-dlq"
)
```

- Unparseable JSON is written to the dead-letter queue (DLQ) with the raw bytes and error reason.
- DLQ messages are retained for manual inspection and replay.
- Parse failures are emitted as metrics to Cloud Monitoring.

### Stage 3: Validate and Normalize

Same logic as `clean_event()` in `pipeline.py`, implemented as a Beam `DoFn`:

```python
class ValidateAndNormalizeFn(beam.DoFn):
    def process(self, element):
        cleaned, meta = clean_event(element)
        if cleaned is None:
            self.drop_counter.inc()          # Cloud Monitoring metric
            yield beam.pvalue.TaggedOutput("dropped", (meta["drop_reason"], element))
        else:
            yield cleaned
```

- Dropped records (bad timestamp, bad status code, etc.) go to a `dropped` side output.
- Dropped records are written to BigQuery `pipeline_quality_daily` for audit trails.
- Normalization counters (timestamp, status code) increment Cloud Monitoring custom metrics.

### Stage 4: Stateful Deduplication by event_id

```python
class DeduplicateFn(beam.DoFn):
    SEEN_STATE = ReadModifyWriteStateSpec("seen", BooleanCoder())
    EXPIRY_TIMER = TimerSpec("expiry", TimeDomain.REAL_TIME)

    def process(self, element, seen=beam.DoFn.StateParam(SEEN_STATE),
                expiry=beam.DoFn.TimerParam(EXPIRY_TIMER)):
        if seen.read():
            return  # duplicate — drop silently, increment metric
        seen.write(True)
        expiry.set(Timestamp.now() + Duration(seconds=86400))  # 24h TTL
        yield element

    @on_timer(EXPIRY_TIMER)
    def expire(self, seen=beam.DoFn.StateParam(SEEN_STATE)):
        seen.clear()
```

Key decisions:
- **Key by `event_id`** so state is co-located with the event.
- **24h TTL** — covers reasonable redelivery windows without unbounded state growth.
- **Real-time timer** (not event-time) — deduplication is about message identity, not event ordering.
- First occurrence wins. If out-of-order delivery causes a late duplicate to arrive first, the
  originally-intended record is the one that arrives second and gets dropped. This is acceptable:
  the data content is identical by definition.

### Stage 5a: Write Cleaned Events to BigQuery

```python
cleaned_events | "WriteCleanedEvents" >> beam.io.WriteToBigQuery(
    table="analytics.raw_events_cleaned",
    schema=RAW_EVENTS_CLEANED_SCHEMA,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
    method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
)
```

- `STORAGE_WRITE_API` (BigQuery Storage Write API) provides exactly-once semantics and higher
  throughput than the legacy streaming insert API.
- Partitioned by `DATE(event_ts)`, clustered by `(service, event_type)` — aligns with common
  query patterns (date-range + service filter).

### Stage 5b: Windowed Aggregation

```python
metrics = (
    cleaned_events
    | "AssignEventTime" >> beam.Map(lambda e: beam.window.TimestampedValue(e, e.timestamp))
    | "FixedWindows" >> beam.WindowInto(
        beam.window.FixedWindows(60),           # 1-minute windows
        trigger=AfterWatermark(
            early=AfterCount(100),              # speculative early firing every 100 events
            late=AfterCount(1),                 # re-fire on every late event
        ),
        allowed_lateness=Duration(seconds=600), # 10-minute late data tolerance
        accumulation_mode=AccumulationMode.ACCUMULATING,
    )
    | "GroupByServiceMinute" >> beam.GroupBy(lambda e: (e.service, ...))
    | "ComputeMetrics" >> beam.ParDo(AggregateMetricsFn())
    | "WriteMetricsBQ" >> beam.io.WriteToBigQuery(...)
)
```

#### Trigger strategy

| Firing | When | Why |
|---|---|---|
| Early (speculative) | Every 100 events | Low-latency visibility before window closes |
| On-time | When watermark passes window end | The "official" result |
| Late | On every late-arriving event | Corrects the metric as delayed data arrives |

`AccumulationMode.ACCUMULATING` means each late firing replaces the previous result for that
window. Combined with BigQuery MERGE or upsert on `(minute_ts, service)`, this ensures the
metric table always reflects the most up-to-date aggregate.

#### Allowed lateness

10 minutes balances two concerns:
- Too short: late events are dropped, metrics under-count during network delays or producer lag.
- Too long: Dataflow must hold window state in memory longer, increasing cost and latency.

10 minutes covers the 99th percentile of Pub/Sub delivery latency in most GCP deployments.

### Stage 5c: Write Metrics to Bigtable (Hot Path)

```python
metrics | "WriteBigtable" >> beam.ParDo(WriteBigtableFn(
    project_id=PROJECT_ID,
    instance_id="metrics-serving",
    table_id="service_metrics",
))
```

Row key design: `{service}#{reverse_timestamp}` where `reverse_timestamp = MAX_LONG - epoch_millis`

This makes the most recent entries for a service appear first in a row scan, enabling
sub-millisecond p99 lookups for the API's hot path without scanning BigQuery.

---

## Late-Arriving Events

This is a multi-layer approach in production:

1. **Allowed lateness** — the pipeline tolerates events up to 10 minutes past the window watermark.
   Events within this window are accumulated into the existing result and trigger a corrected metric.

2. **Beyond allowed lateness** — events that arrive more than 10 minutes late are dropped from the
   streaming path. They are written to a `late_events` side output and land in a BigQuery
   `raw_events_late` table.

3. **Scheduled backfill** — a separate daily Cloud Composer (Airflow) DAG scans
   `raw_events_late` for any records from the past 7 days and re-runs the aggregation using a
   batch Beam job. The corrected metrics upsert into `service_metrics_minute` using BigQuery MERGE.

4. **Monitoring** — a Cloud Monitoring alert fires when the late event rate exceeds 1% of total
   volume in any 5-minute window, which signals upstream producer lag requiring investigation.

This three-layer design (window tolerance → side output → scheduled backfill) means:
- Real-time dashboards are approximately correct within seconds.
- Historical metrics are exactly correct within 24 hours.
- No data is ever permanently lost.

---

## Watermark Strategy

The pipeline uses **event-time watermarks** derived from the `timestamp` field in each event.

In practice, Pub/Sub does not advance the event-time watermark automatically — Dataflow infers it
from the minimum observed event timestamp across all shards. To prevent stalled watermarks from
idle shards, configure:

```python
beam.io.ReadFromPubSub(...).with_output_types(bytes)
# + custom watermark estimator or use pubsub_message_attributes for event time
```

Alternatively, use **processing-time watermarks** for the streaming path (acceptable for
near-real-time dashboards) and reconcile against event-time in the nightly backfill.

---

## Deduplication Strategy

| Layer | Mechanism | Coverage |
|---|---|---|
| Streaming (Beam) | Stateful `seen` flag keyed by `event_id`, 24h TTL | Same-day duplicates |
| BigQuery load | `INSERT IF NOT EXISTS` / MERGE on `event_id` | Cross-day or replay duplicates |
| Metrics | Dedup before aggregation so counts are accurate | All |

Duplicate events that survive the streaming dedup (e.g. exact same `event_id` sent 25 hours apart)
are caught at the BigQuery layer by a daily integrity check that counts distinct `event_id` values
per partition.

---

## Error Handling and Replay

| Scenario | Handling |
|---|---|
| Malformed JSON | Written to DLQ topic; ops alerted; manual inspect + replay |
| Missing required field | Dropped with reason counter; written to `raw_events_dropped` |
| Downstream BigQuery outage | Beam buffers in runner memory up to `max_buffering_duration`; auto-retries |
| Complete pipeline failure | Re-read from Pub/Sub using seek-to-timestamp or snapshot |
| Late events beyond window | Side output to `raw_events_late`; nightly backfill corrects metrics |

Replays use Pub/Sub's `seek` API to re-deliver from a snapshot taken before the incident:

```bash
gcloud pubsub subscriptions seek raw-events-sub \
  --snapshot=pre-incident-snapshot
```

---

## Operational Signals

### SLOs

| Signal | Target | Alert threshold |
|---|---|---|
| End-to-end latency (publish → BQ) | p99 < 90s | > 120s for 5 min |
| Drop rate | < 5% of daily volume | > 10% in any 1h window |
| Late event rate | < 1% of daily volume | > 1% in any 5 min window |
| Watermark lag | < 2 minutes | > 5 minutes for 10 min |

### Dashboards

- **Throughput**: input events/sec, output events/sec, drop events/sec
- **Quality**: drop rate by reason (stacked), duplicate rate, normalization rate
- **Freshness**: watermark lag, event-time to process-time skew (p50/p95/p99)
- **Cost**: Dataflow worker CPU/memory, autoscaling events, BigQuery slot usage

### Alerting

```yaml
# Cloud Monitoring alert policy (conceptual)
conditions:
  - display_name: "High drop rate"
    condition_threshold:
      filter: metric.type="custom.googleapis.com/pipeline/drop_rate"
      threshold_value: 0.10
      duration: 300s

  - display_name: "Watermark stalled"
    condition_threshold:
      filter: metric.type="dataflow.googleapis.com/job/element_count"
      # ... watermark advancement check
      duration: 600s
```

---

## Cost Considerations

| Component | Cost driver | Optimization |
|---|---|---|
| Dataflow workers | Worker count × hours | Autoscaling; use Spot VMs for non-critical windows |
| BigQuery storage | Bytes stored | Partition expiry: keep raw events 90 days, metrics 2 years |
| BigQuery queries | Bytes scanned | Clustering by `(service, event_type)` reduces scan by ~80% on typical queries |
| Bigtable | Node count | Scale down during off-peak; use SSD only for hot-path tables |
| Pub/Sub | Message volume | Filter client-side before publish; compress large payloads |

---

## Scalability Notes

- This pipeline is designed for **horizontal scale**: adding Dataflow workers increases throughput
  linearly up to Pub/Sub subscription throughput limits (~10 GB/s per subscription).
- Stateful deduplication is the bottleneck at very high cardinality. At >10M unique `event_id`
  values per hour, consider sharding the dedup step or switching to approximate dedup using a
  Bloom filter with a configurable false-positive rate.
- Windowed aggregation is embarrassingly parallel by `(service, minute)` key. No cross-shard
  coordination is needed until the final write.
