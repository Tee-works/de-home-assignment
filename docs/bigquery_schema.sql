-- Raw cleaned events table
CREATE TABLE IF NOT EXISTS analytics.raw_events_cleaned (
  event_id STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  service STRING NOT NULL,
  event_type STRING NOT NULL,
  latency_ms INT64,
  status_code INT64 NOT NULL,
  user_id STRING,
  ingest_ts TIMESTAMP NOT NULL,
  transform_version STRING NOT NULL,
  source_payload JSON
)
PARTITION BY DATE(event_ts)
CLUSTER BY service, event_type;

-- Aggregated per-minute metrics table
CREATE TABLE IF NOT EXISTS analytics.service_metrics_minute (
  minute_ts TIMESTAMP NOT NULL,
  service STRING NOT NULL,
  request_count INT64 NOT NULL,
  average_latency_ms FLOAT64,
  p50_latency_ms FLOAT64,
  p95_latency_ms FLOAT64,
  p99_latency_ms FLOAT64,
  error_rate FLOAT64 NOT NULL,
  window_start_ts TIMESTAMP NOT NULL,
  window_end_ts TIMESTAMP NOT NULL,
  transform_version STRING NOT NULL,
  computed_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(minute_ts)
CLUSTER BY service;

-- Optional quality summary table for monitoring/audits
CREATE TABLE IF NOT EXISTS analytics.pipeline_quality_daily (
  metric_date DATE NOT NULL,
  transform_version STRING NOT NULL,
  raw_events INT64 NOT NULL,
  cleaned_events INT64 NOT NULL,
  dropped_events INT64 NOT NULL,
  dropped_missing_timestamp INT64 NOT NULL,
  dropped_missing_status_code INT64 NOT NULL,
  dropped_missing_service INT64 NOT NULL,
  dropped_invalid_timestamp INT64 NOT NULL,
  dropped_invalid_status_code INT64 NOT NULL,
  dropped_duplicate_event_id INT64 NOT NULL,
  created_ts TIMESTAMP NOT NULL
)
PARTITION BY metric_date;
