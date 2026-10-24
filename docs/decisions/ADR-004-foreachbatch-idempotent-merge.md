# ADR-004: foreachBatch + Delta MERGE as the Silver idempotency boundary

## Status

Accepted

## Context

Streaming state dedup (`dropDuplicatesWithinWatermark`) only remembers keys
inside the watermark window. A retry arriving after the watermark expired would
re-insert unless the sink itself is idempotent.

## Decision

Layered dedup, implemented in `streaming/jobs/silver_transform.py`:

1. `withWatermark(ingest_ts, 10 minutes)` +
   `dropDuplicatesWithinWatermark(event_id, payload_hash)` removes recent
   exact retries in streaming state. The watermark bounds state by arrival
   time; event-time lateness is the separate `EVENT_TOO_LATE` quarantine rule.
2. Inside `foreachBatch`, events whose `event_id` carries multiple distinct
   payload hashes *within the batch* are quarantined as `IDEMPOTENCY_CONFLICT`.
3. The remaining batch is classified against `silver_telemetry_history`
   (`split_batch_against_history`): INSERT / SAFE RETRY (no-op) /
   IDEMPOTENCY_CONFLICT, then written through an insert-only Delta `MERGE`.

Because history is keyed by `event_id`, a retry outside the watermark still
matches an existing row with the same hash and becomes a no-op.

Gold follows the same principle one level up: complete-window results are
merged into `gold_..._current` (key `site_id + device_id + window_start`)
rather than incrementally added, and every published version is appended once
to `gold_..._history` with a `metric_version`. A changed FINAL window is
republished as RESTATED; history keeps the prior version for audit.

## Consequences

- Replay of the same Kafka range yields zero new Silver rows and unchanged Gold
  metrics (README acceptance criteria).
- `silver_telemetry_current` is derivable from history at any time and is
  rebuilt deterministically after each batch.
- On Azure Databricks the same job runs as a scheduled task over the Lakeflow
  Bronze streaming table, with its checkpoint on a Unity Catalog volume, so
  each run reads only new Bronze rows. Lakeflow owns Bronze only.
