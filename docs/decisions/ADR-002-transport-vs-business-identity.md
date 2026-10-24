# ADR-002: Separate transport identity from business-event identity

## Status

Accepted

## Context

Gateway retries and pipeline restarts both produce duplicates. Using only
`sensor_id + event_time` as a dedup key is unsafe: events can share a
timestamp, device clocks drift or reset, and sequence numbers restart on boot.

## Decision

- **Transport identity** = Kafka topic + partition + offset. Stored on every
  Bronze row; used for ingestion traceability and replay. Bronze is append-only
  and never upserts on business keys.
- **Business identity** = `event_id` (gateway-generated), falling back to a
  deterministic hash of `gateway_id + device_id + boot_id + sequence_number`.
- Every payload gets a `payload_hash` (sha256 of raw bytes at Bronze). Dedup
  and merge decisions are made on `(event_id, payload_hash)`:
  - new `event_id` → INSERT
  - same hash → SAFE RETRY, no-op
  - different hash → `IDEMPOTENCY_CONFLICT` quarantine
- Corrections receive a *new* `event_id`, keep a stable `logical_event_id`,
  increment `revision_number`, and reference the original via
  `supersedes_event_id`. Silver history keeps every revision; Silver current
  selects the highest revision per logical id.

## Consequences

- Retried micro-batches and at-least-once producers cannot double-count.
- Conflicting redeliveries are detected and quarantined instead of silently
  dropping one of the two payloads.
- `streaming/idempotency.py` is the pure-Python reference; the Silver job
  implements the same classification with a Delta `MERGE` in `foreachBatch`.
