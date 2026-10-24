# ADR-005: Advisory recommendations with safety gating and revocation events

## Status

Accepted

## Context

The platform surfaces optimization opportunities (overcooling, excessive fan
speed) but must never issue a physical control action, and must never emit a
recommendation from data that is stale, low-quality, or from unhealthy
equipment. Kafka records also cannot be retracted, so a recommendation based
on a Gold window that is later restated needs an explicit compensating event.

## Decision

Implemented in `streaming/recommendations.py` (pure rules) and
`streaming/jobs/recommendations_job.py` (Spark aggregation + driver-side rule
evaluation):

1. **Advisory only.** Every recommendation carries `mode = ADVISORY`. There is
   no write path back to any control system.
2. **Safety gates (all must pass):** device commissioned (`ACTIVE`), latest
   event fresh (< 3x expected report interval), at least 3 supporting readings
   in the window, zero quarantine errors for the device in the last 5 minutes,
   no active equipment-state inconsistency signals, and a known
   `rule_version`. `gating_failures()` returns every blocking reason.
3. **Deterministic identity.** `recommendation_id(device_id, window_end, type,
   rule_version)` — reprocessing the same window re-derives the same ID, so
   the Gold write is an insert-only merge.
4. **Revocation, not retraction.** When a source Gold window is republished as
   RESTATED, the original recommendation is marked REVOKED and a
   `RECOMMENDATION_REVOKED` compensating event is appended to
   `recommendation_events`, referencing a replacement recommendation when the
   corrected data still passes every gate.
5. **Rule evaluation off the hot path.** Spark computes per-device 5-minute
   aggregates; the small aggregate set is collected to the driver and evaluated
   with the pure functions, so there is exactly one tested implementation.

## Consequences

- Recommendations are reproducible and safe to recompute; replays add no
  duplicates.
- Restatement of a metric automatically cascades to revocation of any
  recommendation derived from it, keeping advice consistent with the best
  available data.
- Rule changes are versioned (`rule_version`), so old recommendations remain
  attributable to the logic that produced them.
