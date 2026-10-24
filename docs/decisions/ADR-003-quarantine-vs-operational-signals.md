# ADR-003: Quarantine corrupt data, but never quarantine alarming data

## Status

Accepted

## Context

A cleanroom at 29 °C with a 26 °C limit is *valid data describing a real
problem*. Discarding it as "bad data" would hide exactly the conditions the
platform exists to detect. Conversely, a humidity of 140% is physically
impossible and must never reach analytics.

## Decision

Two disjoint rule families:

1. **Hard validity rules** (`quality/rules.py`) — structural and physical
   impossibility. Failures are quarantined with explainable error codes and the
   original payload. Nothing is silently dropped.
2. **Operational signals** (`quality/signals.py`) — valid measurements that
   violate facility limits or contradict equipment state
   (`ENVIRONMENTAL_LIMIT_VIOLATION`, `EQUIPMENT_STATE_INCONSISTENT`,
   `SENSOR_STALE`, `SEQUENCE_GAP`). The event continues into Silver; the signal
   feeds health/compliance outputs.

Freshness is computed from the *last observed event*, never as a quarantine
rule: when a sensor dies, no event exists to reject.

Advisory recommendations are generated only when inputs pass hard rules, are
fresh, and satisfy all safety checks — so no recommendation can be produced
from invalid input.

## Consequences

- Compliance duration metrics can be computed from Silver directly.
- Quarantine volume measures data-pipeline health; signal volume measures
  physical-plant health. Mixing them would corrupt both.
