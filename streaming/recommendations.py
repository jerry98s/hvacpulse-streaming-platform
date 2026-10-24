"""Advisory recommendation rules and safety gating (pure Python, no Spark).

A recommendation is produced only when *all* gates pass: the device is
commissioned, data is fresh, enough supporting readings exist, no recent
quarantine failures, no active equipment-health signals, and the rule version
is known. Recommendation IDs are deterministic so reprocessing a window
re-derives the same identity (see ``streaming.idempotency.recommendation_id``).

The Spark job (``streaming.jobs.recommendations_job``) aggregates with Spark,
then evaluates these rules on the driver — rule evaluation is deliberately off
the hot path so there is exactly one implementation to test.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone

from quality.signals import is_stale
from streaming.idempotency import recommendation_id

RULE_VERSION = "rules-v1"
ADVISORY = "ADVISORY"

RAISE_SETPOINT = "RAISE_SETPOINT"
INCREASE_COOLING = "INCREASE_COOLING"
REDUCE_FAN_SPEED = "REDUCE_FAN_SPEED"

OVERCOOLING_DELTA_C = -1.0
UNDERCOOLING_DELTA_C = 1.0
COMFORTABLE_DELTA_C = 0.5
HIGH_FAN_SPEED_PCT = 80.0

RECOMMENDATION_REVOKED = "RECOMMENDATION_REVOKED"
SOURCE_METRIC_RESTATED = "SOURCE_METRIC_RESTATED"


@dataclass(frozen=True)
class WindowReading:
    """Per-device window aggregate used as rule input."""

    device_id: str
    site_id: str
    window_start: str
    window_end: str
    event_count: int
    avg_zone_temp_c: float | None
    avg_setpoint_c: float | None
    avg_fan_speed_pct: float | None
    avg_power_kw: float | None
    equipment_running: bool
    max_event_time: str


@dataclass(frozen=True)
class GateContext:
    """Safety inputs that are not part of the window aggregate itself."""

    now: datetime
    expected_interval_sec: float
    quarantine_errors_recent: int
    active_signals: tuple[str, ...]
    commissioning_status: str
    min_readings: int = 3
    stale_factor: float = 3.0


def gating_failures(reading: WindowReading, ctx: GateContext) -> list[str]:
    """Return every reason a recommendation must be blocked (empty = safe)."""
    failures: list[str] = []
    if ctx.commissioning_status != "ACTIVE":
        failures.append("EQUIPMENT_NOT_COMMISSIONED")
    if reading.event_count < ctx.min_readings:
        failures.append("INSUFFICIENT_SUPPORTING_READINGS")
    if ctx.quarantine_errors_recent > 0:
        failures.append("RECENT_DATA_QUALITY_FAILURES")
    if ctx.active_signals:
        failures.append("EQUIPMENT_UNHEALTHY")
    if is_stale(
        reading.max_event_time,
        now=ctx.now,
        expected_interval_sec=ctx.expected_interval_sec,
        stale_factor=ctx.stale_factor,
    ):
        failures.append("STALE_DATA")
    return failures


def evaluate(reading: WindowReading, ctx: GateContext) -> dict | None:
    """Evaluate advisory rules for one device window; None when blocked/no-op."""
    if gating_failures(reading, ctx):
        return None
    if reading.avg_zone_temp_c is None or reading.avg_setpoint_c is None:
        return None
    if not reading.equipment_running:
        return None

    delta = reading.avg_zone_temp_c - reading.avg_setpoint_c
    rationale: dict = {"zone_minus_setpoint_c": round(delta, 3)}
    rec_type: str | None = None
    if delta <= OVERCOOLING_DELTA_C:
        rec_type = RAISE_SETPOINT
        rationale["reason"] = "zone overcooled; raising setpoint saves energy"
    elif delta >= UNDERCOOLING_DELTA_C:
        rec_type = INCREASE_COOLING
        rationale["reason"] = "zone above setpoint; increase cooling output"
    elif (
        abs(delta) < COMFORTABLE_DELTA_C
        and reading.avg_fan_speed_pct is not None
        and reading.avg_fan_speed_pct >= HIGH_FAN_SPEED_PCT
    ):
        rec_type = REDUCE_FAN_SPEED
        rationale["reason"] = "zone comfortable with high fan speed; reduce fan"

    if rec_type is None:
        return None
    return {
        "recommendation_id": recommendation_id(
            reading.device_id, reading.window_end, rec_type, RULE_VERSION
        ),
        "device_id": reading.device_id,
        "site_id": reading.site_id,
        "window_start": reading.window_start,
        "window_end": reading.window_end,
        "recommendation_type": rec_type,
        "rule_version": RULE_VERSION,
        "mode": ADVISORY,
        "rationale": rationale,
    }


def build_revocation(
    original_recommendation_id: str,
    *,
    reason: str = SOURCE_METRIC_RESTATED,
    replacement_recommendation_id: str | None = None,
    now: datetime | None = None,
) -> dict:
    """Compensating event: Kafka records cannot be retracted, so we revoke."""
    return {
        "event_type": RECOMMENDATION_REVOKED,
        "recommendation_id": original_recommendation_id,
        "revoked_at": (now or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z"),
        "reason": reason,
        "replacement_recommendation_id": replacement_recommendation_id,
    }
