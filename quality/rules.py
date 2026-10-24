"""Hard data-validity rules. A rule failure quarantines the event.

This module is the portable reference implementation (pure Python, no Spark).
The streaming pipeline mirrors the same codes and precedence in
``streaming.transforms.with_validation`` using Spark SQL expressions, so local
unit tests and the production path cannot silently drift apart in meaning.

Quarantine error codes (README, "Quarantine"):
    SCHEMA_INVALID, UNSUPPORTED_SCHEMA_VERSION, EQUIPMENT_UNKNOWN,
    TEMPERATURE_PHYSICALLY_IMPOSSIBLE, HUMIDITY_MISSING, NEGATIVE_POWER,
    EVENT_TOO_LATE, IDEMPOTENCY_CONFLICT, UNIT_CONVERSION_FAILED

Refinements used where the README names a rule but not a code:
    HUMIDITY_OUT_OF_RANGE, FAN_VALVE_OUT_OF_RANGE, TIMESTAMP_INVALID
"""

from __future__ import annotations

import math
from datetime import datetime
from datetime import timedelta
from datetime import timezone

SCHEMA_INVALID = "SCHEMA_INVALID"
UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
EQUIPMENT_UNKNOWN = "EQUIPMENT_UNKNOWN"
TEMPERATURE_PHYSICALLY_IMPOSSIBLE = "TEMPERATURE_PHYSICALLY_IMPOSSIBLE"
HUMIDITY_MISSING = "HUMIDITY_MISSING"
HUMIDITY_OUT_OF_RANGE = "HUMIDITY_OUT_OF_RANGE"
FAN_VALVE_OUT_OF_RANGE = "FAN_VALVE_OUT_OF_RANGE"
NEGATIVE_POWER = "NEGATIVE_POWER"
EVENT_TOO_LATE = "EVENT_TOO_LATE"
IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
UNIT_CONVERSION_FAILED = "UNIT_CONVERSION_FAILED"
TIMESTAMP_INVALID = "TIMESTAMP_INVALID"

SUPPORTED_SCHEMA_VERSIONS = frozenset({1, 2})

# Outside this band a reading cannot be a real air-side temperature sensor value.
PHYSICAL_TEMP_MIN_C = -60.0
PHYSICAL_TEMP_MAX_C = 100.0

TEMPERATURE_FIELDS = ("supply_air_temp_c", "return_air_temp_c", "zone_temp_c")
PERCENT_FIELDS = ("fan_speed_pct", "chilled_water_valve_pct")

REQUIRED_ENVELOPE_FIELDS = (
    "event_id",
    "logical_event_id",
    "revision_number",
    "schema_id",
    "schema_version",
    "firmware_version",
    "event_time",
    "gateway_id",
    "device_id",
    "boot_id",
    "sequence_number",
    "site_id",
    "zone_id",
    "measurements",
    "operating_state",
)

MAX_EVENT_LATENESS = timedelta(days=7)


def parse_event_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _is_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def validate_event(
    event: object,
    *,
    now: datetime | None = None,
    known_devices: set[str] | None = None,
    max_lateness: timedelta = MAX_EVENT_LATENESS,
) -> str | None:
    """Return the first hard-failure error code for an event, or None if valid.

    Rule order is deliberate: structural → version → identity → timestamp →
    measurement typing → physical plausibility, so quarantine reasons are
    explainable and stable.
    """
    if not isinstance(event, dict):
        return SCHEMA_INVALID

    for field in REQUIRED_ENVELOPE_FIELDS:
        if event.get(field) is None:
            return SCHEMA_INVALID

    if event["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS:
        return UNSUPPORTED_SCHEMA_VERSION

    if known_devices is not None and event["device_id"] not in known_devices:
        return EQUIPMENT_UNKNOWN

    event_time = parse_event_time(event["event_time"])
    if event_time is None:
        return TIMESTAMP_INVALID
    now = now or datetime.now(timezone.utc)
    if now - event_time > max_lateness:
        return EVENT_TOO_LATE

    measurements = event["measurements"]
    if not isinstance(measurements, dict):
        return SCHEMA_INVALID
    for value in measurements.values():
        if value is not None and not _is_number(value):
            return SCHEMA_INVALID

    humidity = measurements.get("relative_humidity_pct")
    if humidity is None:
        return HUMIDITY_MISSING
    if not 0.0 <= humidity <= 100.0:
        return HUMIDITY_OUT_OF_RANGE

    power = measurements.get("power_kw")
    if power is not None and power < 0:
        return NEGATIVE_POWER

    for field in PERCENT_FIELDS:
        value = measurements.get(field)
        if value is not None and not 0.0 <= value <= 100.0:
            return FAN_VALVE_OUT_OF_RANGE

    for field in TEMPERATURE_FIELDS:
        value = measurements.get(field)
        if value is not None and not PHYSICAL_TEMP_MIN_C <= value <= PHYSICAL_TEMP_MAX_C:
            return TEMPERATURE_PHYSICALLY_IMPOSSIBLE

    return None


def normalize_units(event: dict) -> tuple[dict, str | None]:
    """Convert legacy Fahrenheit payloads to canonical Celsius.

    Firmware that reports in Fahrenheit marks ``measurements.temperature_unit``
    as ``"F"``. Conversion failures quarantine with UNIT_CONVERSION_FAILED.
    """
    measurements = event.get("measurements")
    if not isinstance(measurements, dict):
        return event, SCHEMA_INVALID
    unit = measurements.get("temperature_unit", "C")
    if unit == "C":
        return event, None
    if unit != "F":
        return event, UNIT_CONVERSION_FAILED

    converted = dict(event)
    new_measurements = dict(measurements)
    for field in TEMPERATURE_FIELDS:
        value = new_measurements.get(field)
        if value is None:
            continue
        if not _is_number(value):
            return event, UNIT_CONVERSION_FAILED
        new_measurements[field] = round((value - 32.0) * 5.0 / 9.0, 4)
    setpoint = event.get("operating_state", {}).get("temperature_setpoint_c")
    if setpoint is not None:
        if not _is_number(setpoint):
            return event, UNIT_CONVERSION_FAILED
        converted["operating_state"] = dict(event["operating_state"])
        converted["operating_state"]["temperature_setpoint_c"] = round(
            (setpoint - 32.0) * 5.0 / 9.0, 4
        )
    new_measurements.pop("temperature_unit", None)
    converted["measurements"] = new_measurements
    return converted, None
