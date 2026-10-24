"""Pydantic models for the structural shape of a telemetry event.

These models check *structure* (required envelope fields, types) only. Business
validity — physical plausibility, freshness, equipment identity — lives in
``quality.rules`` so that structurally valid but physically impossible events
can still be modelled here and quarantined downstream with precise error codes.
"""

from __future__ import annotations

from pydantic import BaseModel
from pydantic import ConfigDict


class Measurements(BaseModel):
    model_config = ConfigDict(extra="allow")

    supply_air_temp_c: float | None = None
    return_air_temp_c: float | None = None
    zone_temp_c: float | None = None
    relative_humidity_pct: float | None = None
    airflow_m3h: float | None = None
    static_pressure_pa: float | None = None
    fan_speed_pct: float | None = None
    chilled_water_valve_pct: float | None = None
    power_kw: float | None = None
    co2_ppm: float | None = None


class OperatingState(BaseModel):
    mode: str
    temperature_setpoint_c: float | None = None
    equipment_running: bool


class TelemetryEvent(BaseModel):
    event_id: str
    logical_event_id: str
    revision_number: int = 1
    supersedes_event_id: str | None = None
    schema_id: int
    schema_version: int
    firmware_version: str
    event_time: str
    gateway_id: str
    device_id: str
    boot_id: str
    sequence_number: int
    site_id: str
    zone_id: str
    measurements: Measurements
    operating_state: OperatingState


def validate_structural(event: dict) -> TelemetryEvent:
    """Parse a raw event dict into the structural model, raising on bad shape."""
    return TelemetryEvent.model_validate(event)
