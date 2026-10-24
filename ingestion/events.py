"""Deterministic HVAC telemetry event builders shared by the simulator and tests.

Events built here match the logical representation in the README and the Avro
contracts under ``contracts/avro/``. Business-level validity is *not* enforced
here — that is the job of ``quality.rules`` — so anomaly injectors can build
physically impossible but structurally well-formed events.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone


@dataclass(frozen=True)
class DeviceSpec:
    device_id: str
    site_id: str
    zone_id: str
    gateway_id: str
    firmware_version: str
    schema_version: int
    rated_power_kw: float


# Must stay in sync with reference/equipment_master.csv (except intentionally
# unknown devices, which only ever appear through anomaly injection).
DEFAULT_DEVICES: tuple[DeviceSpec, ...] = (
    DeviceSpec("ahu-001", "factory-kl-01", "cleanroom-a", "gateway-kl-01", "2.4.1", 1, 25.0),
    DeviceSpec("ahu-002", "factory-kl-01", "cleanroom-b", "gateway-kl-01", "2.4.1", 1, 25.0),
    DeviceSpec("ahu-101", "factory-penang-01", "assembly-hall", "gateway-pg-01", "2.4.1", 1, 40.0),
    DeviceSpec("fcu-201", "factory-kl-01", "lab-east", "gateway-kl-02", "3.0.0", 2, 8.0),
    DeviceSpec("ahu-301", "factory-jb-01", "packaging", "gateway-jb-01", "2.4.1", 1, 30.0),
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def base_measurements(rng: random.Random, device: DeviceSpec) -> dict:
    """Realistic, roughly correlated measurement set for a running unit."""
    zone_temp = rng.gauss(23.5, 0.8)
    fan_speed = min(100.0, max(20.0, rng.gauss(70.0, 8.0)))
    measurements = {
        "supply_air_temp_c": round(zone_temp - rng.uniform(8.0, 11.0), 2),
        "return_air_temp_c": round(zone_temp + rng.uniform(0.5, 1.5), 2),
        "zone_temp_c": round(zone_temp, 2),
        "relative_humidity_pct": round(min(100.0, max(5.0, rng.gauss(55.0, 5.0))), 2),
        "airflow_m3h": round(fan_speed * rng.uniform(105.0, 125.0), 1),
        "static_pressure_pa": round(rng.gauss(400.0, 30.0), 1),
        "fan_speed_pct": round(fan_speed, 1),
        "chilled_water_valve_pct": round(min(100.0, max(0.0, rng.gauss(60.0, 12.0))), 1),
        "power_kw": round(device.rated_power_kw * (fan_speed / 100.0) ** 2 * rng.uniform(0.9, 1.1), 2),
    }
    if device.schema_version >= 2:
        measurements["co2_ppm"] = round(rng.gauss(620.0, 80.0), 1)
    return measurements


def build_event(
    device: DeviceSpec,
    *,
    rng: random.Random,
    sequence_number: int,
    event_time: str | None = None,
    boot_id: str | None = None,
    event_id: str | None = None,
    logical_event_id: str | None = None,
    revision_number: int = 1,
    supersedes_event_id: str | None = None,
    measurements: dict | None = None,
    operating_state: dict | None = None,
) -> dict:
    event_id = event_id or f"evt-{rng.getrandbits(64):016x}"
    return {
        "event_id": event_id,
        "logical_event_id": logical_event_id or event_id,
        "revision_number": revision_number,
        "supersedes_event_id": supersedes_event_id,
        "schema_id": 1001,
        "schema_version": device.schema_version,
        "firmware_version": device.firmware_version,
        "event_time": event_time or utcnow_iso(),
        "gateway_id": device.gateway_id,
        "device_id": device.device_id,
        "boot_id": boot_id or f"boot-{datetime.now(timezone.utc):%Y%m%d}-{device.device_id}",
        "sequence_number": sequence_number,
        "site_id": device.site_id,
        "zone_id": device.zone_id,
        "measurements": measurements if measurements is not None else base_measurements(rng, device),
        "operating_state": operating_state
        or {"mode": "AUTO", "temperature_setpoint_c": 23.0, "equipment_running": True},
    }
