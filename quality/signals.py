"""Operational signals: valid data that reveals a real equipment or facility problem.

Unlike hard rules (``quality.rules``), signals never quarantine. A physically
valid temperature outside the facility limit is an
ENVIRONMENTAL_LIMIT_VIOLATION signal, not corrupt data — it must stay in Silver
and reach operators.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path

from quality.rules import parse_event_time

ENVIRONMENTAL_LIMIT_VIOLATION = "ENVIRONMENTAL_LIMIT_VIOLATION"
EQUIPMENT_STATE_INCONSISTENT = "EQUIPMENT_STATE_INCONSISTENT"
SENSOR_STALE = "SENSOR_STALE"
SEQUENCE_GAP = "SEQUENCE_GAP"

# Cross-field plausibility thresholds (engineering heuristics, not physical laws).
POWER_HIGH_FRACTION_OF_RATED = 0.30
AIRFLOW_HIGH_M3H = 1000.0
COOLING_HIGH_DELTA_C = 2.0

REFERENCE_CSV = Path(__file__).resolve().parents[1] / "reference" / "equipment_master.csv"


@dataclass(frozen=True)
class EquipmentLimits:
    device_id: str
    equipment_type: str
    rated_power_kw: float
    temp_min_c: float
    temp_max_c: float
    humidity_min_pct: float
    humidity_max_pct: float
    expected_report_interval_sec: float
    commissioning_status: str


def load_equipment_limits(csv_path: Path | str = REFERENCE_CSV) -> dict[str, EquipmentLimits]:
    limits: dict[str, EquipmentLimits] = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            limits[row["device_id"]] = EquipmentLimits(
                device_id=row["device_id"],
                equipment_type=row["equipment_type"],
                rated_power_kw=float(row["rated_power_kw"]),
                temp_min_c=float(row["temp_min_c"]),
                temp_max_c=float(row["temp_max_c"]),
                humidity_min_pct=float(row["humidity_min_pct"]),
                humidity_max_pct=float(row["humidity_max_pct"]),
                expected_report_interval_sec=float(row["expected_report_interval_sec"]),
                commissioning_status=row["commissioning_status"],
            )
    return limits


def environmental_violations(event: dict, limits: EquipmentLimits) -> list[str]:
    """Compliance signals for physically valid readings outside facility limits."""
    measurements = event.get("measurements", {})
    violations: list[str] = []
    zone_temp = measurements.get("zone_temp_c")
    if zone_temp is not None and not limits.temp_min_c <= zone_temp <= limits.temp_max_c:
        violations.append(ENVIRONMENTAL_LIMIT_VIOLATION + ":ZONE_TEMP")
    humidity = measurements.get("relative_humidity_pct")
    if humidity is not None and not limits.humidity_min_pct <= humidity <= limits.humidity_max_pct:
        violations.append(ENVIRONMENTAL_LIMIT_VIOLATION + ":HUMIDITY")
    return violations


def equipment_state_inconsistencies(event: dict, limits: EquipmentLimits | None = None) -> list[str]:
    """Cross-field anomalies: valid individual values that cannot be true together."""
    measurements = event.get("measurements", {})
    state = event.get("operating_state", {})
    signals: list[str] = []

    rated = limits.rated_power_kw if limits else 25.0
    power = measurements.get("power_kw")
    if state.get("equipment_running") is False and power is not None and power > rated * POWER_HIGH_FRACTION_OF_RATED:
        signals.append(EQUIPMENT_STATE_INCONSISTENT + ":NOT_RUNNING_HIGH_POWER")

    fan = measurements.get("fan_speed_pct")
    airflow = measurements.get("airflow_m3h")
    if fan == 0.0 and airflow is not None and airflow > AIRFLOW_HIGH_M3H:
        signals.append(EQUIPMENT_STATE_INCONSISTENT + ":FAN_STOPPED_HIGH_AIRFLOW")

    valve = measurements.get("chilled_water_valve_pct")
    supply = measurements.get("supply_air_temp_c")
    ret = measurements.get("return_air_temp_c")
    if (
        valve == 0.0
        and supply is not None
        and ret is not None
        and ret - supply > COOLING_HIGH_DELTA_C
    ):
        signals.append(EQUIPMENT_STATE_INCONSISTENT + ":VALVE_CLOSED_HIGH_COOLING")

    return signals


def is_stale(
    last_event_time: str,
    *,
    now: datetime | None = None,
    expected_interval_sec: float,
    stale_factor: float = 3.0,
) -> bool:
    """Freshness is computed from the last observed event, never a quarantine rule."""
    parsed = parse_event_time(last_event_time)
    if parsed is None:
        return True
    now = now or datetime.now(timezone.utc)
    return (now - parsed).total_seconds() > expected_interval_sec * stale_factor


def sequence_gap(previous_sequence: int | None, current_sequence: int) -> int:
    """Return the number of missing messages between two sequence numbers (0 = none)."""
    if previous_sequence is None:
        return 0
    return max(0, current_sequence - previous_sequence - 1)
