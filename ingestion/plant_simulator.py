"""Deterministic raw plant source. Transport lives in ingestion.producer."""

from __future__ import annotations

import math
import random
from datetime import datetime
from datetime import timedelta

from plant.contracts import UNITS
from plant.contracts import Features
from plant.contracts import Plant
from plant.contracts import Reading


def features_at(
    plant: Plant, target: datetime, *, seed: int = 42, issued_at: datetime | None = None
) -> Features:
    """Weather forecast has independent error; only schedules are known exactly."""
    if issued_at is not None and target <= issued_at:
        raise ValueError("Forecast target must be after issue time")
    rng = random.Random(f"{seed}:{target.isoformat()}:{issued_at.isoformat() if issued_at else 'observed'}")
    hour = target.hour + target.minute / 60
    operating = target.weekday() < 5 and 7 <= hour < 21
    outdoor = 29 + 4 * math.sin((hour - 8) * math.pi / 12) + rng.gauss(0, 0.4)
    humidity = 65 - 8 * math.sin((hour - 8) * math.pi / 12) + rng.gauss(0, 0.8)
    # A mock wet-bulb signal, not a psychrometric calculation.
    wet_bulb = outdoor - 4 - (100 - humidity) * 0.04
    production = (280 if operating else 70) + rng.gauss(0, 6)
    return Features(
        plant.scope,
        target,
        issued_at or target,
        outdoor,
        humidity,
        wet_bulb,
        85 if operating else 10,
        production,
        operating,
    )


def cooling_load(features: Features) -> float:
    return (
        150
        + 11 * max(features.outdoor_c - 20, 0)
        + 0.75 * features.production_kw
        + 1.2 * features.occupancy_pct
        + 0.7 * max(features.outdoor_c - 27, 0) ** 2
    )


def mock_readings(
    plant: Plant,
    time: datetime,
    *,
    seed: int = 42,
    fault: str | None = None,
    applied_plan: dict | None = None,
) -> list[Reading]:
    features = features_at(plant, time, seed=seed)
    load = cooling_load(features)
    chillers = [item for item in plant.equipment if item.kind == "chiller"]
    allocations = applied_plan["allocations_kw"] if applied_plan else None
    supply = applied_plan["supply_c"] if applied_plan else plant.min_supply_c
    if applied_plan and (
        applied_plan["scope"]
        != {
            "tenant_id": plant.scope.tenant_id,
            "site_id": plant.scope.site_id,
            "plant_id": plant.scope.plant_id,
        }
        or applied_plan["plant_version"] != plant.version
    ):
        raise ValueError("Mock feedback scope/configuration mismatch")
    total_allocation = sum(allocations.values()) if allocations else 0
    readings = []
    for tag in plant.tags:
        unit = next(item for item in plant.equipment if item.equipment_id == tag.equipment_id)
        shared = load / len(chillers)
        if allocations is not None:
            shared = load * allocations.get(unit.equipment_id, 0) / max(total_allocation, 1e-9)
        values = {
            "supply_temp_c": supply,
            "return_temp_c": supply + plant.design_delta_c,
            "flow_m3h": shared / (1.163 * plant.design_delta_c),
            "running": shared > 0 if unit.kind == "chiller" else True,
            "loading_pct": shared / unit.capacity_kw * 100 if unit.capacity_kw else 60,
            "differential_pressure_kpa": 90,
            "speed_pct": 70,
            "outdoor_temp_c": features.outdoor_c,
            "humidity_pct": features.humidity_pct,
            "wet_bulb_c": features.wet_bulb_c,
            "occupancy_pct": features.occupancy_pct,
            "heat_load_kw": features.production_kw,
            "particle_count_m3": 120.0,
        }
        if unit.kind == "chiller":
            values["power_kw"] = unit.power(shared, supply, features.wet_bulb_c) * (
                1.0 if applied_plan else 1.08
            )
        else:
            values["power_kw"] = {"cooling_tower": 0.03 * load, "chw_pump": 5.0, "cw_pump": 4.0}.get(
                unit.kind, 0
            )
        canonical = values[tag.metric]
        scale, offset = UNITS[tag.metric][tag.unit]
        raw = (
            canonical
            if tag.metric == "running"
            else (((canonical - tag.calibration_offset) / tag.calibration_scale - offset) / scale)
        )
        readings.append(Reading(tag.source_tag, raw, time))
    if fault and readings:
        first = readings[0]
        if fault == "missing":
            readings.pop(0)
        elif fault == "stale":
            readings[0] = Reading(first.source_tag, first.value, time - timedelta(minutes=5))
        elif fault == "bad":
            readings[0] = Reading(first.source_tag, first.value, time, "BAD")
        else:
            raise ValueError("Unsupported mock fault")
    return readings
