"""Seeded stochastic load allocation with explicit mock-plant constraints."""

from __future__ import annotations

import itertools
import random
from dataclasses import dataclass

from plant.contracts import Plant
from plant.contracts import Scope
from plant.contracts import finite


@dataclass(frozen=True)
class Plan:
    scope: Scope
    plant_version: str
    supply_c: float
    allocations_kw: dict[str, float]
    predicted_power_kw: float
    required_load_kw: float
    flow_m3h: float
    algorithm: str = "seeded-stochastic-allocation-v1"


def optimize(
    plant: Plant,
    demand_kw: float,
    wet_bulb_c: float,
    current_supply_c: float,
    *,
    eligible: bool,
    unavailable: tuple[str, ...] = (),
    seed: int = 42,
    trials: int = 40,
) -> Plan | None:
    for name, value in (("demand", demand_kw), ("wet bulb", wet_bulb_c), ("supply", current_supply_c)):
        finite(value, name)
    if demand_kw < 0 or not 0 <= wet_bulb_c <= 35 or not 1 <= trials <= 1000:
        raise ValueError("Inputs outside the mock optimization envelope")
    if not plant.min_supply_c <= current_supply_c <= plant.max_supply_c:
        raise ValueError("Current setpoint outside configured limits")
    if not eligible:
        return None
    equipment_ids = {item.equipment_id for item in plant.equipment}
    if set(unavailable) - equipment_ids:
        raise ValueError("Unknown unavailable equipment")
    # Auxiliary redundancy/hydraulics is not modelled: fail closed on any unavailable auxiliary.
    if any(item.equipment_id in unavailable and item.kind != "chiller" for item in plant.equipment):
        return None
    chillers = [
        item for item in plant.equipment if item.kind == "chiller" and item.equipment_id not in unavailable
    ]
    if len(chillers) > 6:
        raise ValueError("Mock optimizer supports at most six chillers")
    required = demand_kw * (1 + plant.reserve_fraction)
    # Demonstration water-only heat balance: 1.163 kW / (m3/h * K).
    flow = required / (1.163 * plant.design_delta_c)
    if flow > plant.max_flow_m3h:
        return None
    candidates = sorted(
        {
            current_supply_c,
            min(plant.max_supply_c, current_supply_c + plant.max_step_c),
            max(plant.min_supply_c, current_supply_c - plant.max_step_c),
        }
    )
    rng = random.Random(seed)
    best = None
    if required == 0:
        return Plan(plant.scope, plant.version, current_supply_c, {}, 0.0, 0.0, 0.0)
    for count in range(1, len(chillers) + 1):
        for group in itertools.combinations(chillers, count):
            minimum = [unit.capacity_kw * unit.min_load_fraction for unit in group]
            if not sum(minimum) <= required <= sum(unit.capacity_kw for unit in group):
                continue
            for trial in range(trials):
                allocation = minimum.copy()
                remaining = required - sum(allocation)
                if trial == 0:
                    headroom = sum(
                        unit.capacity_kw - load for unit, load in zip(group, allocation, strict=True)
                    )
                    if headroom > 0:
                        allocation = [
                            load + remaining * (unit.capacity_kw - load) / headroom
                            for unit, load in zip(group, allocation, strict=True)
                        ]
                else:
                    order = list(range(count))
                    rng.shuffle(order)
                    for index in order:
                        increment = min(remaining, group[index].capacity_kw - allocation[index])
                        allocation[index] += increment
                        remaining -= increment
                for supply in candidates:
                    power = sum(
                        unit.power(load, supply, wet_bulb_c)
                        for unit, load in zip(group, allocation, strict=True)
                    )
                    power += 0.03 * required + 8 * (flow / plant.max_flow_m3h) ** 3
                    if best is None or power < best.predicted_power_kw:
                        best = Plan(
                            plant.scope,
                            plant.version,
                            supply,
                            {unit.equipment_id: load for unit, load in zip(group, allocation, strict=True)},
                            power,
                            required,
                            flow,
                        )
    return best
