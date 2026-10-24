"""Versioned, tenant-scoped plant and feature contracts shared by pipeline jobs."""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal


def finite(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")


def aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("UTC-aware timestamps are required")


@dataclass(frozen=True)
class Scope:
    tenant_id: str
    site_id: str
    plant_id: str

    def __post_init__(self):
        for value in asdict(self).values():
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
                raise ValueError("Scope identifiers must be nonempty, path-safe identifiers")

    @property
    def key(self) -> str:
        return "/".join((self.tenant_id, self.site_id, self.plant_id))


EQUIPMENT_KINDS = {"chiller", "cooling_tower", "chw_pump", "cw_pump", "vfd", "process"}


@dataclass(frozen=True)
class Equipment:
    equipment_id: str
    kind: str
    capacity_kw: float = 0.0
    min_load_fraction: float = 0.2
    idle_power_kw: float = 0.0
    linear_kw_per_kw: float = 0.0
    quadratic_kw_per_kw2: float = 0.0

    def __post_init__(self):
        if not self.equipment_id or self.kind not in EQUIPMENT_KINDS:
            raise ValueError("Unknown equipment kind or empty equipment ID")
        for name in (
            "capacity_kw",
            "min_load_fraction",
            "idle_power_kw",
            "linear_kw_per_kw",
            "quadratic_kw_per_kw2",
        ):
            value = getattr(self, name)
            finite(value, name)
            if value < 0:
                raise ValueError(f"{name} cannot be negative")
        if self.min_load_fraction > 1:
            raise ValueError("Minimum loading must be within [0, 1]")
        if self.kind == "chiller" and (self.capacity_kw <= 0 or self.linear_kw_per_kw <= 0):
            raise ValueError("Chillers require positive capacity and power coefficient")

    def power(self, load_kw: float, supply_c: float, wet_bulb_c: float) -> float:
        """Illustrative efficiency curve, NOT a manufacturer performance map."""
        if load_kw <= 0:
            return 0.0
        return (
            self.idle_power_kw + self.linear_kw_per_kw * load_kw + self.quadratic_kw_per_kw2 * load_kw**2
        ) * (1 + 0.015 * (wet_bulb_c - 24) - 0.025 * (supply_c - 6))


# Canonical units and supported input conversions: canonical = raw * scale + offset.
UNITS = {
    "supply_temp_c": {"C": (1, 0), "F": (5 / 9, -160 / 9)},
    "return_temp_c": {"C": (1, 0), "F": (5 / 9, -160 / 9)},
    "outdoor_temp_c": {"C": (1, 0), "F": (5 / 9, -160 / 9)},
    "wet_bulb_c": {"C": (1, 0), "F": (5 / 9, -160 / 9)},
    "flow_m3h": {"m3/h": (1, 0), "l/s": (3.6, 0)},
    "differential_pressure_kpa": {"kPa": (1, 0), "Pa": (0.001, 0)},
    "power_kw": {"kW": (1, 0), "W": (0.001, 0)},
    "heat_load_kw": {"kW": (1, 0), "W": (0.001, 0)},
    "loading_pct": {"%": (1, 0), "ratio": (100, 0)},
    "speed_pct": {"%": (1, 0), "ratio": (100, 0)},
    "humidity_pct": {"%": (1, 0), "ratio": (100, 0)},
    "occupancy_pct": {"%": (1, 0), "ratio": (100, 0)},
    "particle_count_m3": {"count/m3": (1, 0)},
    "running": {"bool": (1, 0)},
}


@dataclass(frozen=True)
class Tag:
    source_tag: str
    equipment_id: str
    metric: str
    unit: str
    minimum: float
    maximum: float
    calibration_due: str
    required: bool = True
    calibration_scale: float = 1.0
    calibration_offset: float = 0.0

    def __post_init__(self):
        if not self.source_tag or self.unit not in UNITS.get(self.metric, {}):
            raise ValueError("Unknown tag, metric or unit")
        for name in ("minimum", "maximum", "calibration_scale", "calibration_offset"):
            finite(getattr(self, name), name)
        if self.minimum > self.maximum or self.calibration_scale <= 0:
            raise ValueError("Invalid tag bounds or calibration scale")
        if self.metric == "running" and (self.calibration_scale != 1 or self.calibration_offset != 0):
            raise ValueError("Boolean tags cannot be calibrated numerically")
        aware(datetime.fromisoformat(self.calibration_due))


@dataclass(frozen=True)
class Plant:
    scope: Scope
    version: str
    equipment: tuple[Equipment, ...]
    tags: tuple[Tag, ...]
    connections: tuple[tuple[str, str, str], ...]
    min_supply_c: float = 6.0
    max_supply_c: float = 8.0
    max_step_c: float = 0.5
    max_flow_m3h: float = 250.0
    design_delta_c: float = 5.0
    reserve_fraction: float = 0.05
    max_age_seconds: float = 60.0
    max_skew_seconds: float = 10.0

    def __post_init__(self):
        ids = [item.equipment_id for item in self.equipment]
        if not self.version or len(ids) != len(set(ids)) or not self.equipment or not self.tags:
            raise ValueError("Plant requires a version and unique equipment and mapped tags")
        if not any(item.kind == "chiller" for item in self.equipment):
            raise ValueError("Plant requires at least one chiller")
        sources = [tag.source_tag for tag in self.tags]
        targets = [(tag.equipment_id, tag.metric) for tag in self.tags]
        if len(sources) != len(set(sources)) or len(targets) != len(set(targets)):
            raise ValueError("Duplicate source tag or equipment/metric mapping")
        if any(tag.equipment_id not in ids for tag in self.tags):
            raise ValueError("Tag references unknown equipment")
        for source, target, circuit in self.connections:
            if (
                source not in ids
                or target not in ids
                or source == target
                or circuit not in {"chw", "cw", "drive"}
            ):
                raise ValueError("Invalid topology connection")
        for name in (
            "min_supply_c",
            "max_supply_c",
            "max_step_c",
            "max_flow_m3h",
            "design_delta_c",
            "reserve_fraction",
            "max_age_seconds",
            "max_skew_seconds",
        ):
            finite(getattr(self, name), name)
        if not 0 < self.min_supply_c <= self.max_supply_c <= 20:
            raise ValueError("Invalid mock supply-temperature envelope")
        if (
            min(
                self.max_step_c,
                self.max_flow_m3h,
                self.design_delta_c,
                self.max_age_seconds,
                self.max_skew_seconds,
            )
            <= 0
            or not 0 <= self.reserve_fraction <= 0.5
        ):
            raise ValueError("Invalid plant limits")

    @classmethod
    def load(cls, path: Path) -> Plant:
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            **{
                **data,
                "scope": Scope(**data["scope"]),
                "equipment": tuple(Equipment(**item) for item in data["equipment"]),
                "tags": tuple(Tag(**item) for item in data["tags"]),
                "connections": tuple(tuple(item) for item in data["connections"]),
            }
        )


@dataclass(frozen=True)
class Reading:
    source_tag: str
    value: float | bool | None
    source_time: datetime
    status: Literal["GOOD", "BAD", "UNCERTAIN"] = "GOOD"

    def __post_init__(self):
        aware(self.source_time)


@dataclass(frozen=True)
class Features:
    scope: Scope
    time: datetime
    available_at: datetime
    outdoor_c: float
    humidity_pct: float
    wet_bulb_c: float
    occupancy_pct: float
    production_kw: float
    operating: bool

    def __post_init__(self):
        aware(self.time)
        aware(self.available_at)
        for name in ("outdoor_c", "humidity_pct", "wet_bulb_c", "occupancy_pct", "production_kw"):
            finite(getattr(self, name), name)
        if not -30 <= self.outdoor_c <= 60 or not -40 <= self.wet_bulb_c <= self.outdoor_c:
            raise ValueError("Invalid weather features")
        if not 0 <= self.humidity_pct <= 100 or not 0 <= self.occupancy_pct <= 100 or self.production_kw < 0:
            raise ValueError("Invalid process features")
        if not isinstance(self.operating, bool):
            raise ValueError("Operating flag must be boolean")

    def vector(self) -> list[float]:
        hour = self.time.hour + self.time.minute / 60
        return [
            self.outdoor_c,
            self.humidity_pct,
            self.wet_bulb_c,
            self.occupancy_pct,
            self.production_kw,
            float(self.operating),
            math.sin(hour * math.pi / 12),
            math.cos(hour * math.pi / 12),
        ]
