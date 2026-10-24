"""Observe-only tag normalization. Invalid inputs never enter model features."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from plant.contracts import UNITS
from plant.contracts import Plant
from plant.contracts import Reading
from plant.contracts import aware


@dataclass(frozen=True)
class Snapshot:
    values: dict[str, dict[str, float | bool]]
    issues: tuple[str, ...]
    completeness: float

    @property
    def eligible(self) -> bool:
        return not self.issues and self.completeness == 1.0


def normalize(plant: Plant, readings: list[Reading], captured_at: datetime) -> Snapshot:
    aware(captured_at)
    values: dict[str, dict[str, float | bool]] = {}
    issues: list[str] = []
    by_source: dict[str, Reading] = {}
    duplicates = set()
    for reading in readings:
        if reading.source_tag in by_source:
            duplicates.add(reading.source_tag)
        by_source[reading.source_tag] = reading
    mapped = {tag.source_tag for tag in plant.tags}
    issues.extend(f"UNKNOWN_TAG:{name}" for name in sorted(set(by_source) - mapped))
    good_required = 0
    timestamps = []
    for tag in plant.tags:
        reading = by_source.get(tag.source_tag)
        reason = None
        if tag.source_tag in duplicates:
            reason = "DUPLICATE_TAG"
        elif reading is None:
            if tag.required:
                issues.append(f"MISSING_TAG:{tag.source_tag}")
            continue
        elif reading.status != "GOOD":
            reason = "SOURCE_QUALITY"
        elif (captured_at - reading.source_time).total_seconds() > plant.max_age_seconds:
            reason = "STALE"
        elif reading.source_time > captured_at:
            reason = "FUTURE_TIMESTAMP"
        elif datetime.fromisoformat(tag.calibration_due) <= captured_at:
            reason = "CALIBRATION_EXPIRED"
        elif tag.metric == "running":
            if type(reading.value) is not bool:
                reason = "INVALID_BOOLEAN"
        elif type(reading.value) not in (int, float) or not math.isfinite(reading.value):
            reason = "INVALID_NUMBER"
        if reason:
            issues.append(f"{reason}:{tag.source_tag}")
            continue
        if tag.metric == "running":
            value = reading.value
        else:
            scale, offset = UNITS[tag.metric][tag.unit]
            value = (reading.value * scale + offset) * tag.calibration_scale + tag.calibration_offset
            if not math.isfinite(value) or not tag.minimum <= value <= tag.maximum:
                issues.append(f"OUT_OF_RANGE:{tag.source_tag}")
                continue
        values.setdefault(tag.equipment_id, {})[tag.metric] = value
        timestamps.append(reading.source_time)
        good_required += int(tag.required)
    if timestamps and (max(timestamps) - min(timestamps)).total_seconds() > plant.max_skew_seconds:
        issues.append("TIME_ALIGNMENT")
    for equipment_id, metrics in values.items():
        if (
            "supply_temp_c" in metrics
            and "return_temp_c" in metrics
            and metrics["return_temp_c"] < metrics["supply_temp_c"]
        ):
            issues.append(f"NEGATIVE_COOLING_DELTA:{equipment_id}")
    total_required = sum(tag.required for tag in plant.tags)
    return Snapshot(values, tuple(issues), good_required / total_required if total_required else 1.0)
