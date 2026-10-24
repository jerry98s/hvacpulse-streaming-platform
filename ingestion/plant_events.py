"""Plant Avro wire contract; no derived features or labels are produced here."""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import asdict
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from functools import lru_cache

from fastavro import schemaless_reader
from fastavro import schemaless_writer

from ingestion.plant_simulator import mock_readings
from ingestion.simulator import SimulatedEvent
from plant.contracts import Plant
from streaming.config import REPO_ROOT

EVENT_TYPE = "plant_snapshot"


@lru_cache
def schema():
    return json.loads((REPO_ROOT / "contracts/plant/plant_snapshot_v1.avsc").read_text(encoding="utf-8"))


def timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def encode(event: dict) -> bytes:
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema(), event)
    return buffer.getvalue()


def decode(value: bytes) -> dict:
    buffer = io.BytesIO(value)
    event = schemaless_reader(buffer, schema())
    if buffer.read(1):
        raise ValueError("Trailing bytes after plant record")
    return event


def build_snapshot(
    plant: Plant,
    at: datetime,
    *,
    seed: int = 42,
    fault: str | None = None,
    applied_plan: dict | None = None,
    applied_decision_id: str | None = None,
) -> dict:
    if at.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    at = at.astimezone(timezone.utc)
    time = timestamp(at)
    return {
        "schema_version": 1,
        "event_id": hashlib.sha256(f"{plant.scope.key}:{time}".encode()).hexdigest(),
        **asdict(plant.scope),
        "config_version": plant.version,
        "captured_at": time,
        "operating": at.weekday() < 5 and 7 <= at.hour < 21,
        "synthetic": True,
        "applied_decision_id": applied_decision_id,
        "readings": [
            {**asdict(item), "source_time": timestamp(item.source_time)}
            for item in mock_readings(plant, at, seed=seed, fault=fault, applied_plan=applied_plan)
        ],
    }


class PlantSimulator:
    """Live snapshots by default; explicit replay ends at (never after) start time."""

    def __init__(self, plant: Plant, *, seed: int = 42, replay: bool = False, anomaly_rate: float = 0):
        self.plant, self.seed, self.replay, self.anomaly_rate = plant, seed, replay, anomaly_rate

    def events(self, count: int):
        import random

        rng = random.Random(self.seed)
        end = datetime.now(timezone.utc)
        for index in range(count):
            at = (
                end - timedelta(minutes=15 * (count - index - 1))
                if self.replay
                else datetime.now(timezone.utc)
            )
            fault = rng.choice(["missing", "bad", "stale"]) if rng.random() < self.anomaly_rate else None
            event = build_snapshot(self.plant, at, seed=self.seed, fault=fault)
            yield SimulatedEvent(
                key=self.plant.scope.key.encode(),
                value=encode(event),
                headers=(("event_type", EVENT_TYPE.encode()), ("schema_version", b"1")),
                label=fault or "normal",
                event=event,
            )
