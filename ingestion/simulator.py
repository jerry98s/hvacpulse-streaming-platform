"""HVAC telemetry simulator with deliberate anomaly injection.

Generates the full mix described in the README: valid telemetry, exact
duplicate retries, conflicting duplicates, late / out-of-order events, missing
values, physically impossible measurements, frozen sensors, unknown equipment,
unsupported schema versions, firmware v2 (additive ``co2_ppm``) events and
unparseable payloads.

Deterministic for a given ``seed`` so tests and demos are reproducible.
"""

from __future__ import annotations

import io
import json
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from fastavro import parse_schema
from fastavro import schemaless_writer

from ingestion.events import DEFAULT_DEVICES
from ingestion.events import DeviceSpec
from ingestion.events import base_measurements
from ingestion.events import build_event

CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "avro"

VALID = "valid"
DUPLICATE = "duplicate"
CONFLICTING_DUPLICATE = "conflicting_duplicate"
LATE = "late"
OUT_OF_ORDER = "out_of_order"
MISSING_HUMIDITY = "missing_humidity"
IMPOSSIBLE_TEMP = "impossible_temp"
NEGATIVE_POWER = "negative_power"
UNKNOWN_EQUIPMENT = "unknown_equipment"
UNSUPPORTED_SCHEMA = "unsupported_schema"
FROZEN_SENSOR = "frozen_sensor"
CORRUPT_BYTES = "corrupt_bytes"

ANOMALY_LABELS = (
    DUPLICATE,
    CONFLICTING_DUPLICATE,
    LATE,
    OUT_OF_ORDER,
    MISSING_HUMIDITY,
    IMPOSSIBLE_TEMP,
    NEGATIVE_POWER,
    UNKNOWN_EQUIPMENT,
    UNSUPPORTED_SCHEMA,
    FROZEN_SENSOR,
    CORRUPT_BYTES,
)

UNKNOWN_DEVICE = DeviceSpec(
    "ahu-999-unknown", "factory-kl-01", "cleanroom-a", "gateway-kl-01", "2.4.1", 1, 25.0
)


def load_avro_schema(version: int) -> dict:
    path = CONTRACTS_DIR / f"hvac_telemetry_v{version}.avsc"
    return parse_schema(json.loads(path.read_text(encoding="utf-8")))


@dataclass(frozen=True)
class SimulatedEvent:
    key: bytes
    value: bytes
    headers: tuple[tuple[str, bytes], ...]
    label: str
    event: dict | None  # None for payloads that are intentionally unparseable


def _serialize(event: dict, schema: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, event)
    return buf.getvalue()


class HvacSimulator:
    """Stateful simulator: per-device sequences, replay caches, frozen sensors."""

    def __init__(
        self,
        *,
        seed: int = 42,
        anomaly_rate: float = 0.15,
        devices: list[DeviceSpec] | None = None,
        start_time: datetime | None = None,
    ) -> None:
        self._rng = random.Random(seed)
        self.devices = list(devices or DEFAULT_DEVICES)
        self.anomaly_rate = anomaly_rate
        self._schemas = {1: load_avro_schema(1), 2: load_avro_schema(2)}
        self._clock = start_time or datetime.now(timezone.utc)
        self._seq: dict[str, int] = {d.device_id: 0 for d in self.devices}
        self._last_sent: dict[str, SimulatedEvent] = {}
        self._frozen_measurements: dict[str, dict] = {}

    # -- event factory ------------------------------------------------------

    def _tick(self, seconds: float = 5.0) -> str:
        self._clock += timedelta(seconds=seconds)
        return self._clock.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _next_event(self, device: DeviceSpec, *, event_time: str | None = None) -> dict:
        self._seq.setdefault(device.device_id, 0)
        self._seq[device.device_id] += 1
        return build_event(
            device,
            rng=self._rng,
            sequence_number=self._seq[device.device_id],
            event_time=event_time or self._tick(),
            boot_id=f"boot-{self._clock:%Y%m%d}-{device.device_id}",
        )

    def _to_wire(self, event: dict, label: str, *, schema_version: int | None = None) -> SimulatedEvent:
        version = schema_version if schema_version is not None else event["schema_version"]
        serialize_as = version if version in self._schemas else 1
        value = _serialize(event, self._schemas[serialize_as])
        return SimulatedEvent(
            key=event["device_id"].encode("utf-8"),
            value=value,
            headers=(("schema_version", str(version).encode("ascii")),),
            label=label,
            event=event,
        )

    # -- anomalies ----------------------------------------------------------

    def _duplicate(self) -> SimulatedEvent | None:
        if not self._last_sent:
            return None
        original = self._rng.choice(list(self._last_sent.values()))
        return SimulatedEvent(original.key, original.value, original.headers, DUPLICATE, original.event)

    def _conflicting_duplicate(self) -> SimulatedEvent | None:
        candidates = [e for e in self._last_sent.values() if e.event is not None]
        if not candidates:
            return None
        original = self._rng.choice(candidates)
        conflict = dict(original.event)
        conflict["measurements"] = dict(original.event["measurements"])
        conflict["measurements"]["power_kw"] = round(
            (original.event["measurements"].get("power_kw") or 10.0) * 1.5, 2
        )
        return self._to_wire(conflict, CONFLICTING_DUPLICATE)

    def _late(self, device: DeviceSpec) -> SimulatedEvent:
        late_time = (self._clock - timedelta(days=8)).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
        return self._to_wire(self._next_event(device, event_time=late_time), LATE)

    def _out_of_order(self, device: DeviceSpec) -> SimulatedEvent:
        old_time = (self._clock - timedelta(seconds=90)).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
        return self._to_wire(self._next_event(device, event_time=old_time), OUT_OF_ORDER)

    def _missing_humidity(self, device: DeviceSpec) -> SimulatedEvent:
        event = self._next_event(device)
        event["measurements"]["relative_humidity_pct"] = None
        return self._to_wire(event, MISSING_HUMIDITY)

    def _impossible_temp(self, device: DeviceSpec) -> SimulatedEvent:
        event = self._next_event(device)
        event["measurements"]["zone_temp_c"] = 180.0
        return self._to_wire(event, IMPOSSIBLE_TEMP)

    def _negative_power(self, device: DeviceSpec) -> SimulatedEvent:
        event = self._next_event(device)
        event["measurements"]["power_kw"] = -5.0
        return self._to_wire(event, NEGATIVE_POWER)

    def _unknown_equipment(self) -> SimulatedEvent:
        return self._to_wire(self._next_event(UNKNOWN_DEVICE), UNKNOWN_EQUIPMENT)

    def _unsupported_schema(self, device: DeviceSpec) -> SimulatedEvent:
        event = self._next_event(device)
        event["schema_version"] = 99
        return self._to_wire(event, UNSUPPORTED_SCHEMA, schema_version=99)

    def _frozen_sensor(self, device: DeviceSpec) -> SimulatedEvent:
        if device.device_id not in self._frozen_measurements:
            self._frozen_measurements[device.device_id] = base_measurements(self._rng, device)
        event = self._next_event(device)
        event["measurements"] = dict(self._frozen_measurements[device.device_id])
        return self._to_wire(event, FROZEN_SENSOR)

    def _corrupt_bytes(self, device: DeviceSpec) -> SimulatedEvent:
        self._next_event(device)  # keep sequence numbers advancing
        return SimulatedEvent(
            key=device.device_id.encode("utf-8"),
            value=self._rng.randbytes(37),
            headers=(("schema_version", b"1"),),
            label=CORRUPT_BYTES,
            event=None,
        )

    # -- public API ---------------------------------------------------------

    def events(self, count: int) -> Iterator[SimulatedEvent]:
        anomaly_handlers = {
            LATE: self._late,
            OUT_OF_ORDER: self._out_of_order,
            MISSING_HUMIDITY: self._missing_humidity,
            IMPOSSIBLE_TEMP: self._impossible_temp,
            NEGATIVE_POWER: self._negative_power,
            FROZEN_SENSOR: self._frozen_sensor,
            CORRUPT_BYTES: self._corrupt_bytes,
        }
        for _ in range(count):
            device = self._rng.choice(self.devices)
            label = VALID
            wire: SimulatedEvent | None = None
            if self._rng.random() < self.anomaly_rate:
                label = self._rng.choice(ANOMALY_LABELS)
                if label == DUPLICATE:
                    wire = self._duplicate()
                elif label == CONFLICTING_DUPLICATE:
                    wire = self._conflicting_duplicate()
                elif label == UNKNOWN_EQUIPMENT:
                    wire = self._unknown_equipment()
                elif label == UNSUPPORTED_SCHEMA:
                    wire = self._unsupported_schema(device)
                else:
                    wire = anomaly_handlers[label](device)
            if wire is None:
                label, wire = VALID, self._to_wire(self._next_event(device), VALID)
            if wire.event is not None and wire.label in (VALID, FROZEN_SENSOR):
                self._last_sent[wire.event["device_id"]] = wire
            yield wire


def build_correction(simulator: HvacSimulator, original: dict, *, new_power_kw: float) -> SimulatedEvent:
    """Build a late-correction revision: new event_id, same logical_event_id."""
    correction = dict(original)
    # Deliberately uses the real RNG: a correction is a *new* physical event.
    correction["event_id"] = f"evt-{simulator._rng.getrandbits(64):016x}"
    correction["revision_number"] = int(original.get("revision_number", 1)) + 1
    correction["supersedes_event_id"] = original["event_id"]
    # A correction changes what was observed, not when it was observed. Keeping
    # the effective event time ensures Gold recomputes the original window.
    correction["event_time"] = original["event_time"]
    correction["measurements"] = dict(original["measurements"])
    correction["measurements"]["power_kw"] = new_power_kw
    return simulator._to_wire(correction, "correction")
