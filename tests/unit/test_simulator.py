import io
from datetime import datetime
from datetime import timezone

import pytest
from fastavro import schemaless_reader

from ingestion.events import DEFAULT_DEVICES
from ingestion.simulator import CONFLICTING_DUPLICATE
from ingestion.simulator import CORRUPT_BYTES
from ingestion.simulator import DUPLICATE
from ingestion.simulator import VALID
from ingestion.simulator import HvacSimulator
from ingestion.simulator import build_correction
from ingestion.simulator import load_avro_schema
from quality import rules

pytestmark = pytest.mark.unit

KNOWN_DEVICES = {d.device_id for d in DEFAULT_DEVICES}


def test_simulator_is_deterministic_per_seed():
    start = datetime(2026, 8, 26, tzinfo=timezone.utc)
    a = [e.value for e in HvacSimulator(seed=1, anomaly_rate=0.5, start_time=start).events(200)]
    b = [e.value for e in HvacSimulator(seed=1, anomaly_rate=0.5, start_time=start).events(200)]
    assert a == b


def test_anomaly_mix_covers_expected_labels():
    labels = {e.label for e in HvacSimulator(seed=3, anomaly_rate=0.5).events(2000)}
    assert VALID in labels
    assert DUPLICATE in labels
    assert CONFLICTING_DUPLICATE in labels
    assert CORRUPT_BYTES in labels
    assert len(labels) >= 8


def test_valid_events_pass_hard_validation():
    sim = HvacSimulator(seed=11, anomaly_rate=0.0)
    for wire in sim.events(100):
        assert wire.label == VALID
        assert wire.event is not None
        assert rules.validate_event(wire.event, known_devices=KNOWN_DEVICES) is None


def test_duplicate_is_byte_identical_retry():
    sim = HvacSimulator(seed=5, anomaly_rate=0.9)
    wires = list(sim.events(500))
    by_id = {}
    for w in wires:
        if w.event is not None and w.label not in (DUPLICATE, CONFLICTING_DUPLICATE):
            by_id.setdefault(w.event["event_id"], w)
    duplicates = [w for w in wires if w.label == DUPLICATE]
    assert duplicates
    for dup in duplicates:
        original = by_id[dup.event["event_id"]]
        assert dup.value == original.value


def test_conflicting_duplicate_keeps_event_id_but_changes_payload():
    sim = HvacSimulator(seed=6, anomaly_rate=0.9)
    wires = list(sim.events(500))
    conflicts = [w for w in wires if w.label == CONFLICTING_DUPLICATE]
    assert conflicts
    by_id = {}
    for w in wires:
        if w.event is not None and w.label != CONFLICTING_DUPLICATE:
            by_id.setdefault(w.event["event_id"], w)
    for conflict in conflicts:
        original = by_id[conflict.event["event_id"]]
        assert conflict.value != original.value


def test_v2_events_round_trip_with_co2():
    sim = HvacSimulator(seed=8, anomaly_rate=0.0, devices=[DEFAULT_DEVICES[3]])  # fcu-201, v2
    schema_v2 = load_avro_schema(2)
    for wire in sim.events(20):
        assert wire.headers == (("schema_version", b"2"),)
        decoded = schemaless_reader(io.BytesIO(wire.value), schema_v2)
        assert decoded["schema_version"] == 2
        assert decoded["measurements"]["co2_ppm"] is not None


def test_corrupt_events_have_no_parsed_payload():
    sim = HvacSimulator(seed=9, anomaly_rate=1.0)
    corrupt = [w for w in sim.events(300) if w.label == CORRUPT_BYTES]
    assert corrupt
    for w in corrupt:
        assert w.event is None
        assert len(w.value) == 37


def test_correction_advances_revision_and_links_superseded():
    sim = HvacSimulator(seed=12, anomaly_rate=0.0)
    original = next(sim.events(1)).event
    correction = build_correction(sim, original, new_power_kw=99.0)
    assert correction.event["logical_event_id"] == original["logical_event_id"]
    assert correction.event["revision_number"] == original["revision_number"] + 1
    assert correction.event["supersedes_event_id"] == original["event_id"]
    assert correction.event["event_id"] != original["event_id"]
    assert correction.event["event_time"] == original["event_time"]
    assert correction.event["measurements"]["power_kw"] == 99.0
