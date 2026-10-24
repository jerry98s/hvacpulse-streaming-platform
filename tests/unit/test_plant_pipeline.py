"""Wire → normalized snapshot → features → model → gated control contracts."""

import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from ingestion.plant_events import PlantSimulator
from ingestion.plant_events import build_snapshot
from ingestion.plant_events import decode
from ingestion.plant_events import encode
from ingestion.producer import main as producer_main
from ingestion.producer import parse_args
from plant.contracts import Plant
from plant.learning import decide
from plant.learning import fit_chronological
from streaming.config import REPO_ROOT
from streaming.config import Settings
from streaming.plant_store import control_writer
from streaming.plant_transforms import extract_features
from streaming.plant_transforms import normalize_record

pytestmark = pytest.mark.unit
AT = datetime(2026, 9, 10, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def plant():
    return Plant.load(REPO_ROOT / "reference/mock_plant.json")


def feature(plant, at, *, fault=None, received=None):
    row = normalize_record(encode(build_snapshot(plant, at, fault=fault)), received or at, plant)
    if row["eligible"]:
        row.update(extract_features(row, plant))
    return row


@pytest.fixture(scope="module")
def trained(plant):
    # Backfilled data arrives together: chronological splitting is by observation
    # time, not ingestion order, with availability checked by the training job.
    rows = [feature(plant, AT - timedelta(minutes=15 * i), received=AT) for i in range(384, 0, -1)]
    model, metrics = fit_chronological(rows, plant, epochs=3)
    return {
        "artifact_json": json.dumps(model.artifact()),
        "approved": metrics["approved"],
        "holdout_through": metrics["holdout_through"],
        "trained_at": AT.isoformat(),
    }, metrics


def test_avro_roundtrip_and_raw_only_source(plant):
    event = build_snapshot(plant, AT)
    assert decode(encode(event)) == event
    assert "cooling_kw" not in event and len(event["readings"]) == 24
    assert feature(plant, AT)["cooling_kw"] > 0
    wires = list(PlantSimulator(plant, replay=True).events(3))
    assert wires[0].key == plant.scope.key.encode()
    times = [datetime.fromisoformat(decode(w.value)["captured_at"]) for w in wires]
    assert times[1] - times[0] == timedelta(minutes=15)
    assert times[-1] <= datetime.now(timezone.utc)


@pytest.mark.parametrize("fault", ["missing", "bad", "stale"])
def test_invalid_snapshots_have_no_labels(plant, fault):
    row = feature(plant, AT, fault=fault)
    assert not row["eligible"] and "cooling_kw" not in row


def test_corrupt_foreign_and_future_quarantined(plant):
    assert not normalize_record(b"bad", AT, plant)["eligible"]
    event = build_snapshot(plant, AT)
    event["tenant_id"] = "foreign"
    assert not normalize_record(encode(event), AT, plant)["eligible"]
    assert not feature(plant, AT + timedelta(seconds=1), received=AT)["eligible"]


def test_training_selection_uses_separate_holdout(trained):
    registry, metrics = trained
    assert registry["approved"]
    assert metrics["test"]["count"] == 77
    assert set(metrics["validation"]) == {"linear_ridge", "neural_tanh"}


def test_shadow_and_simulated_control(plant, trained):
    registry, _ = trained
    row = feature(plant, AT + timedelta(seconds=1))
    shadow = decide(plant, row, registry, AT + timedelta(seconds=2))
    assert shadow["state"] == "BLOCKED" and shadow["reason"] == "SHADOW_ONLY"
    controlled = decide(plant, row, registry, AT + timedelta(seconds=2), simulate=True)
    assert controlled["state"] == "ACKNOWLEDGED"
    assert controlled["supply_c"] <= plant.min_supply_c + plant.max_step_c
    next_row = feature(plant, AT + timedelta(seconds=10))
    repeated = decide(
        plant, next_row, registry, AT + timedelta(seconds=11), simulate=True, previous=controlled
    )
    assert repeated["reason"] == "MINIMUM_COMMAND_INTERVAL"


@pytest.mark.parametrize("case", ["stale", "unsafe", "overlap", "rejected", "override", "interlock"])
def test_control_fail_closed(plant, trained, case):
    registry, _ = trained
    row = feature(plant, AT + timedelta(seconds=1))
    now = AT + timedelta(seconds=2)
    kwargs = {}
    if case == "stale":
        now += timedelta(minutes=2)
    elif case == "unsafe":
        kwargs["unsafe"] = True
    elif case == "overlap":
        row = feature(plant, AT - timedelta(minutes=15))
        now = AT
    elif case == "rejected":
        registry = {**registry, "approved": False}
    elif case == "override":
        kwargs["manual_override"] = True
    else:
        kwargs["interlock_ok"] = False
    assert decide(plant, row, registry, now, simulate=True, **kwargs)["state"] == "BLOCKED"


def test_producer_cli_reuses_repo_setup():
    assert parse_args(["--source", "plant", "--replay"]).replay
    assert parse_args([]).source == "air"
    with pytest.raises(SystemExit):
        parse_args(["--replay"])


def test_producer_reports_delivery_failure(monkeypatch):
    closed = []

    class FailedFuture:
        def get(self, timeout):
            raise RuntimeError("broker refused delivery")

    class Producer:
        def __init__(self, **kwargs):
            assert kwargs["max_in_flight_requests_per_connection"] == 1

        def send(self, *args, **kwargs):
            return FailedFuture()

        def close(self):
            closed.append(True)

    monkeypatch.setattr("kafka.KafkaProducer", Producer)
    with pytest.raises(RuntimeError, match="broker refused"):
        producer_main(["--count", "1", "--rate", "0", "--anomaly-rate", "0"])
    assert closed == [True]


def test_local_writer_lock_and_cloud_simulation_block(tmp_path):
    settings = Settings(root=tmp_path)
    with control_writer(settings), pytest.raises(RuntimeError, match="writer"), control_writer(settings):
        pass
    with control_writer(settings):
        pass  # Released even across separate file handles.
    with (
        pytest.raises(ValueError, match="shadow-only"),
        control_writer(Settings(catalog="c", schema="s"), simulate=True),
    ):
        pass


def test_acknowledged_plan_changes_next_raw_telemetry(plant, trained):
    registry, _ = trained
    at = AT + timedelta(seconds=1)
    observed = feature(plant, at)
    decision = decide(plant, observed, registry, at, simulate=True)
    assert decision["state"] == "ACKNOWLEDGED"
    raw = build_snapshot(
        plant, at + timedelta(seconds=1), applied_plan=decision["plan"], applied_decision_id="ack-1"
    )
    normalized = normalize_record(encode(raw), at + timedelta(seconds=1), plant)
    assert normalized["eligible"]
    values = json.loads(normalized["values_json"])
    assert values["ch-1"]["supply_temp_c"] == decision["supply_c"]
    assert raw["applied_decision_id"] == "ack-1"
    assert extract_features(normalized, plant)["power_kw"] != pytest.approx(observed["power_kw"])
