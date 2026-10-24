"""Offline regression tests for shared plant domain logic."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest

from ingestion.plant_events import build_snapshot
from ingestion.plant_events import encode
from ingestion.plant_simulator import features_at
from ingestion.plant_simulator import mock_readings
from plant.contracts import Features
from plant.contracts import Plant
from plant.contracts import Reading
from plant.contracts import Scope
from plant.control import MockController
from plant.learning import features_from_row
from plant.models import Model
from plant.models import Observation
from plant.models import drift
from plant.models import evaluate
from plant.models import forecast
from plant.models import train
from plant.optimization import optimize
from plant.telemetry import normalize
from plant.verification import Outcome
from plant.verification import verify
from streaming.plant_transforms import extract_features
from streaming.plant_transforms import normalize_record

pytestmark = pytest.mark.unit
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
CONFIG = Path(__file__).resolve().parents[2] / "reference/mock_plant.json"


def observe(plant, time, *, fault=None):
    """Tests use the production transformations, not a second demo pipeline."""
    row = normalize_record(encode(build_snapshot(plant, time, fault=fault)), time, plant)
    audit = {**row, "issues": json.loads(row["issues"])}
    if not row["eligible"]:
        return Observation(features_at(plant, time), 0, 0, False, tuple(audit["issues"])), audit
    row.update(extract_features(row, plant))
    return Observation(features_from_row(row, plant), row["cooling_kw"], row["power_kw"]), audit


@pytest.fixture(scope="module")
def plant():
    return Plant.load(CONFIG)


@pytest.fixture(scope="module")
def observations(plant):
    return [observe(plant, START + timedelta(minutes=15 * index))[0] for index in range(5 * 96)]


@pytest.fixture(scope="module")
def model(observations):
    return train(observations[: 3 * 96], neural=True, epochs=4)


def test_plant_models_full_equipment_and_loops(plant):
    assert {item.kind for item in plant.equipment} == {
        "chiller",
        "cooling_tower",
        "chw_pump",
        "cw_pump",
        "vfd",
        "process",
    }
    assert {item[2] for item in plant.connections} == {"cw", "chw", "drive"}
    assert plant.scope.key == "demo-tenant/mock-factory/chiller-plant-1"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: replace(p, tags=p.tags + (p.tags[0],)),
        lambda p: replace(p, equipment=p.equipment + (p.equipment[0],)),
        lambda p: replace(p, connections=(("missing", "ch-1", "chw"),)),
        lambda p: replace(p, max_supply_c=float("nan")),
        lambda p: replace(p, max_step_c=-1),
        lambda p: replace(p, tags=(replace(p.tags[0], unit="miles"),)),
        lambda p: replace(p, reserve_fraction=2),
        lambda p: replace(p, tags=(replace(p.tags[0], calibration_due="2027-01-01"),)),
    ],
)
def test_invalid_configuration_rejected(plant, mutate):
    with pytest.raises(ValueError):
        mutate(plant)


def test_scope_and_feature_validation(plant):
    with pytest.raises(ValueError):
        Scope("../customer", "site", "plant")
    with pytest.raises(ValueError):
        Features(plant.scope, START, START, 30, float("nan"), 25, 50, 100, True)
    with pytest.raises(ValueError):
        Reading("tag", 1.0, START.replace(tzinfo=None))


def test_mock_unit_conversion_and_heat_balance(plant):
    readings = mock_readings(plant, START)
    snapshot = normalize(plant, readings, START)
    assert snapshot.eligible
    assert snapshot.values["ch-2"]["supply_temp_c"] == pytest.approx(6)
    assert snapshot.values["chw-pump-1"]["differential_pressure_kpa"] == pytest.approx(90)
    assert snapshot.values["vfd-1"]["speed_pct"] == pytest.approx(70)
    observation, audit = observe(plant, START)
    assert observation.eligible and observation.cooling_kw > 0 and audit["synthetic"]


@pytest.mark.parametrize(
    "fault,reason", [("missing", "MISSING_TAG"), ("stale", "STALE"), ("bad", "SOURCE_QUALITY")]
)
def test_faults_quarantine_and_block_features(plant, fault, reason):
    observation, audit = observe(plant, START, fault=fault)
    assert not observation.eligible
    assert any(reason in issue for issue in audit["issues"])
    assert observation.cooling_kw == 0


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -100, True, None, "6"])
def test_unsafe_numeric_samples_blocked(plant, value):
    readings = mock_readings(plant, START)
    readings[0] = replace(readings[0], value=value)
    snapshot = normalize(plant, readings, START)
    assert not snapshot.eligible
    assert "supply_temp_c" not in snapshot.values["ch-1"]


def test_timestamp_alignment_duplicates_and_calibration(plant):
    readings = mock_readings(plant, START)
    altered = [replace(readings[0], source_time=START - timedelta(seconds=20)), *readings[1:]]
    assert "TIME_ALIGNMENT" in normalize(plant, altered, START).issues
    assert not normalize(plant, readings + [readings[0]], START).eligible
    future = [replace(readings[0], source_time=START + timedelta(seconds=1)), *readings[1:]]
    assert "FUTURE_TIMESTAMP" in normalize(plant, future, START).issues[0]
    expired = replace(
        plant, tags=(replace(plant.tags[0], calibration_due=START.isoformat()), *plant.tags[1:])
    )
    assert "CALIBRATION_EXPIRED" in normalize(expired, readings, START).issues[0]


def test_neural_roundtrip_and_scope_isolation(model, observations):
    restored = Model.from_artifact(json.loads(json.dumps(model.artifact())))
    features = observations[-1].features
    assert restored.predict(features) == pytest.approx(model.predict(features))
    with pytest.raises(ValueError, match="mismatch"):
        restored.predict(replace(features, scope=Scope("other-tenant", "mock-factory", "chiller-plant-1")))
    artifact = model.artifact()
    artifact["output"][0] += 1
    with pytest.raises(ValueError, match="modified"):
        Model.from_artifact(artifact)


def test_training_rejects_mixed_scope_and_duplicate_times(observations):
    with pytest.raises(ValueError, match="distinct"):
        train([observations[0]] * 32)
    foreign = replace(
        observations[0], features=replace(observations[0].features, scope=Scope("foreign", "s", "p"))
    )
    with pytest.raises(ValueError, match="exactly one"):
        train([*observations[:32], foreign])
    with pytest.raises(ValueError, match="eligible"):
        train([replace(point, eligible=False) for point in observations[:40]])


def test_chronological_holdout_does_not_leak(model, observations):
    with pytest.raises(ValueError, match="strictly after"):
        evaluate(model, observations[:96])
    metrics = evaluate(model, observations[3 * 96 :])
    assert metrics["count"] == 192
    assert metrics["rmse_kw"] >= 0


def test_forecast_has_all_24_horizons_and_no_future_observations(plant, model, observations):
    issued = observations[-1].features.time
    features = [
        features_at(plant, issued + timedelta(minutes=15 * step), issued_at=issued) for step in range(1, 25)
    ]
    result = forecast(model, issued, features)
    assert [row["horizon_minutes"] for row in result] == list(range(15, 361, 15))
    with pytest.raises(ValueError, match="Future observed"):
        forecast(model, issued, [replace(item, available_at=item.time) for item in features])
    with pytest.raises(ValueError, match="24 ordered"):
        forecast(model, issued, features[:-1])
    shifted = [replace(item, production_kw=item.production_kw + 2000) for item in features]
    assert drift(model, shifted)["alert"]


def test_optimizer_balances_load_and_respects_equipment_and_rate(plant):
    plan = optimize(plant, 600, 25, 6, eligible=True)
    assert plan is not None and 6 <= plan.supply_c <= 6.5
    assert sum(plan.allocations_kw.values()) == pytest.approx(600 * 1.05)
    assert plan.flow_m3h <= plant.max_flow_m3h
    for item in plant.equipment:
        if item.equipment_id in plan.allocations_kw:
            assert (
                item.capacity_kw * item.min_load_fraction
                <= plan.allocations_kw[item.equipment_id]
                <= item.capacity_kw
            )
    assert optimize(plant, 600, 25, 6, eligible=True) == plan
    assert optimize(plant, 600, 25, 6, eligible=False) is None
    assert optimize(plant, 2000, 25, 6, eligible=True) is None
    assert optimize(plant, 600, 25, 6, eligible=True, unavailable=("chw-pump-1",)) is None
    assert optimize(plant, 600, 25, 6, eligible=True, unavailable=("ch-1", "ch-2")) is None


def test_control_shadow_default_and_ack_idempotency(plant):
    plan = optimize(plant, 600, 25, 6, eligible=True)
    shadow = MockController(plant)
    command = shadow.propose(plan, START)
    with pytest.raises(ValueError, match="Explicit"):
        shadow.approve(command, "operator", START)
    assert (
        shadow.execute(command, START, eligible=True, interlock_ok=True, manual_override=False) == "BLOCKED"
    )
    assert shadow.current_supply_c == 6
    controller = MockController(plant, simulate_approval=True)
    command = controller.propose(plan, START)
    controller.approve(command, "mock-operator", START)
    assert (
        controller.execute(command, START, eligible=True, interlock_ok=True, manual_override=False)
        == "ACKNOWLEDGED"
    )
    count = len(controller.audit)
    assert (
        controller.execute(command, START, eligible=True, interlock_ok=True, manual_override=False)
        == "ACKNOWLEDGED"
    )
    assert len(controller.audit) == count


@pytest.mark.parametrize(
    "kwargs,state",
    [
        ({"eligible": False}, "BLOCKED"),
        ({"interlock_ok": False}, "BLOCKED"),
        ({"manual_override": True}, "BLOCKED"),
        ({"ack": "timeout"}, "TIMED_OUT"),
        ({"ack": "mismatch"}, "ACK_MISMATCH"),
    ],
)
def test_control_fail_closed_and_mock_rollback(plant, kwargs, state):
    controller = MockController(plant, simulate_approval=True)
    plan = optimize(plant, 600, 25, 6, eligible=True)
    command = controller.propose(plan, START)
    controller.approve(command, "mock-operator", START)
    args = {"eligible": True, "interlock_ok": True, "manual_override": False, **kwargs}
    assert controller.execute(command, START, **args) == state
    assert controller.current_supply_c == 6
    if state != "BLOCKED":
        assert controller.audit[-1]["state"] == "MOCK_ROLLBACK_CONFIRMED"


def test_control_expiry_limits_scope_and_minimum_interval(plant):
    plan = optimize(plant, 600, 25, 6, eligible=True)
    controller = MockController(plant, simulate_approval=True)
    with pytest.raises(ValueError, match="scope"):
        controller.propose(replace(plan, scope=Scope("other", "s", "p")), START)
    with pytest.raises(ValueError, match="capacity"):
        controller.propose(replace(plan, allocations_kw={"ch-1": 99999}), START)
    for target, now in [(10, START), (7, START), (6.5, START + timedelta(seconds=61))]:
        local = MockController(plant, simulate_approval=True)
        command = local.propose(replace(plan, supply_c=target), START)
        local.approve(command, "operator", START)
        assert (
            local.execute(command, now, eligible=True, interlock_ok=True, manual_override=False) == "BLOCKED"
        )
    command = controller.propose(plan, START)
    controller.approve(command, "operator", START)
    controller.execute(command, START, eligible=True, interlock_ok=True, manual_override=False)
    now = START + timedelta(seconds=20)
    command = controller.propose(plan, now)
    controller.approve(command, "operator", now)
    assert (
        controller.execute(command, now, eligible=True, interlock_ok=True, manual_override=False) == "BLOCKED"
    )


def test_savings_handles_shadow_negative_savings_and_reporting_gaps(observations):
    baseline = train(observations[: 3 * 96], target="power_kw")
    holdout, reporting = observations[3 * 96 : 4 * 96], observations[4 * 96 :]
    outcomes = [Outcome(point, point.power_kw + 100, True, 0.2, 0.5) for point in reporting]
    result = verify(baseline, holdout, outcomes)
    assert result["savings_kwh"] < 0 and result["verified"] is False
    assert result["cost_savings"] == pytest.approx(result["savings_kwh"] * 0.2)
    assert result["carbon_savings_kg"] == pytest.approx(result["savings_kwh"] * 0.5)
    assert result["savings_kwh"] == pytest.approx(sum(row["savings_kwh"] for row in result["intervals"]))
    shadow = verify(baseline, holdout, [replace(item, ai_on=False) for item in outcomes])
    assert shadow["savings_kwh"] is None and shadow["eligible_intervals"] == 0
    with pytest.raises(ValueError, match="contiguous"):
        verify(baseline, holdout, outcomes[::2])
    with pytest.raises(ValueError, match="follow"):
        verify(baseline, holdout, [Outcome(holdout[0], 1, True, 0.2, 0.5)])
