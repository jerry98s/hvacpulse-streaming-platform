"""Chronological training and control policy over the shared feature contract.

The learned model estimates load from measured context. It is NOT a learned
actuator response model or an RL policy; allocation uses configured mock curves.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict
from datetime import datetime

from plant.contracts import Features
from plant.contracts import Plant
from plant.control import MockController
from plant.models import Model
from plant.models import Observation
from plant.models import drift
from plant.models import evaluate
from plant.models import train
from plant.optimization import optimize


def features_from_row(row: dict, plant: Plant) -> Features:
    if row["scope"] != plant.scope.key or row["config_version"] != plant.version:
        raise ValueError("Feature scope/configuration mismatch")
    return Features(
        plant.scope,
        datetime.fromisoformat(row["captured_at"]),
        datetime.fromisoformat(row["available_at"]),
        row["outdoor_c"],
        row["humidity_pct"],
        row["wet_bulb_c"],
        row["occupancy_pct"],
        row["production_kw"],
        row["operating"],
    )


def fit_chronological(rows: list[dict], plant: Plant, *, epochs: int = 15) -> tuple[Model, dict]:
    points = sorted(
        [Observation(features_from_row(r, plant), r["cooling_kw"], r["power_kw"]) for r in rows],
        key=lambda p: p.features.time,
    )
    if len(points) < 96 or (points[-1].features.time - points[0].features.time).total_seconds() < 2 * 86400:
        raise ValueError("Need at least 96 clean snapshots spanning two days")
    if len({p.features.time for p in points}) != len(points):
        raise ValueError("Training timestamps must be unique")
    a, b = int(len(points) * 0.6), int(len(points) * 0.8)
    fit, validation, test = points[:a], points[a:b], points[b:]
    candidates = [train(fit), train(fit, neural=True, epochs=epochs)]
    validation_metrics = [evaluate(m, validation) for m in candidates]
    winner = min(range(2), key=lambda i: validation_metrics[i]["rmse_kw"])
    model = candidates[winner]
    metrics = evaluate(model, test)
    naive = math.sqrt(sum((p.cooling_kw - fit[-1].cooling_kw) ** 2 for p in test) / len(test))
    return model, {
        "validation": dict(zip([m.kind for m in candidates], validation_metrics, strict=True)),
        "test": metrics,
        "naive_test_rmse_kw": naive,
        "approved": metrics["cvrmse"] <= 0.15 and metrics["rmse_kw"] < naive,
        "holdout_through": test[-1].features.time.isoformat(),
        "split": "60% train / 20% selection / 20% untouched test, ordered by capture time",
        "purpose": "conditional current-load estimate, not a future-weather forecast",
    }


def decide(
    plant: Plant,
    feature: dict,
    registry: dict,
    now: datetime,
    *,
    simulate=False,
    previous: dict | None = None,
    unsafe=False,
    manual_override=False,
    interlock_ok=True,
) -> dict:
    """Fail-closed pure decision; caller persists the result before reporting success."""
    result = {
        "state": "BLOCKED",
        "reason": "",
        "plan": None,
        "audit": [],
        "supply_c": plant.min_supply_c,
        "last_applied": None,
        "synthetic": True,
    }
    if previous:
        result.update(supply_c=previous["supply_c"], last_applied=previous["last_applied"])
    context = features_from_row(feature, plant)
    if not registry["approved"]:
        result["reason"] = "MODEL_NOT_APPROVED"
        return result
    age = (now - context.time).total_seconds()
    if unsafe or not 0 <= age <= plant.max_age_seconds or context.available_at > now:
        result["reason"] = "STALE_OR_QUARANTINED_INPUT"
        return result
    model = Model.from_artifact(json.loads(registry["artifact_json"]))
    if context.time <= datetime.fromisoformat(registry["holdout_through"]):
        result["reason"] = "INPUT_OVERLAPS_TRAINING_OR_HOLDOUT"
        return result
    if not 0 <= (now - datetime.fromisoformat(registry["trained_at"])).total_seconds() <= 7 * 86400:
        result["reason"] = "MODEL_EXPIRED_OR_NOT_YET_AVAILABLE"
        return result
    if drift(model, [context])["alert"]:
        result["reason"] = "FEATURE_DRIFT"
        return result
    demand = model.predict(context)
    result["estimated_cooling_kw"] = demand
    # Current measured load is a safety floor, never evidence of model accuracy.
    demand = max(demand, feature["cooling_kw"])
    try:
        plan = optimize(plant, demand, context.wet_bulb_c, result["supply_c"], eligible=True)
    except ValueError:
        plan = None
    if plan is None:
        result["reason"] = "NO_FEASIBLE_PLAN"
        return result
    result["plan"] = asdict(plan)
    controller = MockController(plant, simulate_approval=simulate)
    controller.current_supply_c = result["supply_c"]
    controller.last_applied = (
        datetime.fromisoformat(result["last_applied"]) if result["last_applied"] else None
    )
    command = controller.propose(plan, now)
    if simulate:
        controller.approve(command, "explicit-mock-mode", now)
    state = controller.execute(
        command, now, eligible=True, interlock_ok=interlock_ok, manual_override=manual_override
    )
    result.update(
        state=state,
        audit=controller.audit,
        supply_c=controller.current_supply_c,
        last_applied=controller.last_applied.isoformat() if controller.last_applied else None,
        reason=controller.audit[-1].get("reason", "MOCK_ACKNOWLEDGED"),
    )
    return result
