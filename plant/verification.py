"""Synthetic M&V demonstration. Never emits a verified-savings claim."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta

from plant.contracts import finite
from plant.models import Model
from plant.models import Observation
from plant.models import evaluate


@dataclass(frozen=True)
class Outcome:
    observation: Observation
    measured_power_kw: float
    ai_on: bool
    tariff_per_kwh: float
    carbon_kg_per_kwh: float

    def __post_init__(self):
        for name in ("measured_power_kw", "tariff_per_kwh", "carbon_kg_per_kwh"):
            finite(getattr(self, name), name)
            if getattr(self, name) < 0:
                raise ValueError("Power, tariffs and carbon factors must be nonnegative")


def verify(baseline: Model, holdout: list[Observation], outcomes: list[Outcome]) -> dict:
    if baseline.target != "power_kw":
        raise ValueError("M&V requires an independently fitted baseline power model")
    metrics = evaluate(baseline, holdout)
    if not outcomes or not holdout:
        raise ValueError("Need holdout and reporting windows")
    times = [item.observation.features.time for item in outcomes]
    if times[0] <= max(point.features.time for point in holdout):
        raise ValueError("Reporting must follow baseline holdout")
    if any(right - left != timedelta(minutes=15) for left, right in zip(times, times[1:], strict=False)):
        raise ValueError("Reporting requires unique, ordered, contiguous 15-minute intervals")
    if any(item.observation.features.scope != baseline.scope for item in outcomes):
        raise ValueError("Cross-tenant/site/plant reporting is forbidden")
    rows = []
    exclusions: dict[str, int] = {}
    for item in outcomes:
        point = item.observation
        reasons = list(point.exclusions)
        if not point.eligible and not reasons:
            reasons.append("DATA_QUALITY")
        if not item.ai_on:
            reasons.append("AI_OFF_OR_SHADOW")
        if metrics["cvrmse"] > 0.15:
            reasons.append("BASELINE_HOLDOUT_ERROR")
        if reasons:
            for reason in set(reasons):
                exclusions[reason] = exclusions.get(reason, 0) + 1
            continue
        baseline_kwh = baseline.predict(point.features) * 0.25
        measured_kwh = item.measured_power_kw * 0.25
        savings = baseline_kwh - measured_kwh  # Negative savings are retained.
        rows.append(
            {
                "time": point.features.time.isoformat(),
                "baseline_kwh": baseline_kwh,
                "measured_kwh": measured_kwh,
                "savings_kwh": savings,
                "cost_savings": savings * item.tariff_per_kwh,
                "carbon_savings_kg": savings * item.carbon_kg_per_kwh,
            }
        )
    savings = sum(row["savings_kwh"] for row in rows)
    # Conservative diagnostic envelope assuming fully correlated holdout errors.
    # It is NOT a calibrated confidence interval, especially for synthetic data.
    uncertainty = 1.96 * metrics["rmse_kw"] * len(rows) * 0.25
    if not math.isfinite(uncertainty):
        raise ValueError("Invalid uncertainty estimate")
    return {
        "status": "SYNTHETIC_ESTIMATE_NOT_VERIFIED" if rows else "INSUFFICIENT_ELIGIBLE_AI_ON_DATA",
        "verified": False,
        "synthetic": True,
        "baseline_model_id": baseline.artifact()["model_id"],
        "baseline_holdout": metrics,
        "eligible_intervals": len(rows),
        "total_intervals": len(outcomes),
        "exclusions": exclusions,
        "savings_kwh": savings if rows else None,
        "baseline_kwh": sum(row["baseline_kwh"] for row in rows) if rows else None,
        "measured_kwh": sum(row["measured_kwh"] for row in rows) if rows else None,
        "cost_savings": sum(row["cost_savings"] for row in rows) if rows else None,
        "carbon_savings_kg": sum(row["carbon_savings_kg"] for row in rows) if rows else None,
        "diagnostic_band_kwh": [savings - uncertainty, savings + uncertainty] if rows else None,
        "uncertainty_method": "1.96 * holdout RMSE * eligible hours; diagnostic, not a confidence interval",
        "limitations": [
            "Synthetic meters, tariffs and carbon factors; no customer-approved baseline",
            "Not IPMVP certification or independent verification",
            "AI-on subset is selected, not a randomized causal comparison",
            "No demand charges, tariff tiers or calibrated interval coverage",
        ],
        "intervals": rows,
    }
