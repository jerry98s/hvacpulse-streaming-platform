"""Small dependency-free reference models, with chronological evaluation.

These are mock-site models, not commissioned HVAC models. A tanh neural network
is compared with a weather/process-normalized linear regression challenger.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from statistics import mean
from statistics import pstdev

from plant.contracts import Features
from plant.contracts import Scope
from plant.contracts import aware
from plant.contracts import finite

FEATURE_NAMES = (
    "outdoor_c",
    "humidity_pct",
    "wet_bulb_c",
    "occupancy_pct",
    "production_kw",
    "operating",
    "hour_sin",
    "hour_cos",
)


@dataclass(frozen=True)
class Observation:
    features: Features
    cooling_kw: float
    power_kw: float
    eligible: bool = True
    exclusions: tuple[str, ...] = ()

    def __post_init__(self):
        for name in ("cooling_kw", "power_kw"):
            finite(getattr(self, name), name)
            if getattr(self, name) < 0:
                raise ValueError("Observed load/power cannot be negative")
        if self.features.available_at < self.features.time:
            raise ValueError("Observed features cannot be available before measurement time")


@dataclass
class Model:
    scope: Scope
    kind: str
    target: str
    trained_from: datetime
    trained_through: datetime
    center: list[float]
    scale: list[float]
    target_center: float
    target_scale: float
    weights: list[list[float]]
    output: list[float]
    training_count: int

    def predict(self, features: Features) -> float:
        if features.scope != self.scope:
            raise ValueError("Model/feature tenant, site or plant mismatch")
        x = [
            (value - center) / scale
            for value, center, scale in zip(features.vector(), self.center, self.scale, strict=True)
        ] + [1.0]
        if self.kind == "neural_tanh":
            x = [
                math.tanh(sum(w * value for w, value in zip(row, x, strict=True))) for row in self.weights
            ] + [1.0]
        prediction = self.target_center + self.target_scale * sum(
            w * value for w, value in zip(self.output, x, strict=True)
        )
        finite(prediction, "prediction")
        return max(0.0, prediction)

    def artifact(self) -> dict:
        artifact = asdict(self)
        artifact.update(
            schema_version=1,
            synthetic=True,
            feature_names=list(FEATURE_NAMES),
            trained_from=self.trained_from.isoformat(),
            trained_through=self.trained_through.isoformat(),
        )
        artifact["model_id"] = hashlib.sha256(json.dumps(artifact, sort_keys=True).encode()).hexdigest()
        return artifact

    @classmethod
    def from_artifact(cls, artifact: dict) -> Model:
        data = dict(artifact)
        expected_id = data.pop("model_id")
        actual_id = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
        if expected_id != actual_id or data.pop("schema_version") != 1 or data.pop("synthetic") is not True:
            raise ValueError("Invalid or modified mock model artifact")
        if data.pop("feature_names") != list(FEATURE_NAMES):
            raise ValueError("Model feature contract mismatch")
        model = cls(
            **{
                **data,
                "scope": Scope(**data["scope"]),
                "trained_from": datetime.fromisoformat(data["trained_from"]),
                "trained_through": datetime.fromisoformat(data["trained_through"]),
            }
        )
        aware(model.trained_from)
        aware(model.trained_through)
        if model.kind not in {"neural_tanh", "linear_ridge"} or model.target not in {
            "cooling_kw",
            "power_kw",
        }:
            raise ValueError("Unsupported model type")
        width = len(FEATURE_NAMES)
        if len(model.center) != width or len(model.scale) != width or any(s <= 0 for s in model.scale):
            raise ValueError("Invalid feature normalization")
        if model.kind == "neural_tanh":
            if not model.weights or any(len(row) != width + 1 for row in model.weights):
                raise ValueError("Invalid hidden-layer shape")
            expected_width = len(model.weights) + 1
        else:
            expected_width = width + 1
        if len(model.output) != expected_width or model.target_scale <= 0:
            raise ValueError("Invalid output shape or scale")
        for value in [
            *model.center,
            *model.scale,
            model.target_center,
            model.target_scale,
            *model.output,
            *(value for row in model.weights for value in row),
        ]:
            finite(value, "model parameter")
        return model


def _solve(matrix: list[list[float]], rhs: list[float]) -> list[float]:
    """Pivoted elimination of a small regularized normal equation."""
    size = len(rhs)
    augmented = [list(row) + [value] for row, value in zip(matrix, rhs, strict=True)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        if abs(divisor) < 1e-12:
            raise ValueError("Singular training matrix")
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row != column:
                multiplier = augmented[row][column]
                augmented[row] = [
                    a - multiplier * b for a, b in zip(augmented[row], augmented[column], strict=True)
                ]
    return [row[-1] for row in augmented]


def train(
    points: list[Observation],
    *,
    target: str = "cooling_kw",
    neural: bool = False,
    seed: int = 42,
    epochs: int = 45,
) -> Model:
    if target not in {"cooling_kw", "power_kw"} or not 1 <= epochs <= 500:
        raise ValueError("Unsupported target or training epoch count")
    if not points or any(point.features.scope != points[0].features.scope for point in points):
        raise ValueError("Training must contain exactly one tenant/site/plant")
    good = sorted(
        (point for point in points if point.eligible and not point.exclusions),
        key=lambda point: point.features.time,
    )
    if len(good) < 32 or len({point.features.time for point in good}) != len(good):
        raise ValueError("Need at least 32 distinct eligible training timestamps")
    raw = [point.features.vector() for point in good]
    center = [mean(column) for column in zip(*raw, strict=True)]
    scale = [max(pstdev(column), 1.0) for column in zip(*raw, strict=True)]
    targets = [getattr(point, target) for point in good]
    target_center, target_scale = mean(targets), max(pstdev(targets), 1.0)
    xs = [[(value - c) / s for value, c, s in zip(row, center, scale, strict=True)] + [1.0] for row in raw]
    ys = [(value - target_center) / target_scale for value in targets]
    rng = random.Random(seed)
    weights = []
    if neural:
        weights = [[rng.uniform(-0.3, 0.3) for _ in xs[0]] for _ in range(10)]
        output = [rng.uniform(-0.1, 0.1) for _ in range(10)] + [0.0]
        order = list(range(len(xs)))
        for _ in range(epochs):
            rng.shuffle(order)
            for index in order:
                x = xs[index]
                hidden = [
                    math.tanh(sum(w * value for w, value in zip(row, x, strict=True))) for row in weights
                ]
                error = sum(w * value for w, value in zip(output, hidden + [1.0], strict=True)) - ys[index]
                gradients = [
                    max(-5, min(5, error * output[j] * (1 - hidden[j] ** 2))) for j in range(len(hidden))
                ]
                for j, activation in enumerate(hidden + [1.0]):
                    output[j] -= 0.008 * max(-5, min(5, error * activation))
                for row, gradient in zip(weights, gradients, strict=True):
                    for j, value in enumerate(x):
                        row[j] -= 0.008 * gradient * value
    else:
        width = len(xs[0])
        matrix = [
            [sum(row[i] * row[j] for row in xs) + (0.01 if i == j else 0) for j in range(width)]
            for i in range(width)
        ]
        rhs = [sum(row[i] * value for row, value in zip(xs, ys, strict=True)) for i in range(width)]
        output = _solve(matrix, rhs)
    return Model(
        good[0].features.scope,
        "neural_tanh" if neural else "linear_ridge",
        target,
        good[0].features.time,
        # Observation-time boundary; the registry separately records ingestion
        # availability and fit time. Historical backfills may arrive together.
        good[-1].features.time,
        center,
        scale,
        target_center,
        target_scale,
        weights,
        output,
        len(good),
    )


def evaluate(model: Model, points: list[Observation]) -> dict:
    if any(point.features.time <= model.trained_through for point in points):
        raise ValueError("Evaluation must be strictly after training; random splits leak time")
    good = [point for point in points if point.eligible and not point.exclusions]
    if not good:
        raise ValueError("No eligible evaluation observations")
    errors = [model.predict(point.features) - getattr(point, model.target) for point in good]
    actual = [getattr(point, model.target) for point in good]
    rmse = math.sqrt(mean(error**2 for error in errors))
    return {
        "count": len(good),
        "excluded": len(points) - len(good),
        "mae_kw": mean(map(abs, errors)),
        "rmse_kw": rmse,
        "bias_kw": mean(errors),
        "cvrmse": rmse / max(mean(actual), 1e-6),
    }


def forecast(model: Model, issued_at: datetime, features: list[Features]) -> list[dict]:
    aware(issued_at)
    if model.target != "cooling_kw" or issued_at <= model.trained_through:
        raise ValueError("Forecast requires a previously trained cooling-load model")
    expected = [issued_at + timedelta(minutes=15 * step) for step in range(1, 25)]
    if [item.time for item in features] != expected:
        raise ValueError("Forecast requires exactly 24 ordered, 15-minute horizons (six hours)")
    if any(item.available_at > issued_at for item in features):
        raise ValueError("Future observed data cannot be used as a forecast input")
    model_id = model.artifact()["model_id"]
    return [
        {
            "scope": asdict(model.scope),
            "issued_at": issued_at.isoformat(),
            "target_time": item.time.isoformat(),
            "horizon_minutes": step * 15,
            "cooling_kw": model.predict(item),
            "model_id": model_id,
            "synthetic": True,
        }
        for step, item in enumerate(features, start=1)
    ]


def drift(model: Model, features: list[Features], threshold: float = 3.0) -> dict:
    if not features or any(item.scope != model.scope for item in features):
        raise ValueError("Drift window must be nonempty and match the model scope")
    centers = [mean(column) for column in zip(*(item.vector() for item in features), strict=True)]
    shifts = {
        name: abs(value - center) / scale
        for name, value, center, scale in zip(FEATURE_NAMES, centers, model.center, model.scale, strict=True)
    }
    return {
        "mean_shift_standard_deviations": shifts,
        "alert": any(v > threshold for v in shifts.values()),
        "method": "mean-shift diagnostic; does not detect every kind of drift",
    }
