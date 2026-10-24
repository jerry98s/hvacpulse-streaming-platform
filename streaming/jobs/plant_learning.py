"""Train from Delta features; estimate load and persist gated mock control decisions.

One serialized writer per plant is required. No hardware I/O is implemented.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from pyspark.sql import functions as F

from ingestion.plant_events import timestamp
from plant.contracts import Plant
from plant.learning import decide
from plant.learning import fit_chronological
from streaming.config import load_settings
from streaming.plant_store import bounded_rows
from streaming.plant_store import control_writer
from streaming.plant_store import merge_rows
from streaming.plant_transforms import FEATURE_VERSION
from streaming.plant_transforms import identity
from streaming.spark_session import create_spark

REGISTRY_SCHEMA = """model_id string, scope string, config_version string, feature_version string,
 trained_at string, holdout_through string, approved boolean, artifact_json string, metrics_json string, lineage_json string"""
DECISION_SCHEMA = """decision_id string, scope string, config_version string, model_id string,
 feature_id string, captured_at string, decided_at string, mode string, state string, reason string, result_json string"""
PREDICTION_SCHEMA = """prediction_id string, scope string, model_id string, feature_id string,
 captured_at string, issued_at string, estimated_cooling_kw double, synthetic boolean, purpose string"""


def feature_source(spark, settings, plant, now, *, version=None):
    if version is None:
        source = settings.read_delta(spark, settings.plant_features_path)
    else:
        reader = spark.read.format("delta").option("versionAsOf", version)
        source = (
            reader.table(settings.plant_features_path)
            if settings.uses_unity_catalog
            else reader.load(settings.plant_features_path)
        )
    frame = source.filter(
        (F.col("scope") == plant.scope.key)
        & (F.col("config_version") == plant.version)
        & (F.col("feature_version") == FEATURE_VERSION)
        & (F.col("available_at") <= timestamp(now))
        & (F.col("captured_at") <= timestamp(now))
    )
    if settings.delta_exists(spark, settings.plant_quarantine_path):
        unsafe = settings.read_delta(spark, settings.plant_quarantine_path).select("row_id")
        frame = frame.join(unsafe, "row_id", "left_anti")
    return frame


def train_model(spark, settings, *, now=None, days=30, epochs=15):
    if not 2 <= days <= 365:
        raise ValueError("Training window must be between 2 and 365 days")
    now = now or datetime.now(timezone.utc)
    plant = Plant.load(settings.plant_config)
    version = settings.delta_table(spark, settings.plant_features_path).history(1).first().version
    frame = feature_source(spark, settings, plant, now, version=version).filter(
        F.col("captured_at") >= timestamp(now - timedelta(days=days))
    )
    rows = bounded_rows(frame.orderBy("captured_at"))
    model, metrics = fit_chronological(rows, plant, epochs=epochs)
    artifact = model.artifact()
    lineage = {
        "source": settings.plant_features_path,
        "as_of": timestamp(now),
        "delta_version": version,
        "feature_ids": [r["row_id"] for r in rows],
        "payload_hashes": [r["payload_hash"] for r in rows],
    }
    model_id = identity(artifact["model_id"], lineage["feature_ids"], lineage["payload_hashes"])
    row = dict(
        model_id=model_id,
        scope=plant.scope.key,
        config_version=plant.version,
        feature_version=FEATURE_VERSION,
        trained_at=timestamp(now),
        holdout_through=timestamp(datetime.fromisoformat(metrics["holdout_through"])),
        approved=metrics["approved"],
        artifact_json=json.dumps(artifact),
        metrics_json=json.dumps(metrics),
        lineage_json=json.dumps(lineage),
    )
    merge_rows(
        spark,
        settings,
        spark.createDataFrame([row], REGISTRY_SCHEMA),
        settings.plant_models_path,
        ["model_id"],
    )
    return row


def predict_and_control(
    spark, settings, *, now=None, simulate=False, manual_override=False, interlock_ok=True
):
    with control_writer(settings, simulate=simulate):
        return _predict_and_control(
            spark,
            settings,
            now=now,
            simulate=simulate,
            manual_override=manual_override,
            interlock_ok=interlock_ok,
        )


def _predict_and_control(
    spark, settings, *, now=None, simulate=False, manual_override=False, interlock_ok=True
):
    wall_clock = now is None
    now = now or datetime.now(timezone.utc)
    plant = Plant.load(settings.plant_config)
    rows = bounded_rows(
        feature_source(spark, settings, plant, now).orderBy(F.col("captured_at").desc()).limit(1)
    )
    if not rows:
        raise ValueError("No eligible plant features available")
    feature = rows[0]
    if not settings.delta_exists(spark, settings.plant_models_path):
        raise ValueError("Train a plant model before prediction")
    models = bounded_rows(
        settings.read_delta(spark, settings.plant_models_path)
        .filter(
            (F.col("scope") == plant.scope.key)
            & (F.col("config_version") == plant.version)
            & (F.col("feature_version") == FEATURE_VERSION)
            & (F.col("trained_at") <= timestamp(now))
        )
        .orderBy(F.col("trained_at").desc(), F.col("model_id").desc())
        .limit(1)
    )
    if not models:
        raise ValueError("No model matching the current plant configuration")
    registry = models[0]
    # A feature can be simulated at most once, even if a new model is trained.
    mode = "simulate" if simulate else "shadow"
    decision_id = identity(plant.scope.key, feature["row_id"], mode)
    previous = None
    if settings.delta_exists(spark, settings.plant_decisions_path):
        history = settings.read_delta(spark, settings.plant_decisions_path)
        existing = history.filter(F.col("decision_id") == decision_id).first()
        if existing:
            return existing.asDict()
        applied = (
            history.filter(
                (F.col("scope") == plant.scope.key)
                & (F.col("config_version") == plant.version)
                & (F.col("state") == "ACKNOWLEDGED")
            )
            .orderBy(F.col("decided_at").desc())
            .first()
        )
        if applied:
            previous = json.loads(applied.result_json)
    unsafe = False
    if settings.delta_exists(spark, settings.plant_quarantine_path):
        quarantine = settings.read_delta(spark, settings.plant_quarantine_path)
        unsafe = not quarantine.filter(
            (F.col("scope").isNull() | (F.col("scope") == plant.scope.key))
            & (F.col("available_at") >= feature["available_at"])
        ).isEmpty()
        # Any conflict in training lineage revokes the model at use time.
        lineage_ids = json.loads(registry["lineage_json"])["feature_ids"]
        if not quarantine.filter(F.col("row_id").isin(lineage_ids)).isEmpty():
            registry = {**registry, "approved": False}
    # Recheck freshness at decision time, not before potentially slow Delta reads.
    if wall_clock:
        now = datetime.now(timezone.utc)
    result = decide(
        plant,
        feature,
        registry,
        now,
        simulate=simulate,
        previous=previous,
        unsafe=unsafe,
        manual_override=manual_override,
        interlock_ok=interlock_ok,
    )
    row = dict(
        decision_id=decision_id,
        scope=plant.scope.key,
        config_version=plant.version,
        model_id=registry["model_id"],
        feature_id=feature["row_id"],
        captured_at=feature["captured_at"],
        decided_at=timestamp(now),
        mode=mode,
        state=result["state"],
        reason=result["reason"],
        result_json=json.dumps(result),
    )
    # Prediction first, terminal decision second: retry repairs partial publication.
    if "estimated_cooling_kw" in result:
        prediction = dict(
            prediction_id=identity(registry["model_id"], feature["row_id"]),
            scope=plant.scope.key,
            model_id=registry["model_id"],
            feature_id=feature["row_id"],
            captured_at=feature["captured_at"],
            issued_at=timestamp(now),
            estimated_cooling_kw=result["estimated_cooling_kw"],
            synthetic=True,
            purpose="current-load estimate; not a future forecast",
        )
        merge_rows(
            spark,
            settings,
            spark.createDataFrame([prediction], PREDICTION_SCHEMA),
            settings.plant_predictions_path,
            ["prediction_id"],
        )
    merge_rows(
        spark,
        settings,
        spark.createDataFrame([row], DECISION_SCHEMA),
        settings.plant_decisions_path,
        ["decision_id"],
    )
    return row


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["train", "control"])
    parser.add_argument(
        "--simulate", action="store_true", help="explicit mock acknowledgement; never hardware control"
    )
    parser.add_argument("--manual-override", action="store_true")
    parser.add_argument("--interlock-failed", action="store_true")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args(argv)
    spark = create_spark("hvacpulse-plant-learning", delta=True)
    settings = load_settings()
    if args.action == "train":
        result = train_model(spark, settings, days=args.days)
        print(json.dumps({key: result[key] for key in ("model_id", "approved", "metrics_json")}))
    else:
        result = predict_and_control(
            spark,
            settings,
            simulate=args.simulate,
            manual_override=args.manual_override,
            interlock_ok=not args.interlock_failed,
        )
        print(json.dumps({key: result[key] for key in ("decision_id", "state", "reason")}))


if __name__ == "__main__":
    main()
