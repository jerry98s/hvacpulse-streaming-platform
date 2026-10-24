"""Orchestrate the existing Kafka/Spark jobs for mock bootstrap or one feedback cycle.

Bootstrap collects history and trains once. A cycle collects a fresh snapshot,
processes it through Bronze/Silver/features and evaluates a control decision.
Explicit --simulate feeds the last acknowledged plan into the next mock reading.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from datetime import timezone

from pyspark.sql import functions as F

from ingestion.plant_events import build_snapshot
from ingestion.plant_events import encode
from ingestion.producer import main as produce
from plant.contracts import Plant
from streaming.config import load_settings
from streaming.jobs.bronze_ingest import run as bronze_run
from streaming.jobs.plant_features import run as features_run
from streaming.jobs.plant_learning import predict_and_control
from streaming.jobs.plant_learning import train_model
from streaming.jobs.plant_silver import run as silver_run
from streaming.spark_session import create_spark


def refresh(settings):
    bronze_run(settings)
    silver_run(settings)
    features_run(settings)


def feedback(spark, settings, plant):
    if not settings.delta_exists(spark, settings.plant_decisions_path):
        return None, None
    previous = (
        settings.read_delta(spark, settings.plant_decisions_path)
        .filter(
            (F.col("scope") == plant.scope.key)
            & (F.col("config_version") == plant.version)
            & (F.col("state") == "ACKNOWLEDGED")
        )
        .orderBy(F.col("decided_at").desc())
        .first()
    )
    if previous is None:
        return None, None
    return json.loads(previous.result_json)["plan"], previous.decision_id


def cycle(spark, settings, *, simulate=False):
    from kafka import KafkaProducer

    plant = Plant.load(settings.plant_config)
    plan, decision_id = feedback(spark, settings, plant) if simulate else (None, None)
    event = build_snapshot(
        plant, datetime.now(timezone.utc), applied_plan=plan, applied_decision_id=decision_id
    )
    producer = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap, acks="all", retries=10)
    try:
        producer.send(
            settings.topic,
            key=plant.scope.key.encode(),
            value=encode(event),
            headers=[("event_type", b"plant_snapshot"), ("schema_version", b"1")],
        ).get(timeout=60)
    finally:
        producer.close()
    refresh(settings)
    return predict_and_control(spark, settings, simulate=simulate)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["bootstrap", "cycle"])
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--simulate", action="store_true")
    args = parser.parse_args(argv)
    settings = load_settings()
    spark = create_spark("hvacpulse-plant-pipeline", delta=True)
    if args.action == "bootstrap":
        produce(
            [
                "--source",
                "plant",
                "--replay",
                "--count",
                str(args.count),
                "--rate",
                "0",
                "--anomaly-rate",
                "0.05",
                "--plant-config",
                str(settings.plant_config),
                "--topic",
                settings.topic,
                "--bootstrap-servers",
                settings.kafka_bootstrap,
            ]
        )
        refresh(settings)
        registry = train_model(spark, settings)
        print(f"Model {registry['model_id']}: approved={registry['approved']}")
    result = cycle(spark, settings, simulate=args.simulate)
    print(json.dumps({key: result[key] for key in ("decision_id", "state", "reason")}))


if __name__ == "__main__":
    main()
