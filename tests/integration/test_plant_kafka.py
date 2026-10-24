"""Opt-in broker integration using a unique topic; never deletes existing topics."""

import os
import uuid
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from ingestion.plant_events import build_snapshot
from ingestion.plant_events import encode
from ingestion.producer import main as produce
from plant.contracts import Plant
from streaming.config import Settings
from streaming.jobs.plant_learning import predict_and_control
from streaming.jobs.plant_learning import train_model
from streaming.jobs.plant_pipeline import feedback
from streaming.jobs.plant_pipeline import refresh
from streaming.spark_session import create_spark

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HVACPULSE_TEST_KAFKA") != "1",
        reason="Set HVACPULSE_TEST_KAFKA=1 with the local broker running",
    ),
]


def test_kafka_to_model_to_mock_feedback(tmp_path):
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient
    from kafka.admin import NewTopic

    settings = replace(Settings(root=tmp_path), topic=f"hvac.plant-test-{uuid.uuid4().hex}")
    admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    admin.create_topics([NewTopic(settings.topic, num_partitions=1, replication_factor=1)])
    spark = create_spark("hvacpulse-test-kafka-plant", delta=True)
    try:
        produce(
            [
                "--source",
                "plant",
                "--replay",
                "--count",
                "384",
                "--rate",
                "0",
                "--anomaly-rate",
                "0",
                "--topic",
                settings.topic,
                "--bootstrap-servers",
                settings.kafka_bootstrap,
            ]
        )
        refresh(settings)
        assert settings.read_delta(spark, settings.plant_features_path).count() == 384
        registry = train_model(spark, settings, epochs=3)
        assert registry["approved"]
        plant = Plant.load(settings.plant_config)
        captured = datetime.now(timezone.utc)
        producer = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap, acks="all")
        try:
            producer.send(
                settings.topic,
                key=plant.scope.key.encode(),
                value=encode(build_snapshot(plant, captured)),
                headers=[("event_type", b"plant_snapshot"), ("schema_version", b"1")],
            ).get(timeout=60)
        finally:
            producer.close()
        refresh(settings)
        # Simulated clock isolates transport correctness from slow CI hardware.
        # Wall-clock freshness is separately tested by the control unit tests.
        availability = (
            settings.read_delta(spark, settings.plant_features_path)
            .orderBy("captured_at", ascending=False)
            .first()
            .available_at
        )
        decision_time = max(captured, datetime.fromisoformat(availability)) + timedelta(seconds=1)
        decision = predict_and_control(spark, settings, now=decision_time, simulate=True)
        assert decision["state"] == "ACKNOWLEDGED"
        plan, command_id = feedback(spark, settings, plant)
        assert plan and command_id == decision["decision_id"]
        event = build_snapshot(plant, decision_time, applied_plan=plan, applied_decision_id=command_id)
        assert event["applied_decision_id"] == decision["decision_id"]
    finally:
        spark.stop()
        admin.delete_topics([settings.topic])
        admin.close()
