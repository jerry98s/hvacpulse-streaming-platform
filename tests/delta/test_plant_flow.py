"""Real Delta / Structured Streaming integration without a Kafka broker.

The same Avro bytes and Bronze metadata are used as the production Kafka source.
Kafka transport itself is covered separately by the optional integration test.
"""

import hashlib
import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from ingestion.plant_events import build_snapshot
from ingestion.plant_events import encode
from plant.contracts import Plant
from streaming.config import Settings
from streaming.jobs.plant_features import run as features_run
from streaming.jobs.plant_learning import predict_and_control
from streaming.jobs.plant_learning import train_model
from streaming.jobs.plant_silver import run as silver_run
from streaming.plant_transforms import normalized_frame
from streaming.plant_transforms import plant_records
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta

BRONZE_SCHEMA = """value binary, topic string, partition int, offset long,
 headers array<struct<key:string,value:binary>>, ingest_ts timestamp, payload_hash string"""


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-plant-flow", delta=True)
    yield session
    session.stop()


def append(spark, settings, plant, times, at, *, conflict=False):
    rows = []
    for offset, time in enumerate(times):
        event = build_snapshot(plant, time)
        if conflict:
            event["readings"][0]["value"] += 0.1
        value = encode(event)
        rows.append(
            (
                value,
                "hvac.telemetry",
                0,
                int(time.timestamp() * 1000) + offset,
                [("event_type", b"plant_snapshot"), ("schema_version", b"1")],
                at,
                hashlib.sha256(value.hex().upper().encode()).hexdigest(),
            )
        )
    settings.write_delta(spark.createDataFrame(rows, BRONZE_SCHEMA), settings.bronze_path, mode="append")


def test_schema_dispatch_and_utc_availability(spark):
    plant = Plant.load(Settings().plant_config)
    now = datetime(2026, 9, 10, tzinfo=timezone.utc)
    payload = encode(build_snapshot(plant, now))
    rows = [
        (payload, "hvac.telemetry", 0, i, headers, now, "hash")
        for i, headers in enumerate(
            [
                [("event_type", b"plant_snapshot"), ("schema_version", b"1")],
                [("event_type", b"plant_snapshot"), ("schema_version", b"2")],
                [("schema_version", b"1")],
            ]
        )
    ]
    bronze = spark.createDataFrame(rows, BRONZE_SCHEMA)
    assert bronze.filter(~plant_records(bronze)).count() == 1
    normalized = normalized_frame(bronze, plant).orderBy("kafka_offset").collect()
    assert len(normalized) == 2
    assert normalized[0].eligible
    assert datetime.fromisoformat(normalized[0].available_at) == now
    assert not normalized[1].eligible and "SCHEMA_HEADER_INVALID" in normalized[1].issues


def test_stream_train_control_restart_and_conflict(spark, tmp_path):
    settings = Settings(root=tmp_path)
    plant = Plant.load(settings.plant_config)
    now = datetime(2026, 9, 10, tzinfo=timezone.utc)
    history = [now - timedelta(minutes=15 * i) for i in range(384, 0, -1)]
    append(spark, settings, plant, history, now)
    silver_run(settings)
    features_run(settings)
    assert settings.read_delta(spark, settings.plant_features_path).count() == 384
    registry = train_model(spark, settings, now=now, epochs=3)
    assert registry["approved"]
    train_model(spark, settings, now=now, epochs=3)
    assert settings.read_delta(spark, settings.plant_models_path).count() == 1

    fresh = now + timedelta(seconds=1)
    append(spark, settings, plant, [fresh], fresh)
    silver_run(settings)
    features_run(settings)
    first = predict_and_control(spark, settings, now=fresh, simulate=True)
    assert first["state"] == "ACKNOWLEDGED"
    retried = predict_and_control(spark, settings, now=fresh + timedelta(seconds=1), simulate=True)
    assert retried == first
    assert settings.read_delta(spark, settings.plant_decisions_path).count() == 1
    assert settings.read_delta(spark, settings.plant_predictions_path).count() == 1

    next_time = fresh + timedelta(seconds=10)
    append(spark, settings, plant, [next_time], next_time)
    silver_run(settings)
    features_run(settings)
    second = predict_and_control(spark, settings, now=next_time, simulate=True)
    assert second["reason"] == "MINIMUM_COMMAND_INTERVAL"
    assert json.loads(second["result_json"])["supply_c"] == json.loads(first["result_json"])["supply_c"]

    # Replaying the same bytes does not double training rows after a checkpoint restart.
    append(spark, settings, plant, [fresh], next_time)
    silver_run(settings)
    features_run(settings)
    assert settings.read_delta(spark, settings.plant_features_path).count() == 386
    # A late conflict in the model's training data must revoke its use.
    append(spark, settings, plant, [history[0]], next_time, conflict=True)
    silver_run(settings)
    assert settings.read_delta(spark, settings.plant_quarantine_path).count() == 1
    newer = next_time + timedelta(minutes=16)
    append(spark, settings, plant, [newer], newer)
    silver_run(settings)
    features_run(settings)
    blocked = predict_and_control(spark, settings, now=newer, simulate=True)
    assert blocked["reason"] == "MODEL_NOT_APPROVED"
