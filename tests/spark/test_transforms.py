"""Spark-level tests for parse + validate transforms (no Delta, no Kafka).

Builds a synthetic Bronze DataFrame containing the same wire bytes the
simulator produces, then asserts the exact quarantine error codes.
"""

import io
import random
from datetime import datetime
from datetime import timezone

import pytest

pytest.importorskip("pyspark")

from fastavro import schemaless_writer  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from ingestion.events import DEFAULT_DEVICES  # noqa: E402
from ingestion.events import build_event  # noqa: E402
from ingestion.simulator import load_avro_schema  # noqa: E402
from quality import rules  # noqa: E402
from streaming.jobs.gold_metrics import compute_window_metrics  # noqa: E402
from streaming.spark_session import create_spark  # noqa: E402
from streaming.transforms import flatten_parsed  # noqa: E402
from streaming.transforms import parse_bronze  # noqa: E402
from streaming.transforms import with_schema_version_header  # noqa: E402
from streaming.transforms import with_validation  # noqa: E402

pytestmark = pytest.mark.spark

SCHEMAS = {1: load_avro_schema(1), 2: load_avro_schema(2)}

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("kafka_timestamp", T.TimestampType()),
        T.StructField(
            "headers",
            T.ArrayType(
                T.StructType([T.StructField("key", T.StringType()), T.StructField("value", T.BinaryType())])
            ),
        ),
        T.StructField("ingest_ts", T.TimestampType()),
        T.StructField("ingest_date", T.DateType()),
        T.StructField("payload_hash", T.StringType()),
    ]
)


def _wire(event: dict, version: int) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, SCHEMAS[version], event)
    return buf.getvalue()


def test_duplicate_headers_do_not_break_bronze_retention(spark):
    frame = spark.createDataFrame(
        [
            ([("schema_version", b"1")],),
            ([("schema_version", b"1"), ("schema_version", b"2")],),
            ([("trace", b"a"), ("trace", b"b"), ("schema_version", b"2")],),
            ([],),
        ],
        "headers array<struct<key:string,value:binary>>",
    )
    assert [r.wire_schema_version for r in with_schema_version_header(frame).collect()] == [
        "1",
        None,
        "2",
        None,
    ]


def _row(value: bytes, version: str, offset: int, device_id: str = "ahu-001"):
    now = datetime.now(timezone.utc)
    return (
        device_id.encode(),
        value,
        "hvac.telemetry",
        0,
        offset,
        now,
        [("schema_version", version.encode())],
        now,
        now.date(),
        f"hash-{offset}",
    )


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-transforms")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def bronze_rows(spark):
    rng = random.Random(21)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    v1_device, v2_device = DEFAULT_DEVICES[0], DEFAULT_DEVICES[3]
    valid_v1 = build_event(v1_device, rng=rng, sequence_number=1, event_time=now_iso)
    valid_v2 = build_event(v2_device, rng=rng, sequence_number=1, event_time=now_iso)
    negative_power = build_event(v1_device, rng=rng, sequence_number=2, event_time=now_iso)
    negative_power["measurements"]["power_kw"] = -3.0
    missing_humidity = build_event(v1_device, rng=rng, sequence_number=3, event_time=now_iso)
    missing_humidity["measurements"]["relative_humidity_pct"] = None
    unknown_device = build_event(v1_device, rng=rng, sequence_number=4, event_time=now_iso)
    unknown_device["device_id"] = "ahu-999-unknown"

    rows = [
        _row(_wire(valid_v1, 1), "1", 1),
        _row(_wire(valid_v2, 2), "2", 2, device_id="fcu-201"),
        _row(_wire(negative_power, 1), "1", 3),
        _row(_wire(missing_humidity, 1), "1", 4),
        _row(b"\x00\x11\x22not-avro", "1", 5),
        _row(_wire(valid_v1, 1), "99", 6),
        _row(_wire(unknown_device, 1), "1", 7, device_id="ahu-999-unknown"),
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def test_parse_and_validate_assigns_expected_error_codes(spark, bronze_rows):
    known = {d.device_id for d in DEFAULT_DEVICES}
    result = with_validation(
        flatten_parsed(parse_bronze(with_schema_version_header(bronze_rows))),
        known_devices=known,
    )
    by_offset = {r.kafka_offset: r for r in result.collect()}

    assert by_offset[1].error_code is None
    assert by_offset[1].device_id == "ahu-001"
    assert by_offset[1].co2_ppm is None  # v1 payload, canonical v2 column stays NULL

    assert by_offset[2].error_code is None
    assert by_offset[2].co2_ppm is not None  # additive v2 field parsed

    assert by_offset[3].error_code == rules.NEGATIVE_POWER
    assert by_offset[4].error_code == rules.HUMIDITY_MISSING
    assert by_offset[5].error_code == rules.SCHEMA_INVALID
    assert by_offset[6].error_code == rules.UNSUPPORTED_SCHEMA_VERSION
    assert by_offset[7].error_code == rules.EQUIPMENT_UNKNOWN


def test_late_event_is_quarantined_with_event_too_late(spark):
    rng = random.Random(22)
    old = build_event(DEFAULT_DEVICES[0], rng=rng, sequence_number=1, event_time="2020-01-01T00:00:00.000Z")
    df = spark.createDataFrame([_row(_wire(old, 1), "1", 10)], BRONZE_SCHEMA)
    result = with_validation(flatten_parsed(parse_bronze(with_schema_version_header(df))))
    assert result.first().error_code == rules.EVENT_TOO_LATE


def test_energy_uses_each_devices_report_interval(spark):
    event_time = datetime(2026, 8, 26, 10, 0, 10)
    silver = spark.createDataFrame(
        [
            ("site-1", "fast", event_time, 36.0, 22.0, 50.0),
            ("site-1", "slow", event_time, 36.0, 22.0, 50.0),
        ],
        "site_id string, device_id string, event_time timestamp, power_kw double, "
        "zone_temp_c double, relative_humidity_pct double",
    )
    metrics = compute_window_metrics(silver, {"fast": 5.0, "slow": 10.0})
    energy = {r.device_id: r.energy_kwh for r in metrics.collect()}

    assert energy["fast"] == pytest.approx(0.05)
    assert energy["slow"] == pytest.approx(0.10)


def test_float_noise_does_not_change_the_content_hash(spark):
    event_time = datetime(2026, 8, 26, 10, 0, 10)
    schema = (
        "site_id string, device_id string, event_time timestamp, power_kw double, "
        "zone_temp_c double, relative_humidity_pct double"
    )

    def content_hash(power_kw):
        silver = spark.createDataFrame([("site-1", "ahu", event_time, power_kw, 22.0, 50.0)], schema)
        return compute_window_metrics(silver, {"ahu": 5.0}).first()._content_hash

    assert content_hash(10.1) == content_hash(10.1 + 1e-12)
    assert content_hash(10.1) != content_hash(10.2)


def test_energy_rejects_missing_report_intervals(spark):
    silver = spark.createDataFrame([], "device_id string")
    with pytest.raises(ValueError, match="report interval"):
        compute_window_metrics(silver, {})
