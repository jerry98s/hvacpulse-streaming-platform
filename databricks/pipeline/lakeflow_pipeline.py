"""Lakeflow pipeline for the governed, immutable Bronze layer.

Bronze is the only dataset Lakeflow owns. Silver, quarantine and Gold run as
scheduled job tasks through the same shared ``foreachBatch`` MERGE code as the
local stack, with checkpoints on a Unity Catalog volume, so each run only reads
new Bronze rows and keeps global event-ID conflict detection.

``pipelines.reset.allowed = false`` keeps a full pipeline refresh from
truncating Bronze: Event Hubs only retains seven days, so a reset could not
rebuild older history.
"""

# Lakeflow injects ``spark`` and ``dbutils`` while evaluating pipeline sources.
# ruff: noqa: F821

from __future__ import annotations

import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = Path(spark.conf.get("bundle.sourcePath"))
if str(SOURCE_PATH) not in sys.path:
    sys.path.insert(0, str(SOURCE_PATH))

from streaming.transforms import with_schema_version_header  # noqa: E402

TOPIC = spark.conf.get("hvacpulse.topic", "hvac.telemetry")
BOOTSTRAP_SERVERS = spark.conf.get("hvacpulse.eventhubs_bootstrap")
SECRET_REF = spark.conf.get("hvacpulse.eventhubs_secret")
SCOPE, KEY = SECRET_REF.split(":", 1)

CONNECTION_STRING = dbutils.secrets.get(scope=SCOPE, key=KEY)
EH_SASL = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{CONNECTION_STRING}";'
)


@dp.table(
    name="bronze_telemetry",
    comment="Raw Kafka or Event Hubs records for both telemetry profiles, append-only and immutable.",
    table_properties={"pipelines.reset.allowed": "false"},
    cluster_by=["ingest_ts"],
)
def bronze_telemetry():
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", EH_SASL)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("includeHeaders", "true")
        .load()
    )
    return (
        kafka.select(
            F.col("key").alias("kafka_key"),
            "value",
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            "headers",
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("payload_hash", F.sha2(F.hex(F.col("value")), 256))
        .transform(with_schema_version_header)
    )
