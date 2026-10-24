"""SparkSession factory shared by all jobs.

Delta Lake auto-merge stays disabled (README, "Schema-Evolution Strategy"):
approved additive changes land through explicit DDL, not automatic evolution.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

# Keep the three packages in lock-step with pyspark==3.5.5 / delta-spark==3.2.1.
SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.12:3.2.1",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        "org.apache.spark:spark-avro_2.12:3.5.5",
    ]
)


def create_spark(app_name: str, *, delta: bool = False) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        # Databricks owns the active session and supplies Delta/Kafka/Avro.
        # Reconfiguring its master or package set would create an invalid local
        # session inside the cluster process.
        return active

    # SPARK_LOCAL_IP=127.0.0.1 triggers a BlockManager NPE on Windows
    # (idWithoutTopologyInfo is null). Prefer hostname resolution.
    os.environ.pop("SPARK_LOCAL_IP", None)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        # AQE: runtime skew handling, join-strategy switching, partition
        # coalescing. Defaults on Databricks; explicit here for local parity.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    packages = SPARK_PACKAGES if delta else "org.apache.spark:spark-avro_2.12:3.5.5"
    builder = builder.config("spark.jars.packages", packages)
    if delta:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    return builder.getOrCreate()
