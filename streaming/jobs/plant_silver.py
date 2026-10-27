"""Bronze → canonical plant snapshots / quarantine, with durable scoped deduplication."""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from plant.contracts import Plant
from streaming.config import load_settings
from streaming.ops import StageRun
from streaming.plant_store import merge_rows
from streaming.plant_transforms import normalized_frame
from streaming.spark_session import create_spark


def process_batch(spark, settings, batch):
    # Snapshot identity is scope + capture time, not a transport offset. v1 is
    # immutable: conflicting payloads are quarantined, not silently corrected.
    rows = batch.withColumn("row_id", F.sha2(F.concat_ws("|", "scope", "captured_at"), 256)).persist()
    try:
        valid = rows.filter("eligible")
        candidates = valid
        conflict_ids = (
            valid.groupBy("row_id")
            .agg(F.countDistinct("payload_hash").alias("n"))
            .filter("n > 1")
            .select("row_id")
        )
        if settings.delta_exists(spark, settings.plant_silver_path):
            history = settings.read_delta(spark, settings.plant_silver_path).select(
                "row_id", F.col("payload_hash").alias("existing_hash")
            )
            conflicts = valid.join(history, "row_id").filter("payload_hash <> existing_hash").select("row_id")
            conflict_ids = conflict_ids.union(conflicts).distinct()
        rejected = rows.filter("NOT eligible").unionByName(
            valid.join(conflict_ids, "row_id")
            .withColumn("eligible", F.lit(False))
            .withColumn("issues", F.lit('["IDEMPOTENCY_CONFLICT"]'))
        )
        merge_rows(
            spark,
            settings,
            rejected,
            settings.plant_quarantine_path,
            ["kafka_topic", "kafka_partition", "kafka_offset", "issues"],
        )
        # Previously conflicted snapshots remain unsafe even on a later retry.
        if settings.delta_exists(spark, settings.plant_quarantine_path):
            bad = settings.read_delta(spark, settings.plant_quarantine_path).select("row_id")
            candidates = candidates.join(bad, "row_id", "left_anti")
        merge_rows(
            spark,
            settings,
            candidates.join(conflict_ids, "row_id", "left_anti"),
            settings.plant_silver_path,
            ["row_id"],
        )
    finally:
        rows.unpersist()


def run(settings, *, once: bool = True):
    spark = create_spark("hvacpulse-plant-silver", delta=True)
    plant = Plant.load(settings.plant_config)
    source = normalized_frame(settings.read_delta_stream(spark, settings.bronze_path), plant)
    writer = source.writeStream.foreachBatch(lambda df, _: process_batch(spark, settings, df)).option(
        "checkpointLocation", settings.checkpoint("plant-silver")
    )
    if once:
        writer = writer.trigger(availableNow=True)
    with StageRun(
        spark, settings, "plant_silver", outputs=["silver_plant_snapshots", "quarantine_plant_snapshots"]
    ) as stage:
        query = writer.start()
        query.awaitTermination()
        stage.record_stream(query)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    run(load_settings(), once=args.once)


if __name__ == "__main__":
    main()
