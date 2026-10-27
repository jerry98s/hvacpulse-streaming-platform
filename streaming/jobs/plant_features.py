"""Append streaming features and water-balance labels from immutable Silver snapshots."""

from __future__ import annotations

import argparse

from plant.contracts import Plant
from streaming.config import load_settings
from streaming.ops import StageRun
from streaming.plant_store import merge_rows
from streaming.plant_transforms import feature_frame
from streaming.spark_session import create_spark


def process_batch(spark, settings, frame, plant):
    if settings.delta_exists(spark, settings.plant_quarantine_path):
        unsafe = settings.read_delta(spark, settings.plant_quarantine_path).select("row_id")
        frame = frame.join(unsafe, "row_id", "left_anti")
    merge_rows(
        spark,
        settings,
        feature_frame(frame, plant),
        settings.plant_features_path,
        ["row_id", "feature_version"],
    )


def run(settings, *, once=True):
    spark = create_spark("hvacpulse-plant-features", delta=True)
    plant = Plant.load(settings.plant_config)
    source = settings.read_delta_stream(spark, settings.plant_silver_path)
    writer = source.writeStream.foreachBatch(lambda df, _: process_batch(spark, settings, df, plant)).option(
        "checkpointLocation", settings.checkpoint("plant-features")
    )
    if once:
        writer = writer.trigger(availableNow=True)
    with StageRun(spark, settings, "plant_features", outputs=["gold_plant_features"]) as stage:
        query = writer.start()
        query.awaitTermination()
        stage.record_stream(query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    run(load_settings(), once=parser.parse_args().once)
