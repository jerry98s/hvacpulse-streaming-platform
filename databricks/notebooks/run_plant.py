# Databricks notebook source
# ruff: noqa: F821, E402
"""Shared plant stages over Lakeflow Bronze; scheduled control is always shadow."""

import sys
from pathlib import Path

for parameter in ("catalog", "schema", "source_path"):
    dbutils.widgets.text(parameter, "")
dbutils.widgets.dropdown("action", "refresh", ["refresh", "train"])
source_path = dbutils.widgets.get("source_path")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from streaming.config import Settings
from streaming.config import ensure_uc_checkpoint_root
from streaming.jobs.plant_features import run as features_run
from streaming.jobs.plant_learning import predict_and_control
from streaming.jobs.plant_learning import train_model
from streaming.jobs.plant_silver import run as silver_run

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
settings = Settings(
    catalog=catalog,
    schema=schema,
    plant_config=Path(source_path) / "reference/mock_plant.json",
    checkpoint_root=ensure_uc_checkpoint_root(spark, catalog, schema),
)
if dbutils.widgets.get("action") == "train":
    train_model(spark, settings)
else:
    silver_run(settings, once=True)
    if settings.delta_exists(spark, settings.plant_silver_path):
        features_run(settings, once=True)
    if settings.delta_exists(spark, settings.plant_models_path):
        predict_and_control(spark, settings)  # Never simulate or write equipment in cloud jobs.
