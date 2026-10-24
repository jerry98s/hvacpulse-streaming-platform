# Databricks notebook source
# ruff: noqa: F821, E402
# COMMAND ----------
# Thin wrapper: incremental Bronze → Silver / quarantine with a checkpoint on a UC volume.
import sys
from pathlib import Path

for parameter in ("catalog", "schema", "source_path"):
    dbutils.widgets.text(parameter, "")

source_path = dbutils.widgets.get("source_path")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from streaming.config import Settings
from streaming.config import ensure_uc_checkpoint_root
from streaming.jobs.silver_transform import run

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
settings = Settings(
    catalog=catalog,
    schema=schema,
    known_devices_csv=Path(source_path) / "reference" / "equipment_master.csv",
    checkpoint_root=ensure_uc_checkpoint_root(spark, catalog, schema),
)
run(settings, once=True)
