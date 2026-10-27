# Databricks notebook source
# ruff: noqa: F821, E402
# COMMAND ----------
# Thin wrapper for the shared late-correction reconciliation implementation.
import sys
from pathlib import Path

for parameter in ("catalog", "schema", "source_path", "run_id"):
    dbutils.widgets.text(parameter, "")

source_path = dbutils.widgets.get("source_path")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from streaming.config import Settings
from streaming.jobs.reconciliation import run

settings = Settings(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    known_devices_csv=Path(source_path) / "reference" / "equipment_master.csv",
    run_id=dbutils.widgets.get("run_id") or None,
)
run(settings)
