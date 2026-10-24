# Databricks notebook source
# ruff: noqa: F821, E402
# COMMAND ----------
# Thin wrapper: apply reference/semantic_model.yml to Unity Catalog.
import sys

for parameter in ("catalog", "schema", "source_path"):
    dbutils.widgets.text(parameter, "")

source_path = dbutils.widgets.get("source_path")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from streaming.config import Settings
from streaming.jobs.semantic_layer import run

run(Settings(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema")))
