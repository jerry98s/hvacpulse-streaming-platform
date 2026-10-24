# Databricks notebook source
# ruff: noqa: F821, E402
# COMMAND ----------
# Run Gold → quality → recommendations → reconciliation in one serverless session.
# Requires Silver (+ optional quarantine) tables already in Unity Catalog.
#
# See docs/runbooks/free-edition.md
import sys
from pathlib import Path

for parameter in ("catalog", "schema", "source_path"):
    dbutils.widgets.text(parameter, "")

source_path = dbutils.widgets.get("source_path")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from streaming.config import Settings
from streaming.jobs.gold_metrics import run as run_gold
from streaming.jobs.quality_metrics import run as run_quality
from streaming.jobs.recommendations_job import run as run_recommendations
from streaming.jobs.reconciliation import run as run_reconciliation

settings = Settings(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    known_devices_csv=Path(source_path) / "reference" / "equipment_master.csv",
)

# COMMAND ----------
print("== gold_metrics ==")
run_gold(settings)

# COMMAND ----------
print("== quality_metrics ==")
run_quality(settings)

# COMMAND ----------
print("== recommendations ==")
run_recommendations(settings)

# COMMAND ----------
print("== reconciliation ==")
run_reconciliation(settings)

# COMMAND ----------
print("Done. Spot-check:")
display(spark.sql(f"SELECT COUNT(*) AS gold_rows FROM {settings.gold_current_path}"))
display(spark.sql(f"SELECT COUNT(*) AS rec_rows FROM {settings.gold_recommendations_path}"))
