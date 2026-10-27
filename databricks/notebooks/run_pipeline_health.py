# Databricks notebook source
# ruff: noqa: F821, E402
# COMMAND ----------
# Thin wrapper: fail this task when any required stage is unhealthy, so the job's
# failure notification fires even when hvacpulse_batch has stopped running.
import sys

for parameter in ("catalog", "schema", "source_path"):
    dbutils.widgets.text(parameter, "")

source_path = dbutils.widgets.get("source_path")
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from streaming.config import Settings
from streaming.jobs.pipeline_health import run
from streaming.ops import CRITICAL

results = run(Settings(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema")))
critical = [f"{r.stage_name}={r.status}" for r in results if r.status in CRITICAL]
if critical:
    raise RuntimeError("HVACPulse pipeline unhealthy: " + ", ".join(critical))
