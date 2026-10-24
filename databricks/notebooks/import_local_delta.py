# Databricks notebook source
# ruff: noqa: F821, E402
# COMMAND ----------
# Import locally generated Delta folders (uploaded to a Volume) into Unity Catalog.
#
# Prerequisite: upload .local/delta/* to a Volume, e.g.
#   /Volumes/main/hvacpulse/local_delta/delta/silver_telemetry_current
#
# See docs/runbooks/free-edition.md
import sys

for parameter in ("catalog", "schema", "volume_delta_path", "source_path"):
    dbutils.widgets.text(parameter, "")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
volume_delta_path = dbutils.widgets.get("volume_delta_path").strip().rstrip("/")
source_path = dbutils.widgets.get("source_path").strip()

if source_path and source_path not in sys.path:
    sys.path.insert(0, source_path)

if not catalog or not schema:
    raise ValueError("Set catalog and schema widgets (e.g. main / hvacpulse)")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

TABLES = (
    "bronze_telemetry",
    "silver_telemetry_history",
    "silver_telemetry_current",
    "quarantine_events",
)

# COMMAND ----------
imported = []
missing = []
for name in TABLES:
    src = f"{volume_delta_path}/{name}"
    target = f"{catalog}.{schema}.{name}"
    try:
        entries = dbutils.fs.ls(src)
    except Exception as exc:  # noqa: BLE001 — surface path to operator
        missing.append((name, src, str(exc)))
        continue
    if not any("_delta_log" in e.name for e in entries):
        missing.append((name, src, "no _delta_log directory"))
        continue
    df = spark.read.format("delta").load(src)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    imported.append((target, df.count()))

# COMMAND ----------
print("Imported tables:")
for table, rows in imported:
    print(f"  {table}: {rows} rows")

if missing:
    print("\nSkipped (upload these folders and re-run):")
    for name, path, reason in missing:
        print(f"  {name} @ {path} — {reason}")
