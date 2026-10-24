# Runbook: Databricks Free Edition (no Azure Event Hubs)

Use this path when you have **Databricks Free Edition only** — no Azure Event
Hubs, no Terraform, and no paid workspace. You still get Unity Catalog tables,
serverless notebooks/jobs, and the shared Gold / quality / recommendation /
reconciliation jobs.

> If you have an **Azure subscription**, use the primary cloud path instead:
> [azure-databricks.md](./azure-databricks.md) (Event Hubs + ADLS + Azure Databricks).

**What you skip:** the Lakeflow Kafka pipeline (`databricks/pipeline/…`) and
`databricks bundle deploy` pipeline resources. Those need Event Hubs and
outbound Kafka access, which Free Edition does not support cleanly.

**What you keep:** the full medallion logic, exercised by generating data
locally once, uploading Delta folders, registering UC tables, then running the
batch notebooks.

---

## Overview

```text
[Your PC]  Docker Kafka + local Spark jobs  →  .local/delta/*
      │
      │  upload (CLI or UI)
      ▼
[Databricks Free Edition]  Volume  →  import notebook  →  catalog.schema.*
      │
      │  batch notebooks (gold → quality → recommendations → reconcile)
      ▼
[Databricks Free Edition]  query in SQL / notebook
```

---

## Prerequisites

| Where | What |
|---|---|
| Your PC | Docker Desktop, Python 3.10+, Java 11/17, this repo cloned |
| Databricks | [Free Edition](https://www.databricks.com/try-databricks) workspace |
| Optional | [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) (`databricks auth login`) |

Free Edition limits that matter here:

- **Serverless only** — matches our notebooks (no cluster config needed).
- **Daily compute quota** — run the import + batch chain manually; avoid the
  5-task scheduled job until you know your quota headroom.
- **One active Lakeflow pipeline per type** — we do not use the Kafka pipeline
  on Free Edition, so this is a non-issue.
- **Outbound internet restricted** — another reason to skip Event Hubs/Kafka.

---

## Step 1 — Generate Delta tables locally

On your PC (once per demo dataset):

```powershell
cd <path-to>\hvacpulse-streaming-platform
uv sync --group dev

# Avoid OneDrive file locks
$env:HVACPULSE_DATA_ROOT = "C:\Temp\hvacpulse-local"

.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task bronze
.\scripts\local\run_local_stack.ps1 -Task silver
# Gold is optional before upload — the Databricks gold notebook can rebuild it
```

Confirm output exists:

```powershell
Get-ChildItem C:\Temp\hvacpulse-local\delta
```

You should see at least `bronze_telemetry`, `silver_telemetry_history`,
`silver_telemetry_current`, and `quarantine_events`.

---

## Step 2 — Prepare Unity Catalog in Free Edition

In a Databricks SQL editor or notebook:

```sql
SHOW CATALOGS;
-- Pick the catalog you own (often `main` on Free Edition)

CREATE SCHEMA IF NOT EXISTS main.hvacpulse;
USE SCHEMA main.hvacpulse;
```

Note your **catalog** and **schema** — the notebooks expect widgets
`catalog=main` and `schema=hvacpulse` (adjust to match your workspace).

Create a Volume for uploads (if you do not already have one):

```sql
CREATE VOLUME IF NOT EXISTS main.hvacpulse.local_delta;
```

Volume path will look like:

```text
/Volumes/main/hvacpulse/local_delta
```

---

## Step 3 — Upload local Delta folders to the Volume

### Option A — Databricks CLI (recommended)

After `databricks auth login`, upload each table folder:

```powershell
$LocalRoot = "C:\Temp\hvacpulse-local\delta"
$RemoteRoot = "/Volumes/main/hvacpulse/local_delta/delta"

foreach ($table in @(
    "bronze_telemetry",
    "silver_telemetry_history",
    "silver_telemetry_current",
    "quarantine_events"
)) {
    databricks fs cp -r --overwrite `
        "$LocalRoot\$table" `
        "dbfs:$RemoteRoot/$table"
}
```

Replace `main/hvacpulse` if you used different catalog/schema names.

### Option B — Workspace UI

1. Open **Catalog** → your schema → **Volumes** → `local_delta`.
2. Upload each folder under `delta/` (must keep `_delta_log` inside each folder).
3. Final layout: `/Volumes/main/hvacpulse/local_delta/delta/silver_telemetry_current/…`

---

## Step 4 — Sync repo code to the workspace

The batch notebooks import `streaming.*` from your repo. Sync the project:

```powershell
databricks sync . /Workspace/Users/<your-email>/hvacpulse `
  --exclude ".local" --exclude ".venv" --exclude ".git"
```

Or clone/upload these paths manually:

- `streaming/`
- `quality/`
- `reference/equipment_master.csv`
- `contracts/avro/`
- `databricks/notebooks/`

Set `source_path` in notebooks to that workspace folder, e.g.
`/Workspace/Users/you@example.com/hvacpulse`.

---

## Step 5 — Import Volume Delta → Unity Catalog tables

Open **`databricks/notebooks/import_local_delta`** in the workspace (import the
`.py` file as a notebook if needed).

Set widgets:

| Widget | Example |
|---|---|
| `catalog` | `main` |
| `schema` | `hvacpulse` |
| `volume_delta_path` | `/Volumes/main/hvacpulse/local_delta/delta` |
| `source_path` | `/Workspace/Users/you@example.com/hvacpulse` |

Run all cells. This registers managed Delta tables:

- `main.hvacpulse.bronze_telemetry`
- `main.hvacpulse.silver_telemetry_history`
- `main.hvacpulse.silver_telemetry_current`
- `main.hvacpulse.quarantine_events`

Verify:

```sql
SELECT 'silver_current' AS layer, COUNT(*) AS rows FROM main.hvacpulse.silver_telemetry_current
UNION ALL
SELECT 'quarantine', COUNT(*) FROM main.hvacpulse.quarantine_events;
```

---

## Step 6 — Run batch jobs (in order)

Run these notebooks **one at a time** on serverless compute. Use the same
`catalog`, `schema`, and `source_path` widgets on each.

| Order | Notebook | Creates / updates |
|---|---|---|
| 1 | `run_gold_metrics` | `gold_hvac_metrics_current`, `gold_hvac_metrics_history` |
| 2 | `run_quality_metrics` | DQ scorecard, freshness, compliance, equipment health |
| 3 | `run_recommendations` | `gold_recommendations`, `recommendation_events` |
| 4 | `run_reconciliation` | `late_corrections`, `metric_restatement_events` |

Or run the combined wrapper **`run_all_batch`** (same widgets) — it executes
the four jobs sequentially in one notebook session (uses more quota in one go).

Example sanity query after Gold:

```sql
SELECT device_id, window_start, energy_kwh, window_status
FROM main.hvacpulse.gold_hvac_metrics_current
ORDER BY window_start DESC
LIMIT 20;
```

---

## Step 7 — Explore results

**SQL warehouse** (Free Edition includes one 2X-Small warehouse):

```sql
-- Quarantine breakdown
SELECT error_code, COUNT(*) FROM main.hvacpulse.quarantine_events GROUP BY 1;

-- Active advisory recommendations
SELECT * FROM main.hvacpulse.gold_recommendations WHERE status = 'ACTIVE';
```

**Notebook charts:** pivot `gold_hvac_metrics_current` by `device_id` and
`window_start` for energy trends.

The local Streamlit dashboard (`dashboard/app.py`) reads files on disk only —
it does **not** connect to Databricks. Use SQL/notebooks in the workspace
instead.

---

## Refreshing the demo

1. Re-run local simulate → bronze → silver (Step 1).
2. Re-upload Delta folders to the Volume (Step 3).
3. Re-run `import_local_delta` (overwrite tables).
4. Re-run batch notebooks (Step 6).

Idempotency still applies: re-importing the same Silver data and re-running Gold
should not double-count energy if payload hashes are unchanged.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: streaming` | `source_path` widget wrong | Point to synced repo root containing `streaming/` |
| `TABLE_OR_VIEW_NOT_FOUND` | Import not run or wrong catalog/schema | Re-run import; check `SHOW TABLES IN main.hvacpulse` |
| `Path does not exist` on Volume | Upload path mismatch | List Volume: `dbutils.fs.ls("/Volumes/main/hvacpulse/local_delta/delta")` |
| `PERMISSION_DENIED` / arclight catalog | Free Edition UC restriction on pipelines | **Do not** use the Lakeflow Kafka pipeline; use this import path |
| Compute shut down for the day | Free Edition daily quota | Wait for reset; run fewer/lighter notebooks |
| `equipment_master.csv` not found | Reference file not synced | Include `reference/` in `databricks sync` |
| Recommendations empty | Stale `max_event_time` vs wall clock | Re-run local simulate with recent timestamps, re-upload Silver |

---

## What not to deploy on Free Edition

Do **not** run:

```powershell
databricks bundle deploy -t dev `
  --var "eventhubs_bootstrap=..." `
  --var "eventhubs_connection_secret=..."
```

That deploys the Event Hubs Lakeflow pipeline and a 5-task scheduled job. It
requires Azure Event Hubs, secrets, and outbound Kafka — outside the Free
Edition sweet spot.

When you later get Azure + a trial workspace, use [Cloud Deploy (Azure)](../../README.md#cloud-deploy-azure) instead.

---

## Quick checklist

- [ ] Local pipeline produced `C:\Temp\hvacpulse-local\delta\*`
- [ ] `CREATE SCHEMA main.hvacpulse` (or your names)
- [ ] Delta folders uploaded to Volume
- [ ] Repo synced; `source_path` widget set
- [ ] `import_local_delta` completed
- [ ] `run_gold_metrics` → `run_quality_metrics` → `run_recommendations` → `run_reconciliation`
- [ ] Spot-check SQL on Gold + quarantine + recommendations
