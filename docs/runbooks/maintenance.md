# Delta Lake maintenance runbook

Periodic maintenance keeps Delta tables healthy. Run this **daily or weekly**
in production; skip it for short demos.

## What the job does

| Operation | Purpose | Command |
|---|---|---|
| `OPTIMIZE` | Compacts many small Parquet files into fewer larger ones (~128–256 MB). Fixes the "small file problem" that slows scans. | `--optimize` |
| `VACUUM` | Deletes files no longer referenced by the Delta log, based on retention. Prevents unbounded storage growth from time travel. | `--vacuum --retention-days 7` |

## Run locally

```powershell
.\.venv\Scripts\python.exe -m streaming.jobs.maintenance --optimize --vacuum --retention-days 7
```

Or via the local runner:

```powershell
.\scripts\local\run_local_stack.ps1 -Task maintain
```

## Run on Azure Databricks

1. **Serverless SQL / Notebook** (recommended for production):

   ```sql
   OPTIMIZE hvacpulse_dev.streaming.silver_telemetry_history;
   VACUUM  hvacpulse_dev.streaming.silver_telemetry_history RETAIN 168 HOURS;
   ```

2. **Scheduled job** — add to `databricks.yml`:

   ```yaml
   resources:
     jobs:
       hvacpulse_maintenance:
         name: hvacpulse-maintenance
         schedule:
           quartz_cron_expression: "0 0 2 * * ?"  # daily 02:00
           timezone_id: UTC
         tasks:
           - task_key: optimize_vacuum
             notebook_task:
               notebook_path: ../databricks/notebooks/run_maintenance.py
   ```

3. **Tables covered** by `streaming/jobs/maintenance.py`:
   - `bronze_telemetry`
   - `silver_telemetry_history`, `silver_telemetry_current`
   - `quarantine_events`
   - `gold_hvac_metrics_current`, `gold_hvac_metrics_history`
   - `gold_data_quality_scorecard`, `gold_sensor_freshness`, `gold_compliance`, `gold_equipment_health`
   - `gold_recommendations`
   - `late_corrections`, `metric_restatement_events`

## Retention guidance

| Table type | Suggested retention |
|---|---|
| Bronze / Silver history | 30 days (audit, replay) |
| Gold / observability | 7–30 days |
| Quarantine | 30 days (debugging) |

## When to run

- **Daily** if streaming continuously and generating many small files.
- **Weekly** for batch / demo workloads.
- **Before cost reports** if storage usage is a concern.

## Safety

- `VACUUM` only removes files outside the retention window; time-travel within the window is preserved.
- `OPTIMIZE` is idempotent; running it twice is harmless.
- The job skips missing tables and prints a log line for each action.
