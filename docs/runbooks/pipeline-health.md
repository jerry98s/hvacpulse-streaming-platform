# Pipeline Health Check Runbook

The pipeline health check monitors all stages defined in `reference/monitoring.yml` by reading the operational tracking tables (`ops_pipeline_stage_runs` and `ops_dataset_publications`) and reporting the health status of each stage.

## Overview

Every pipeline stage automatically records its execution:
- **Stage runs** (`ops_pipeline_stage_runs`): Each attempt records STARTED, then SUCCEEDED or FAILED with timing, input rows, and errors.
- **Dataset publications** (`ops_dataset_publications`): Each Delta table version committed during a stage is recorded with row counts and schema validation status.

The health check job (`streaming.jobs.pipeline_health`) evaluates these tracking records against policy thresholds and reports each stage's status.

## Running the Check

### Local

```powershell
# Run pipeline health check
uv run python -m streaming.jobs.pipeline_health
```

### Cloud (Databricks)

```python
# Via notebook or scheduled job
%run ./databricks/notebooks/run_pipeline_health
```

## Health Statuses

### HEALTHY (normal)
**Meaning**: Stage succeeded recently and is within all thresholds.

**Triggers**: Last successful run finished within `max_success_age_minutes` (default: 15 minutes).

**Example output**:
```
gold_metrics    HEALTHY    last success 3.0 minutes ago
```

**Action**: None required.

---

### RUNNING (normal)
**Meaning**: Stage is currently executing.

**Triggers**: A STARTED event exists with no outcome yet, and runtime is under `max_stage_minutes` (default: 30 minutes).

**Example output**:
```
silver_transform RUNNING    running; last success 8.0 minutes ago
```

**Action**: Monitor; if it doesn't complete within `max_stage_minutes`, it will become STUCK.

---

### IDLE (informational)
**Meaning**: The idle stage (default: `silver_transform`) has processed no new input rows for the last N successful runs.

**Triggers**: The last `idle_runs` (default: 3) consecutive successful runs all had `input_rows=0`.

**Example output**:
```
silver_transform IDLE       last 3 successful runs read no new rows
```

**Action**: 
- Check if data is flowing into Bronze/Kafka
- Verify the simulator or producer is running
- Review the Kafka topic for new messages
- If the pipeline is intentionally paused, IDLE is expected

---

### DELAYED (critical)
**Meaning**: No successful run within the expected window, but the stage hasn't failed.

**Triggers**:
1. No successful runs in the lookback window (default: 24 hours), OR
2. Last success was more than `max_success_age_minutes` (default: 15 minutes) ago

**Example output**:
```
quality_metrics DELAYED    last success 22.0 minutes ago
```

**Action**:
1. Check if the stage is scheduled to run
2. Review cluster/job logs for startup delays
3. Check for resource constraints (memory, CPU)
4. Verify dependencies (upstream tables exist)
5. If using Databricks, check job cluster startup time

---

### STUCK (critical)
**Meaning**: Stage started but hasn't finished within the expected duration.

**Triggers**: A STARTED event exists with no outcome for longer than `max_stage_minutes` (default: 30 minutes).

**Example output**:
```
reconciliation  STUCK      started 35.0 minutes ago, no outcome
```

**Action**:
1. Check Spark UI for hanging tasks
2. Review stage logs for:
   - Resource exhaustion (OOM, disk full)
   - Deadlocks or long-running queries
   - Network issues (Kafka connection lost, Delta storage unavailable)
3. Consider killing and restarting the stage
4. Check for data skew or partition imbalance
5. Review checkpoint corruption (may need checkpoint reset)

---

### FAILED (critical)
**Meaning**: The last stage attempt failed with an error.

**Triggers**: Latest run has status=FAILED with an error message.

**Example output**:
```
gold_metrics    FAILED     ValueError: division by zero
```

**Action**:
1. Read the full error message in `ops_pipeline_stage_runs.error_message`
2. Check stage logs for the full stack trace
3. Common failures:
   - **Schema mismatch**: Column added/removed upstream → update semantic model or fix schema
   - **Data quality**: Null constraint violation → review data validation rules
   - **Resource limit**: OOM → increase executor memory or reduce batch size
   - **Dependency missing**: Upstream table not ready → check stage dependencies
   - **Checkpoint corruption**: Delete checkpoint and restart
4. Fix the root cause and restart the stage

---

### NOT_READY (critical)
**Meaning**: Stage succeeded but didn't publish all required tables.

**Triggers**: The stage has `required_tables` in `reference/monitoring.yml`, and the last successful run didn't publish all of them (status=PUBLISHED).

**Example output**:
```
quality_metrics NOT_READY  not published in the last success: gold_sensor_freshness, gold_compliance
```

**Action**:
1. Check the stage code to verify all outputs are written
2. Review `ops_dataset_publications` for BLOCKED publications (schema validation failures)
3. Common causes:
   - **Schema drift**: Table has undocumented columns → update `reference/semantic_model.yml`
   - **Logic error**: Stage skipped writing some tables due to a conditional path
   - **Delta version unchanged**: Table wasn't modified (e.g., no new data, no-op MERGE) → expected for some stages
4. If a table should not be required, update `reference/monitoring.yml`

---

### MISSING (critical)
**Meaning**: No runs of this stage exist in the lookback window.

**Triggers**: No rows for this stage in `ops_pipeline_stage_runs` within `lookback_hours` (default: 24 hours).

**Example output**:
```
recommendations_job MISSING no runs in the last 24 hours
```

**Action**:
1. Check if the stage is scheduled to run in this environment (local vs. Databricks)
2. Verify the stage is included in the profile (`reference/monitoring.yml` → `profiles.local` or `profiles.databricks`)
3. If the stage should be running:
   - Check job scheduler (Databricks Jobs, cron, etc.)
   - Review cluster logs for startup failures
   - Verify job permissions and cluster access
4. If the stage is not yet implemented or intentionally disabled, remove it from the profile

---

## Exit Codes

The `pipeline_health` job returns:
- **Exit code 0**: All stages are HEALTHY, RUNNING, or IDLE
- **Exit code 1**: At least one stage is FAILED, STUCK, DELAYED, NOT_READY, or MISSING

Critical states (`FAILED`, `STUCK`, `DELAYED`, `NOT_READY`, `MISSING`) trigger alerts in CI/CD pipelines or monitoring systems.

## Configuration

Policy thresholds are defined in `reference/monitoring.yml`:

```yaml
max_success_age_minutes: 15  # DELAYED if last success is older
max_stage_minutes: 30        # STUCK if running longer than this
lookback_hours: 24           # History window for evaluation
idle_stage: silver_transform # Stage checked for zero-input runs
idle_runs: 3                 # Consecutive zero-input runs → IDLE
anomaly_window_minutes: 15   # For equipment_health active anomalies

profiles:
  local:
    - bronze_ingest
    - silver_transform
    - gold_metrics
    - name: quality_metrics
      required_tables: [gold_data_quality_scorecard, gold_sensor_freshness, gold_compliance, gold_equipment_health]
    - recommendations_job
    - reconciliation
    - semantic_layer

  databricks:
    - silver_transform
    - plant_silver
    - plant_features
    - gold_metrics
    - name: quality_metrics
      required_tables: [gold_data_quality_scorecard, gold_sensor_freshness, gold_compliance, gold_equipment_health]
    - recommendations_job
    - reconciliation
    - semantic_layer
```

### Tuning Guidelines

- **`max_success_age_minutes`**: Set based on your scheduling interval. For a 5-minute schedule, 15 minutes allows for up to 2 missed runs before alerting.
- **`max_stage_minutes`**: Set based on p99 stage duration. Check `ops_pipeline_stage_runs.duration_seconds` to calibrate.
- **`lookback_hours`**: Must be long enough to include at least one expected run. For daily jobs, use 36-48 hours.
- **`idle_runs`**: Set based on how many consecutive idle runs indicate a real problem vs. normal gaps in data arrival.

## Troubleshooting

### False positives

**DELAYED due to intentional pause**: Adjust `max_success_age_minutes` or temporarily remove the stage from the profile.

**IDLE during known data gaps**: Increase `idle_runs` or adjust the idle detection logic if your data arrival pattern is bursty.

### False negatives

**Stage fails silently without recording**: Ensure all stages use `StageRun` context manager (see [Adding Tracking to a New Stage](#adding-tracking-to-a-new-stage)).

**Stage succeeds but writes nothing**: Use `required_tables` to enforce publication of critical outputs.

### Diagnostic queries

```python
# Find recent failures with full error messages
spark.read.format("delta").load(settings.ops_stage_runs_path).filter(
    col("status") == "FAILED"
).select("stage_name", "finished_at", "duration_seconds", "error_message").orderBy(
    col("finished_at").desc()
).show(10, truncate=False)

# Check publication history for a stage
spark.read.format("delta").load(settings.ops_publications_path).filter(
    col("stage_name") == "quality_metrics"
).select("recorded_at", "table_name", "delta_version", "rows_written", "status", "error_message").orderBy(
    col("recorded_at").desc()
).show(20, truncate=False)

# Stage duration percentiles
spark.read.format("delta").load(settings.ops_stage_runs_path).filter(
    col("status") == "SUCCEEDED"
).select("stage_name", "duration_seconds").groupBy("stage_name").agg(
    expr("percentile_approx(duration_seconds, 0.5)").alias("p50"),
    expr("percentile_approx(duration_seconds, 0.99)").alias("p99"),
).show()
```

## Adding Tracking to a New Stage

Every streaming or batch job must use the `StageRun` context manager to record its execution:

```python
from streaming.ops import StageRun

def run(settings: Settings) -> None:
    spark = create_spark("my-new-stage", delta=True)
    
    with StageRun(
        spark, 
        settings, 
        "my_new_stage",
        inputs=["upstream_table_a", "upstream_table_b"],
        outputs=["my_output_table"]
    ) as stage:
        # For streaming queries, record input rows:
        query = spark.readStream.format("delta").load(...).writeStream.start()
        stage.record_stream(query)  # Records input_rows and source_offsets
        query.awaitTermination()
        
        # For batch jobs, just write outputs:
        result = spark.read.format("delta").load(...)
        settings.write_delta(result, settings.table_target("my_output_table"))
```

Then add the stage to the appropriate profile in `reference/monitoring.yml`:

```yaml
profiles:
  local:
    # ... existing stages ...
    - my_new_stage

  databricks:
    # ... existing stages ...
    - my_new_stage
```

If the stage must publish specific tables to be considered healthy:

```yaml
profiles:
  local:
    - name: my_new_stage
      required_tables: [critical_output_a, critical_output_b]
```

## Related Documentation

- [Stage Run Tracking Design](../decisions/ADR-006-stage-run-tracking.md) (if it exists, or remove this line)
- [Maintenance Runbook](./maintenance.md) - Checkpoint management and replay procedures
- [Local Replay](./local-replay.md) - Replaying stages from Kafka or Bronze
- [Azure Databricks Runbook](./azure-databricks.md) - Cloud deployment and monitoring
