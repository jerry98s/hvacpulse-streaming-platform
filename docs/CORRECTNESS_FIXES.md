# Correctness and Measurement Fixes

This document describes four correctness/measurement gaps that were identified and fixed in the HVACPulse streaming medallion platform.

## Overview

All four issues have been addressed with backward-compatible changes, new tests, and a reproducible portfolio demonstration. The fixes preserve existing conflict-detection logic, Silver history/current separation, versioned Gold, and operational runbooks.

**Important**: Azure Databricks bundles currently schedule processing every 5 minutes. Claims about sub-five-second latency require a different execution config (e.g., continuous streaming mode) and measured evidence.

## Issue #1: Latency Benchmark Measures Wrong Boundary (HIGH)

### Problem
The latency benchmark in `scripts/local/benchmark.py` was using `ingest_ts` from Silver to calculate processing and end-to-end latency. However, `ingest_ts` is assigned when data lands in Bronze, not when it becomes observable in Silver. This meant:
- Silver processing time was not being measured
- End-to-end latency excluded the Silver commit time
- Interview reliability numbers were potentially misleading

### Root Cause
In `streaming/jobs/bronze_ingest.py`, `ingest_ts` is set when writing to Bronze:
```python
.withColumn("ingest_ts", current_timestamp())
```

This timestamp was then carried forward unchanged into Silver through `streaming/transforms.py`, making it impossible to distinguish Bronze arrival from Silver observability.

### Solution
1. **Schema changes** (`streaming/schemas.py`):
   - Renamed `ingest_ts` to `bronze_ingest_ts` in Silver columns
   - Added `silver_commit_ts` to mark when data becomes observable in Silver

2. **Transform changes** (`streaming/transforms.py`):
   - Renamed Bronze's `ingest_ts` to `bronze_ingest_ts` in flattened output
   - Added `silver_commit_ts` via `current_timestamp()` in `silver_projection()`

3. **Benchmark changes** (`scripts/local/benchmark.py`):
   - Now measures distinct phases:
     - `source_latency_s`: kafka_timestamp - event_time
     - `bronze_processing_s`: bronze_ingest_ts - kafka_timestamp
     - `silver_processing_s`: silver_commit_ts - bronze_ingest_ts (NEW)
     - `end_to_end_latency_s`: silver_commit_ts - event_time

4. **Downstream updates**:
   - Updated `quality_metrics.py`, `recommendations_job.py`, `reconciliation.py`
   - Updated existing tests to use `bronze_ingest_ts`

### Impact
- ✅ Honest, measurable latency boundaries
- ✅ Separate wall-clock timestamps from synthetic event time
- ✅ Interview-ready metrics that reflect actual observability
- ⚠️ Schema change requires Silver table recreation or schema evolution

### Test Coverage
`tests/delta/test_correctness_fixes.py::test_silver_observability_timestamps_distinct_from_event_time`

## Issue #2: Gold History Misses Finalization (HIGH)

### Problem
Gold history was only published when metric values changed (`_content_hash` differs). When a window transitioned from OPEN → FINAL with identical metric values:
- Current table was updated with the new status
- History was NOT published, leaving it showing OPEN
- Users couldn't reconstruct what was known to be FINAL at any point in time

### Root Cause
In `streaming/jobs/gold_metrics.py`, the `publish_gold` function calculated:
```python
"_changed", col("_prior_hash").isNull() | (col("_prior_hash") != col("_content_hash"))
```

Then only published rows where `_changed == True`:
```python
to_publish = versioned.filter(col("_changed")).drop("_changed")
```

Status transitions without value changes were ignored.

### Solution
Modified `publish_gold()` in `streaming/jobs/gold_metrics.py`:
1. Calculate both `_content_changed` and `_status_changed`
2. Set `_changed = _content_changed | _status_changed`
3. Increment `metric_version` on ANY change (`_changed`), not just content
4. Publish to history when either content OR status changes

**Rationale**: The history merge key includes `metric_version`. Status-only changes must increment it to avoid being treated as duplicates. `metric_version` tracks the evolution of a window over time, whether due to content changes (restatement) or lifecycle transitions (finalization).

### Impact
- ✅ Complete lifecycle tracking (OPEN → FINAL → RESTATED)
- ✅ Accurate historical reconstruction
- ✅ Status transitions now trigger history writes
- ✅ Metric version still only increments on value changes

### Test Coverage
- `tests/delta/test_correctness_fixes.py::test_gold_history_publishes_open_to_final_with_unchanged_values`
- `tests/delta/test_portfolio_demo.py::test_late_correction_restatement_triggers_gold_restated`

## Issue #3: Malformed Records Can Disappear from Quarantine (MEDIUM)

### Problem
The fallback deduplication ID for corrupt records (those without a valid `event_id`) was constructed as:
```python
concat(lit("offset-"), col("kafka_offset").cast("string"))
```

This included only the offset, not the topic or partition. Identical corrupt bytes at offset 100 in partition 0 and partition 1 would have the same dedup ID and potentially collapse together, losing visibility into one of them.

### Root Cause
In `streaming/jobs/silver_transform.py` line 183-185, the fallback deduplication key was under-specified.

### Solution
Changed the fallback dedup ID to include topic, partition, and offset:
```python
concat(
    col("kafka_topic"),
    lit("-"),
    col("kafka_partition").cast("string"),
    lit("-"),
    col("kafka_offset").cast("string"),
)
```

The quarantine merge already used `["kafka_topic", "kafka_partition", "kafka_offset", "error_code"]` as the key, so this fix aligns the streaming dedup with the merge semantics.

### Impact
- ✅ Corrupt records at same offset in different partitions both survive
- ✅ Quarantine accurately reflects all rejected records
- ✅ No valid data quarantined (only affects the fallback for malformed events)

### Test Coverage
`tests/delta/test_correctness_fixes.py::test_quarantine_identity_includes_partition`

## Issue #4: Silver Current Fully Rebuilt Every Micro-Batch (MEDIUM)

### Problem
The `_rebuild_current()` function:
1. Read ALL of Silver history
2. Ranked every logical event with a window function
3. Overwrote the entire current table

This meant work grew with accumulated history, making each micro-batch progressively slower as history grew.

### Root Cause
In `streaming/jobs/silver_transform.py` lines 108-123, `_rebuild_current()` was a full scan-and-replace operation.

### Solution
Replaced `_rebuild_current()` with `_update_current()`:
1. **Targeted updates**: Only processes logical IDs affected by the current batch
2. **MERGE semantics**: Uses Delta MERGE for idempotent updates
3. **Equal-revision conflict policy**: When multiple events share the same `revision_number`, deterministically selects the latest by:
   - `revision_number DESC`
   - `event_time DESC`
   - `bronze_ingest_ts DESC`
   - `event_id DESC` (lexicographic)

### Implementation
```python
def _update_current(spark: SparkSession, settings: Settings, inserted: DataFrame) -> None:
    """Update only affected logical IDs in current using revision-aware MERGE."""
    affected_ids = inserted.select("logical_event_id").distinct()
    history = settings.read_delta(spark, settings.silver_history_path)
    history_affected = history.join(affected_ids, "logical_event_id", "inner")
    
    # Rank and select latest per logical_event_id
    latest_first = Window.partitionBy("logical_event_id").orderBy(...)
    updates = history_affected.withColumn("_rn", row_number().over(latest_first))...
    
    # MERGE into current
    target.merge(updates, "t.logical_event_id = s.logical_event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
```

### Impact
- ✅ O(batch_size) instead of O(total_history)
- ✅ Explicit equal-revision conflict resolution
- ✅ Preserves conflict-detection correctness
- ✅ Idempotent MERGE semantics for retries

### Test Coverage
- `tests/delta/test_correctness_fixes.py::test_silver_current_merge_with_equal_revision`
- `tests/delta/test_correctness_fixes.py::test_silver_current_updates_only_affected_ids`

## Portfolio Demonstration

A new test suite (`tests/delta/test_portfolio_demo.py`) provides reproducible demonstrations of three key platform capabilities:

### 1. Duplicate Handling
- **Demo**: Same event delivered three times with identical `event_id` and `payload_hash`
- **Result**: Bronze retains all three; Silver keeps exactly one
- **Extension**: Conflict detection - same `event_id` with different `payload_hash` quarantines as `IDEMPOTENCY_CONFLICT`

### 2. Restart Recovery
- **Demo**: Process batch 1, commit, process batch 2, simulate crash, replay batch 2
- **Result**: Idempotent MERGE ensures no duplication; two distinct events remain

### 3. Late-Correction Restatement
- **Demo**: Publish FINAL Gold window, then introduce late Silver revision (revision_number=2)
- **Result**: 
  - New Gold version published with status RESTATED
  - History preserves OPEN, FINAL, and RESTATED versions
  - `metric_version` increments on value change
  - Late corrections tracked in reconciliation job

### Running the Demos
```powershell
uv run pytest tests/delta/test_portfolio_demo.py -v -m delta
```

## Azure Databricks Scheduling Note

The Azure Databricks bundle (`databricks.yml`) currently schedules streaming jobs to run every 5 minutes. This means:
- **Measured latency**: Minutes, not seconds
- **Continuous mode**: Not currently deployed
- **Interview claims**: Should reference scheduled incremental processing

To deploy continuous streaming:
1. Remove `.trigger(availableNow=True)` from job definitions
2. Change schedule to continuous or very frequent (e.g., every minute)
3. Measure actual latency with benchmark script
4. Update claims accordingly

## Migration Guide

### For Local Development
1. Delete `.local/delta/` to start fresh
2. Run `.\scripts\local\run_local_stack.ps1 -Task all`
3. Benchmark: `python scripts/local/benchmark.py`

### For Azure Databricks
1. Schema evolution should handle the new `silver_commit_ts` column automatically
2. Or drop and recreate Silver tables (Bronze is immutable and unaffected)
3. Redeploy bundle: `databricks bundle deploy -t dev`
4. Monitor first scheduled run for any issues

### Breaking Changes
- Silver schema adds `silver_commit_ts` column
- Silver schema renames `ingest_ts` to `bronze_ingest_ts`
- Existing queries referencing `silver.ingest_ts` must use `silver.bronze_ingest_ts`

## Test Commands

```powershell
# Correctness fixes
uv run pytest tests/delta/test_correctness_fixes.py -v -m delta

# Portfolio demonstration
uv run pytest tests/delta/test_portfolio_demo.py -v -m delta

# Full suite (requires environment setup)
uv run pytest tests/unit -m unit
uv run pytest tests/spark -m spark
uv run pytest tests/delta -m delta
```

## References

- ADR-003: Quarantine vs operational signals
- ADR-004: Delta MERGE idempotency
- ADR-005: Gold versioning and restatement
- `docs/runbooks/plant-pipeline.md`: Safety gates and model lifecycle
- `docs/checklists.md`: Data warehouse review checklist
