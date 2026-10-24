# Data warehouse checklist mapping

How HVACPulse addresses (or intentionally adapts) standard data-warehouse
checklists for a **streaming + Delta Lake** context. Use this in design reviews.

## 1. Data Model Review Checklist (10 items)

| Checklist item | HVACPulse implementation | Evidence |
|---|---|---|
| **Naming conventions** | Layered prefixes: `bronze_`, `silver_`, `gold_`, `quarantine_`. Not a rigid `layer_domain_entity_grain` grammar, but consistent and self-describing. | `streaming/config.py` table names |
| **Comments** | One semantic model documents every table and column (description, grain, keys, layer/domain tags). The semantic-layer job fails on schema drift everywhere and applies table comments, column comments and tags on Unity Catalog. | `reference/semantic_model.yml`, `streaming/jobs/semantic_layer.py` |
| **Partition design** | **Time-based, low-cardinality**: `ingest_date` on Bronze/Silver/quarantine; `window_date` on Gold. Unpartitioned in tiny demos to avoid small-file explosion. | `bronze_ingest.py`, `silver_transform.py`, `gold_metrics.py` |
| **Clear granularity** | Explicit: Bronze = one Kafka record; Silver history = one accepted revision; Silver current = latest revision per `logical_event_id`; Gold current = one `(site_id, device_id, 1-min window)`. | ADR-002, `streaming/schemas.py` |
| **Redundancy check** | Deliberate pairs: history (audit) vs current (queryable latest); Gold current vs Gold history (published versions). | `gold_metrics.py` docstring |
| **Data lineage** | Documented flow: Kafka → Bronze → Silver → Gold; quality/recommendation/reconciliation jobs read Silver/Gold. | ADRs 001–005, runbooks |
| **Dimension associations** | Reference CSV joined on `device_id` in quality/recommendation jobs; no SCD2 in demo. | `quality_metrics.py`, `recommendations_job.py` |
| **Quality checks** | Hard rules → quarantine; DQ scorecard includes **NULL rates** for critical measurements and **quality score** with `dq_status` (PASS/WARN/FAIL). | `gold_data_quality_scorecard` |
| **Historical verification** | Replay runbook + idempotency tests (run twice, same counts). No 7-day backfill in simulator-based demo. | `tests/delta/`, `verify_local.py` |
| **Fallback mechanisms** | Quarantine path, replay from Bronze/Kafka, checkpoint recovery, restatement events. | `docs/runbooks/local-replay.md` |

## 2. Idempotent Design Checklist

HVACPulse uses **Spark Structured Streaming + Delta Lake**, so classic batch
rules map to streaming/Delta primitives:

| Batch rule | Streaming/Delta equivalent in HVACPulse |
|---|---|
| Always `INSERT OVERWRITE`, forbid `INSERT INTO` | **Delta MERGE** on business keys (`event_id`, window key + `metric_version`) |
| Explicit target partition | Merge condition + watermark + explicit partition columns (`ingest_date`, `window_date`) |
| Align WHERE with partition | `dropDuplicatesWithinWatermark` + MERGE condition replaces partition overwrite |
| Accumulating snapshot: `FULL JOIN` + `COALESCE` | `silver_history` append-only + deterministic `silver_current` rebuild (highest revision wins) |
| Real-time tables: OLAP primary-key dedup | Kafka offset checkpoint + `event_id` MERGE |
| **Verification: run twice, same counts** | `tests/delta/test_job_idempotency.py` runs jobs twice and asserts identical counts; `verify_local.py` prints layer counts |

## 3. Data Lake (Delta) Checklist

| Item | Status | Implementation |
|---|---|---|
| Time-based, low-cardinality partitions | **Done** | `ingest_date` (Bronze/Silver/quarantine), `window_date` (Gold) |
| Never partition by high-cardinality | **Done** | No partitioning by `device_id` / `event_id` / timestamp |
| Z-Ordering / clustering | **Not implemented** | Would help large `silver_history` scans; add `OPTIMIZE ... ZORDER BY (device_id, event_time)` if volume grows |
| Compact small files (OPTIMIZE) | **Done** | `streaming/jobs/maintenance.py --optimize` |
| Target 128–256 MB files | **N/A for demo** | Local runs are MB-scale; production would tune `spark.databricks.delta.optimizeWrite` |
| VACUUM / snapshot expiration | **Done** | `streaming/jobs/maintenance.py --vacuum --retention-days 7` |
| MERGE INTO over INSERT OVERWRITE | **Done** | All current/history updates use MERGE |
| Dynamic partition overwrite | **Partial** | Partitioned overwrite used in `silver_current` rebuild; streaming uses MERGE |
| Idempotent writes / batch_id | **Done** | MERGE on `event_id` / window keys + Kafka checkpoint |
| New columns NULLABLE | **Done** | Avro v2 `co2_ppm` is optional; CI compatibility checker enforces backward compatibility |
| Table statistics (ANALYZE) | **Not implemented** | Not critical at demo scale |
| Predicate pushdown / vectorization | **Partial** | Delta format enables it; no explicit monitoring |
| Tiered alerting (file size, partition count, log size) | **Not implemented** | Would be added with production monitoring |

## What to check before "production"

1. **Volume**: If Bronze/Silver exceed ~1 TB/day, add `ZORDER BY (device_id, event_time)` after OPTIMIZE.
2. **Concurrency**: Stagger Gold/quality/recommendation runs to avoid same-table conflicts.
3. **Monitoring**: Wire `dq_status != 'PASS'` to alerts; track file counts and partition counts.
4. **Retention**: Set `VACUUM` retention to your audit requirement (7–30 days).
5. **SLA**: Document expected row-count ranges per table per day; alert on >20% variance.

## 4. Spark Job Checklist

| Item | Status | Implementation |
|---|---|---|
| DataFrames/SQL over RDDs | **Done** | No RDD API anywhere; all transforms are DataFrame ops so Catalyst pushdown/pruning applies |
| No `SELECT *` / avoid `groupByKey` | **Done** | `silver_projection` selects the explicit column contract; aggregations use `groupBy().agg()` (map-side combine) |
| Cache only reused DataFrames + `unpersist()` | **Done** | `silver_transform._process_microbatch` and `gold_metrics.publish_gold` persist reused frames and unpersist in the same scope |
| Broadcast small tables (<200 MB) | **Done** | Equipment-limits joins use explicit `broadcast()` in `quality_metrics.py` |
| AQE enabled | **Done** | `spark.sql.adaptive.enabled` + skew join + partition coalescing set in `streaming/spark_session.py` (Databricks defaults already on) |
| Cluster mode in production | **Done by platform** | Databricks jobs run the driver on the cluster; only local dev uses `local[2]` |
| Dynamic allocation + external shuffle service | **N/A** | Serverless Databricks manages executors; single-node local stack needs neither |
| 128–256 MB output files / coalesce before write | **Partial** | `OPTIMIZE` in the maintenance job compacts small files post-write; demo volume is MB-scale so write-time coalescing is not forced |
| MERGE INTO over partition rewrite | **Done** | All current/history updates are Delta MERGE |
| OOM triage (driver vs executor) | **Documented** | Driver-side `collect()` is only used for small aggregates (window metrics, latest rows); nothing pulls raw telemetry to the driver |
| Checkpoints on reliable storage | **Done** | Local: `.local/checkpoints`; cloud: Silver and plant notebooks use the `checkpoints` Unity Catalog volume (`ensure_uc_checkpoint_root` in `streaming/config.py`). Lakeflow manages the Bronze checkpoint |

## 5. Kafka Checklist

| Item | Status | Implementation |
|---|---|---|
| Producer `acks=all`, retries, bounded in-flight | **Done** | `ingestion/producer.py`: `acks="all"`, `retries=10`, `max_in_flight_requests_per_connection=5` |
| Producer idempotence | **Intentionally downstream** | `kafka-python` does not support `enable.idempotence`; exactly-once is enforced by Spark checkpoint + Delta MERGE on `event_id` (documented in ADR-002). In Azure, the producer targets Event Hubs, which dedupes per partition |
| Broker `min.insync.replicas=2`, no unclean election | **N/A locally / managed in cloud** | Local is a single-broker KRaft container; Azure Event Hubs manages replication internally |
| Consumer manual offset commit | **Done by design** | Structured Streaming never commits offsets to Kafka; offsets live in the Delta checkpoint, which is what makes replays idempotent |
| Producer batching/compression | **Done** | `batch_size=64KB`, `linger_ms=50`, `compression_type="lz4"` |
| KRaft (no ZooKeeper) | **Done** | `infra/docker` runs KRaft mode |
| Schema management | **Done** | Versioned Avro contracts in `contracts/avro`, CI compatibility checker, schema version in record header; Event Hubs Schema Registry provisioned by Terraform |
| Consumer-lag monitoring (Burrow etc.) | **Partial** | `benchmark.py` + DQ scorecard expose throughput/freshness; production would add Burrow or Event Hubs metrics alerts |
| Tiered storage / MirrorMaker 2 / CDC | **N/A** | Out of scope for the demo; noted for enterprise rollout |

