"""Pipeline execution tracking: stage runs, dataset publications and health.

Every stage appends a STARTED row to ``ops_pipeline_stage_runs`` and, when it
ends, a SUCCEEDED or FAILED row for the same (run_id, stage_name, attempt).
Each output table whose Delta version advanced during the stage gets an
``ops_dataset_publications`` row with the committed version. Both tables are
append-only: blind appends never conflict, so stages that run in parallel on
Databricks can record at the same time.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from functools import lru_cache
from pathlib import Path

import yaml
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import max as max_

from streaming.config import REPO_ROOT
from streaming.config import Settings

MONITORING_PATH = REPO_ROOT / "reference" / "monitoring.yml"

STARTED = "STARTED"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
PUBLISHED = "PUBLISHED"
BLOCKED = "BLOCKED"

HEALTHY = "HEALTHY"
RUNNING = "RUNNING"
IDLE = "IDLE"
DELAYED = "DELAYED"
STUCK = "STUCK"
NOT_READY = "NOT_READY"
MISSING = "MISSING"
CRITICAL = frozenset({FAILED, STUCK, DELAYED, NOT_READY, MISSING})

STAGE_RUNS_TABLE = "ops_pipeline_stage_runs"
PUBLICATIONS_TABLE = "ops_dataset_publications"
MAX_ERROR_CHARS = 2000

STAGE_RUN_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("stage_name", T.StringType(), False),
        T.StructField("attempt", T.IntegerType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("event_at", T.TimestampType(), False),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("finished_at", T.TimestampType(), True),
        T.StructField("duration_seconds", T.DoubleType(), True),
        T.StructField("input_rows", T.LongType(), True),
        T.StructField("input_versions_json", T.StringType(), True),
        T.StructField("source_offsets_json", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
    ]
)

PUBLICATION_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("stage_name", T.StringType(), False),
        T.StructField("attempt", T.IntegerType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("delta_version", T.LongType(), False),
        T.StructField("committed_at", T.TimestampType(), True),
        T.StructField("operations", T.StringType(), True),
        T.StructField("rows_written", T.LongType(), True),
        T.StructField("checks_passed", T.BooleanType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("recorded_at", T.TimestampType(), False),
    ]
)

_PROCESS_RUN_ID = f"local-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def run_id_for(settings: Settings) -> str:
    """Databricks passes the job run id; a local process shares one generated id."""
    return settings.run_id or _PROCESS_RUN_ID


@dataclass(frozen=True)
class StagePolicy:
    name: str
    required_tables: tuple[str, ...] = ()


@dataclass(frozen=True)
class Monitoring:
    max_success_age_minutes: float
    max_stage_minutes: float
    lookback_hours: float
    idle_stage: str
    idle_runs: int
    anomaly_window_minutes: int
    profiles: dict[str, tuple[StagePolicy, ...]]


def _positive(raw: dict, key: str) -> float:
    value = raw.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"monitoring.{key} must be a positive number")
    return value


def _policy(entry) -> StagePolicy:
    if isinstance(entry, str):
        return StagePolicy(entry)
    if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
        raise ValueError(f"monitoring stage entry needs a name: {entry!r}")
    return StagePolicy(entry["name"], tuple(entry.get("required_tables") or ()))


@lru_cache(maxsize=4)
def load_monitoring(path: Path = MONITORING_PATH) -> Monitoring:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    profiles = {
        name: tuple(_policy(entry) for entry in stages) for name, stages in (raw.get("profiles") or {}).items()
    }
    if set(profiles) != {"local", "databricks"}:
        raise ValueError("monitoring.profiles must define exactly 'local' and 'databricks'")
    idle_stage = raw.get("idle_stage")
    for name, stages in profiles.items():
        if idle_stage not in {s.name for s in stages}:
            raise ValueError(f"monitoring.idle_stage {idle_stage!r} is not a stage of profile {name}")
    return Monitoring(
        max_success_age_minutes=_positive(raw, "max_success_age_minutes"),
        max_stage_minutes=_positive(raw, "max_stage_minutes"),
        lookback_hours=_positive(raw, "lookback_hours"),
        idle_stage=idle_stage,
        idle_runs=int(_positive(raw, "idle_runs")),
        anomaly_window_minutes=int(_positive(raw, "anomaly_window_minutes")),
        profiles=profiles,
    )


def profile_for(settings: Settings) -> str:
    return "databricks" if settings.uses_unity_catalog else "local"


def rows_written(operation_metrics: dict | None) -> int:
    """Rows a Delta commit inserted, updated or wrote, from its operationMetrics."""
    metrics = operation_metrics or {}
    if "numOutputRows" in metrics:
        return int(metrics["numOutputRows"])
    return int(metrics.get("numTargetRowsInserted", 0)) + int(metrics.get("numTargetRowsUpdated", 0))


def stream_input(progress: list[dict]) -> tuple[int, str | None]:
    """Input rows and the last end offsets from a query's recentProgress."""
    total = sum(int(p.get("numInputRows") or 0) for p in progress)
    if not progress:
        return total, None
    offsets = [source.get("endOffset") for source in progress[-1].get("sources", [])]
    return total, json.dumps(offsets)


class StageRun:
    """Context manager that records one attempt of a pipeline stage."""

    def __init__(self, spark, settings: Settings, stage_name: str, *, inputs=(), outputs=()):
        self.spark = spark
        self.settings = settings
        self.stage_name = stage_name
        self.inputs = tuple(inputs)
        self.outputs = tuple(outputs)
        self.run_id = run_id_for(settings)
        self.input_rows: int | None = None
        self.source_offsets_json: str | None = None

    def _version(self, table_name: str) -> int | None:
        target = self.settings.table_target(table_name)
        if not self.settings.delta_exists(self.spark, target):
            return None
        return self.settings.delta_table(self.spark, target).history(1).first().version

    def _next_attempt(self) -> int:
        target = self.settings.table_target(STAGE_RUNS_TABLE)
        if not self.settings.delta_exists(self.spark, target):
            return 1
        latest = (
            self.settings.read_delta(self.spark, target)
            .filter((col("run_id") == self.run_id) & (col("stage_name") == self.stage_name))
            .agg(max_("attempt"))
            .first()[0]
        )
        return (latest or 0) + 1

    def _append(self, table_name: str, rows: list[dict], schema: T.StructType) -> None:
        frame = self.spark.createDataFrame(rows, schema)
        self.settings.write_delta(frame, self.settings.table_target(table_name), mode="append")

    def _stage_row(self, status: str, error: str | None = None) -> dict:
        now = datetime.now(timezone.utc)
        terminal = status != STARTED
        return {
            "run_id": self.run_id,
            "stage_name": self.stage_name,
            "attempt": self.attempt,
            "status": status,
            "event_at": now,
            "started_at": self.started_at,
            "finished_at": now if terminal else None,
            "duration_seconds": (now - self.started_at).total_seconds() if terminal else None,
            "input_rows": self.input_rows,
            "input_versions_json": json.dumps(self.input_versions, sort_keys=True),
            "source_offsets_json": self.source_offsets_json,
            "error_message": error[:MAX_ERROR_CHARS] if error else None,
        }

    def record_stream(self, query) -> None:
        self.input_rows, self.source_offsets_json = stream_input(query.recentProgress)

    def __enter__(self) -> StageRun:
        self.started_at = datetime.now(timezone.utc)
        self.attempt = self._next_attempt()
        self.input_versions = {name: self._version(name) for name in self.inputs}
        self.output_versions = {name: self._version(name) for name in self.outputs}
        self._append(STAGE_RUNS_TABLE, [self._stage_row(STARTED)], STAGE_RUN_SCHEMA)
        return self

    def _publications(self) -> list[dict]:
        from streaming.semantic import load_model
        from streaming.semantic import schema_drift

        model = load_model()
        rows = []
        for name in self.outputs:
            before = self.output_versions[name]
            target = self.settings.table_target(name)
            after = self._version(name)
            if after is None or after == before:
                continue
            table = self.settings.delta_table(self.spark, target)
            history = table.history() if before is None else table.history(after - before)
            commits = [c for c in history.collect() if before is None or c.version > before]
            problems = schema_drift(model.tables[name], self.settings.read_delta(self.spark, target).columns)
            latest = max(commits, key=lambda c: c.version)
            rows.append(
                {
                    "run_id": self.run_id,
                    "stage_name": self.stage_name,
                    "attempt": self.attempt,
                    "table_name": name,
                    "delta_version": latest.version,
                    "committed_at": latest.timestamp,
                    "operations": ",".join(sorted({c.operation for c in commits})),
                    "rows_written": sum(rows_written(c.operationMetrics) for c in commits),
                    "checks_passed": not problems,
                    "status": BLOCKED if problems else PUBLISHED,
                    "error_message": "; ".join(problems)[:MAX_ERROR_CHARS] or None,
                    "recorded_at": datetime.now(timezone.utc),
                }
            )
        return rows

    def __exit__(self, exc_type, exc, tb) -> bool:
        publications = self._publications()
        if publications:
            self._append(PUBLICATIONS_TABLE, publications, PUBLICATION_SCHEMA)
        blocked = [p["table_name"] for p in publications if p["status"] == BLOCKED]
        if exc is not None:
            self._append(STAGE_RUNS_TABLE, [self._stage_row(FAILED, f"{exc_type.__name__}: {exc}")], STAGE_RUN_SCHEMA)
            return False
        if blocked:
            error = f"publication checks failed for {', '.join(blocked)}"
            self._append(STAGE_RUNS_TABLE, [self._stage_row(FAILED, error)], STAGE_RUN_SCHEMA)
            raise RuntimeError(error)
        self._append(STAGE_RUNS_TABLE, [self._stage_row(SUCCEEDED)], STAGE_RUN_SCHEMA)
        return False


@dataclass(frozen=True)
class StageHealth:
    stage_name: str
    status: str
    detail: str


def _attempts(rows: list[dict]) -> list[dict]:
    """Collapse status rows into one record per (run_id, attempt)."""
    attempts: dict[tuple[str, int], dict] = {}
    for row in sorted(rows, key=lambda r: r["event_at"]):
        key = (row["run_id"], row["attempt"])
        attempt = attempts.setdefault(key, {"run_id": row["run_id"], "attempt": row["attempt"]})
        attempt.update(
            started_at=row["started_at"],
            status=row["status"],
            finished_at=row["finished_at"],
            input_rows=row["input_rows"],
            error_message=row["error_message"],
        )
    return list(attempts.values())


def _age_minutes(now: float, then: float) -> float:
    return (now - then) / 60.0


def evaluate_stage(
    policy: StagePolicy,
    stage_rows: list[dict],
    publication_rows: list[dict],
    monitoring: Monitoring,
    now: float,
) -> StageHealth:
    """Health of one stage. Timestamps are epoch seconds, so the result is timezone-independent."""
    attempts = _attempts([r for r in stage_rows if r["stage_name"] == policy.name])
    if not attempts:
        return StageHealth(policy.name, MISSING, f"no runs in the last {monitoring.lookback_hours:g} hours")
    latest = max(attempts, key=lambda a: a["started_at"])
    successes = sorted((a for a in attempts if a["status"] == SUCCEEDED), key=lambda a: a["finished_at"])

    if latest["status"] == FAILED:
        return StageHealth(policy.name, FAILED, latest["error_message"] or "failed without a message")
    if latest["status"] == STARTED and _age_minutes(now, latest["started_at"]) > monitoring.max_stage_minutes:
        return StageHealth(
            policy.name, STUCK, f"started {_age_minutes(now, latest['started_at']):.0f} minutes ago, no outcome"
        )
    if not successes:
        return StageHealth(policy.name, DELAYED, "no successful run in the lookback window")
    last = successes[-1]
    age = _age_minutes(now, last["finished_at"])
    if age > monitoring.max_success_age_minutes:
        return StageHealth(policy.name, DELAYED, f"last success {age:.0f} minutes ago")
    if policy.required_tables:
        published = {
            p["table_name"]
            for p in publication_rows
            if p["run_id"] == last["run_id"]
            and p["attempt"] == last["attempt"]
            and p["stage_name"] == policy.name
            and p["status"] == PUBLISHED
        }
        missing = sorted(set(policy.required_tables) - published)
        if missing:
            return StageHealth(policy.name, NOT_READY, f"not published in the last success: {', '.join(missing)}")
    if policy.name == monitoring.idle_stage:
        recent = successes[-monitoring.idle_runs :]
        if len(recent) == monitoring.idle_runs and all((a["input_rows"] or 0) == 0 for a in recent):
            return StageHealth(policy.name, IDLE, f"last {monitoring.idle_runs} successful runs read no new rows")
    if latest["status"] == STARTED:
        return StageHealth(policy.name, RUNNING, f"running; last success {age:.0f} minutes ago")
    return StageHealth(policy.name, HEALTHY, f"last success {age:.0f} minutes ago")
