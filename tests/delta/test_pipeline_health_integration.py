"""Integration test demonstrating pipeline health check failure modes."""

from __future__ import annotations

import time
from datetime import datetime

import pytest

from streaming import ops
from streaming.config import REPO_ROOT
from streaming.config import Settings
from streaming.jobs.pipeline_health import check_health
from streaming.jobs.pipeline_health import main
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta

MASTER = REPO_ROOT / "reference" / "equipment_master.csv"


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-pipeline-health-e2e", delta=True)
    yield session
    session.stop()


def _settings(tmp_path, run_id="run-1"):
    return Settings(root=tmp_path, run_id=run_id, known_devices_csv=MASTER)


def _late_corrections(spark):
    return spark.createDataFrame(
        [
            (
                "evt-2",
                "ahu-001",
                "factory-kl-01",
                datetime(2026, 8, 26, 10),
                datetime(2026, 8, 26, 11),
                2,
                "evt-1",
                "hash",
                datetime(2026, 8, 26, 11),
                "2026-08-26",
            )
        ],
        "event_id string, device_id string, site_id string, event_time timestamp, ingest_ts timestamp, "
        "revision_number int, supersedes_event_id string, payload_hash string, tracked_at timestamp, "
        "ingest_date string",
    )


def test_end_to_end_healthy_pipeline_reports_all_green(spark, tmp_path):
    """A complete pipeline run with all stages succeeding reports all HEALTHY."""
    settings = _settings(tmp_path)

    stages_to_run = ["bronze_ingest", "silver_transform", "gold_metrics", "reconciliation"]

    for stage in stages_to_run:
        if stage == "reconciliation":
            with ops.StageRun(spark, settings, stage, outputs=["late_corrections"]):
                settings.write_delta(_late_corrections(spark), settings.late_corrections_path)
        else:
            with ops.StageRun(spark, settings, stage):
                pass

    results = {r.stage_name: r for r in check_health(spark, settings)}

    for stage in stages_to_run:
        assert results[stage].status == ops.HEALTHY, f"{stage} should be HEALTHY but is {results[stage].status}"


def test_end_to_end_mixed_states_exits_nonzero(spark, tmp_path):
    """A pipeline with mixed states (HEALTHY, MISSING, FAILED) returns exit code 1."""
    settings = _settings(tmp_path)

    with ops.StageRun(spark, settings, "gold_metrics"):
        pass

    with pytest.raises(ValueError, match="test failure"), ops.StageRun(spark, settings, "reconciliation"):
        raise ValueError("test failure")

    results = {r.stage_name: r for r in check_health(spark, settings)}

    assert results["gold_metrics"].status == ops.HEALTHY
    assert results["reconciliation"].status == ops.FAILED
    assert results["silver_transform"].status == ops.MISSING

    assert any(r.status in ops.CRITICAL for r in results.values())


def test_end_to_end_delayed_stage_after_time_passes(spark, tmp_path):
    """A stage becomes DELAYED when enough time passes after its last success."""
    settings = _settings(tmp_path)
    monitoring = ops.load_monitoring()

    with ops.StageRun(spark, settings, "gold_metrics"):
        pass

    now = time.time()
    results_fresh = {r.stage_name: r for r in check_health(spark, settings, now=now)}
    assert results_fresh["gold_metrics"].status == ops.HEALTHY

    future = now + (monitoring.max_success_age_minutes + 1) * 60
    results_stale = {r.stage_name: r for r in check_health(spark, settings, now=future)}
    assert results_stale["gold_metrics"].status == ops.DELAYED


def test_end_to_end_idle_detection_requires_consecutive_zero_input_runs(spark, tmp_path):
    """The idle stage becomes IDLE only after consecutive zero-input successes."""
    settings = _settings(tmp_path, run_id="run-1")
    monitoring = ops.load_monitoring()

    with ops.StageRun(spark, settings, monitoring.idle_stage) as stage:
        stage.input_rows = 100

    results_with_data = {r.stage_name: r for r in check_health(spark, settings)}
    assert results_with_data[monitoring.idle_stage].status == ops.HEALTHY

    for i in range(monitoring.idle_runs):
        settings_run = _settings(tmp_path, run_id=f"run-idle-{i}")
        with ops.StageRun(spark, settings_run, monitoring.idle_stage) as stage:
            stage.input_rows = 0

    results_idle = {r.stage_name: r for r in check_health(spark, settings)}
    assert results_idle[monitoring.idle_stage].status == ops.IDLE


def test_end_to_end_not_ready_when_output_missing(spark, tmp_path):
    """A stage with required_tables reports NOT_READY when outputs are missing."""
    settings = _settings(tmp_path)
    monitoring = ops.load_monitoring()

    qm_policy = next(s for s in monitoring.profiles[ops.profile_for(settings)] if s.name == "quality_metrics")

    with ops.StageRun(spark, settings, "quality_metrics"):
        pass

    results = {r.stage_name: r for r in check_health(spark, settings)}
    qm = results["quality_metrics"]

    assert qm.status == ops.NOT_READY
    assert "not published" in qm.detail
    for required_table in qm_policy.required_tables:
        assert required_table in qm.detail


def test_end_to_end_pipeline_health_main_returns_exit_code(spark, tmp_path, monkeypatch):
    """The pipeline_health main function returns 0 for OK, 1 for critical states."""
    settings = _settings(tmp_path)

    monkeypatch.setenv("HVACPULSE_DATA_ROOT", str(tmp_path))

    with ops.StageRun(spark, settings, "gold_metrics"):
        pass

    exit_code_healthy = main([])
    assert exit_code_healthy == 1

    with ops.StageRun(spark, settings, "silver_transform"):
        pass
    with ops.StageRun(spark, settings, "bronze_ingest"):
        pass
    with ops.StageRun(spark, settings, "reconciliation", outputs=["late_corrections"]):
        settings.write_delta(_late_corrections(spark), settings.late_corrections_path)
    with ops.StageRun(spark, settings, "recommendations_job"):
        pass
    with ops.StageRun(spark, settings, "semantic_layer"):
        pass
    with ops.StageRun(spark, settings, "quality_metrics"):
        pass

    exit_code_some_missing = main([])
    assert exit_code_some_missing == 1
