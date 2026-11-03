import time
from datetime import datetime
from datetime import timedelta

import pytest

from streaming import ops
from streaming.config import REPO_ROOT
from streaming.config import Settings
from streaming.jobs.pipeline_health import check_health
from streaming.jobs.quality_metrics import build_equipment_health
from streaming.jobs.quality_metrics import build_sensor_freshness
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta

MASTER = REPO_ROOT / "reference" / "equipment_master.csv"
HEALTH_COLUMNS = (
    "site_id string, device_id string, event_time timestamp, equipment_running boolean, power_kw double, "
    "fan_speed_pct double, airflow_m3h double, chilled_water_valve_pct double, "
    "return_air_temp_c double, supply_air_temp_c double"
)


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-delta-ops", delta=True)
    yield session
    session.stop()


def _settings(tmp_path, run_id="run-1"):
    return Settings(root=tmp_path, run_id=run_id, known_devices_csv=MASTER)


def _stage_rows(spark, settings):
    return settings.read_delta(spark, settings.ops_stage_runs_path).orderBy("attempt", "event_at").collect()


def _publications(spark, settings):
    if not settings.delta_exists(spark, settings.ops_publications_path):
        return []
    return settings.read_delta(spark, settings.ops_publications_path).collect()


def _late_corrections(spark):
    return spark.createDataFrame(
        [("evt-2", "ahu-001", "factory-kl-01", datetime(2026, 8, 26, 10), datetime(2026, 8, 26, 11), 2,
          "evt-1", "hash", datetime(2026, 8, 26, 11), "2026-08-26")],
        "event_id string, device_id string, site_id string, event_time timestamp, ingest_ts timestamp, "
        "revision_number int, supersedes_event_id string, payload_hash string, tracked_at timestamp, "
        "ingest_date string",
    )  # fmt: skip


def test_stage_records_start_success_and_the_published_version(spark, tmp_path):
    settings = _settings(tmp_path)
    with ops.StageRun(spark, settings, "reconciliation", outputs=["late_corrections"]):
        settings.write_delta(_late_corrections(spark), settings.late_corrections_path)

    rows = _stage_rows(spark, settings)
    assert [(r.status, r.attempt) for r in rows] == [(ops.STARTED, 1), (ops.SUCCEEDED, 1)]
    assert rows[1].duration_seconds >= 0 and rows[1].error_message is None
    [publication] = _publications(spark, settings)
    assert (publication.table_name, publication.delta_version, publication.rows_written) == ("late_corrections", 0, 1)
    assert (publication.status, publication.checks_passed) == (ops.PUBLISHED, True)


def test_retry_in_the_same_run_is_a_new_attempt_and_unchanged_tables_are_not_republished(spark, tmp_path):
    settings = _settings(tmp_path)
    for _ in range(2):
        with ops.StageRun(spark, settings, "reconciliation", outputs=["late_corrections"]):
            if not settings.delta_exists(spark, settings.late_corrections_path):
                settings.write_delta(_late_corrections(spark), settings.late_corrections_path)

    assert [r.attempt for r in _stage_rows(spark, settings) if r.status == ops.SUCCEEDED] == [1, 2]
    assert [p.attempt for p in _publications(spark, settings)] == [1]


def test_failure_mid_stage_records_failed_and_only_the_committed_table(spark, tmp_path):
    settings = _settings(tmp_path)
    outputs = ["late_corrections", "metric_restatement_events"]
    with (
        pytest.raises(RuntimeError, match="second write failed"),
        ops.StageRun(spark, settings, "reconciliation", outputs=outputs),
    ):
        settings.write_delta(_late_corrections(spark), settings.late_corrections_path)
        raise RuntimeError("second write failed")

    rows = _stage_rows(spark, settings)
    assert rows[-1].status == ops.FAILED
    assert rows[-1].error_message == "RuntimeError: second write failed"
    assert [p.table_name for p in _publications(spark, settings)] == ["late_corrections"]


def test_undocumented_column_blocks_the_publication_and_fails_the_stage(spark, tmp_path):
    settings = _settings(tmp_path)
    drifted = _late_corrections(spark).withColumnRenamed("tracked_at", "surprise")
    with (
        pytest.raises(RuntimeError, match="publication checks failed for late_corrections"),
        ops.StageRun(spark, settings, "reconciliation", outputs=["late_corrections"]),
    ):
        settings.write_delta(drifted, settings.late_corrections_path)

    [publication] = _publications(spark, settings)
    assert (publication.status, publication.checks_passed) == (ops.BLOCKED, False)
    assert "late_corrections.surprise: column is not documented" in publication.error_message
    assert _stage_rows(spark, settings)[-1].status == ops.FAILED


def test_health_reads_the_ops_tables(spark, tmp_path):
    settings = _settings(tmp_path)
    with ops.StageRun(spark, settings, "gold_metrics"):
        pass
    with pytest.raises(ValueError), ops.StageRun(spark, settings, "reconciliation"):
        raise ValueError("bad input")

    results = {r.stage_name: r for r in check_health(spark, settings)}
    assert results["gold_metrics"].status == ops.HEALTHY
    assert (results["reconciliation"].status, results["reconciliation"].detail) == (
        ops.FAILED,
        "ValueError: bad input",
    )
    assert results["quality_metrics"].status == ops.MISSING

    later = {r.stage_name: r for r in check_health(spark, settings, now=time.time() + 3600)}
    assert later["gold_metrics"].status == ops.DELAYED


def test_freshness_lists_devices_that_never_reported(spark, tmp_path):
    settings = _settings(tmp_path)
    readings = spark.createDataFrame(
        [("factory-kl-01", "ahu-001", datetime(2026, 8, 26, 10, 0))],
        "site_id string, device_id string, event_time timestamp",
    )
    settings.write_delta(readings, settings.silver_current_path)

    rows = {r.device_id: r for r in build_sensor_freshness(spark, settings).collect()}
    assert set(rows) == {"ahu-001", "ahu-002", "ahu-101", "fcu-201", "ahu-301"}
    silent = rows["ahu-301"]
    assert (silent.site_id, silent.total_events, silent.last_event_time) == ("factory-jb-01", 0, None)
    assert silent.is_stale is True
    assert rows["ahu-001"].total_events == 1


def test_active_anomaly_only_counts_the_device_recent_window(spark, tmp_path):
    settings = _settings(tmp_path)
    latest = datetime(2026, 8, 26, 12, 0)
    not_running_high_power = (False, 20.0, 70.0, 8000.0, 60.0, 23.5, 14.0)
    normal = (True, 10.0, 70.0, 8000.0, 60.0, 23.5, 14.0)
    readings = spark.createDataFrame(
        [
            ("factory-kl-01", "ahu-001", latest - timedelta(hours=1), *not_running_high_power),
            ("factory-kl-01", "ahu-001", latest, *normal),
            ("factory-kl-01", "ahu-002", latest - timedelta(minutes=5), *not_running_high_power),
            ("factory-kl-01", "ahu-002", latest, *normal),
        ],
        HEALTH_COLUMNS,
    )
    settings.write_delta(readings, settings.silver_current_path)

    rows = {r.device_id: r for r in build_equipment_health(spark, settings, active_window_minutes=15).collect()}
    old = rows["ahu-001"]
    assert (old.not_running_high_power_events, old.active_anomaly_events, old.has_active_anomaly) == (1, 0, False)
    recent = rows["ahu-002"]
    assert (recent.active_anomaly_events, recent.has_active_anomaly) == (1, True)
    assert recent.last_event_time == latest


def test_health_check_detects_missing_stage(spark, tmp_path):
    """Pipeline health reports MISSING when no runs exist for a stage."""
    settings = _settings(tmp_path)
    with ops.StageRun(spark, settings, "gold_metrics"):
        pass

    results = {r.stage_name: r for r in check_health(spark, settings)}
    assert results["gold_metrics"].status == ops.HEALTHY
    assert results["silver_transform"].status == ops.MISSING
    assert "no runs in the last" in results["silver_transform"].detail


def test_health_check_detects_not_ready_when_required_table_unpublished(spark, tmp_path):
    """Pipeline health reports NOT_READY when a stage succeeds but doesn't publish required tables."""
    settings = _settings(tmp_path)
    monitoring = ops.load_monitoring()
    qm_policy = next(s for s in monitoring.profiles[ops.profile_for(settings)] if s.name == "quality_metrics")

    with ops.StageRun(spark, settings, "quality_metrics", outputs=[qm_policy.required_tables[0]]):
        path = settings.table_target(qm_policy.required_tables[0])
        settings.write_delta(
            spark.createDataFrame(
                [("site-1", "dev-1", datetime(2026, 8, 26, 10, 0), datetime(2026, 8, 26, 10, 5), "2026-08-26", 10, 2, 12, 1, 0, 0.0, 0.0, 0.0, 0.833, "PASS")],
                "site_id string, device_id string, window_start timestamp, window_end timestamp, window_date string, valid_events int, quarantined_events int, received_events int, distinct_error_codes int, idempotency_conflicts int, null_zone_temp_rate double, null_power_rate double, null_humidity_rate double, quality_score double, dq_status string",
            ),
            path,
        )

    results = {r.stage_name: r for r in check_health(spark, settings)}
    qm = results["quality_metrics"]
    assert qm.status == ops.NOT_READY
    assert "not published" in qm.detail


def test_health_check_reports_healthy_when_all_required_tables_published_for_simple_stage(spark, tmp_path):
    """Pipeline health reports HEALTHY when a simple stage publishes all required tables."""
    settings = _settings(tmp_path)

    with ops.StageRun(spark, settings, "reconciliation", outputs=["late_corrections"]):
        settings.write_delta(_late_corrections(spark), settings.late_corrections_path)

    results = {r.stage_name: r for r in check_health(spark, settings)}
    assert results["reconciliation"].status == ops.HEALTHY


def test_health_check_detects_idle_stage_with_zero_input_runs(spark, tmp_path):
    """Pipeline health reports IDLE when the idle_stage has consecutive zero-input runs."""
    settings = _settings(tmp_path)
    monitoring = ops.load_monitoring()

    for _ in range(monitoring.idle_runs):
        with ops.StageRun(spark, settings, monitoring.idle_stage) as stage:
            stage.input_rows = 0

    results = {r.stage_name: r for r in check_health(spark, settings)}
    idle_result = results[monitoring.idle_stage]
    assert idle_result.status == ops.IDLE
    assert "read no new rows" in idle_result.detail


def test_health_check_clears_idle_when_new_data_arrives(spark, tmp_path):
    """Pipeline health reports HEALTHY when the idle_stage processes new data after being idle."""
    settings = _settings(tmp_path, run_id="run-1")
    monitoring = ops.load_monitoring()

    for _ in range(monitoring.idle_runs):
        with ops.StageRun(spark, settings, monitoring.idle_stage) as stage:
            stage.input_rows = 0

    settings2 = _settings(tmp_path, run_id="run-2")
    with ops.StageRun(spark, settings2, monitoring.idle_stage) as stage:
        stage.input_rows = 100

    results = {r.stage_name: r for r in check_health(spark, settings2)}
    assert results[monitoring.idle_stage].status == ops.HEALTHY


def test_health_check_detects_stuck_stage(spark, tmp_path):
    """Pipeline health reports STUCK when a stage starts but doesn't finish within max_stage_minutes."""
    settings = _settings(tmp_path)
    monitoring = ops.load_monitoring()

    with ops.StageRun(spark, settings, "gold_metrics"):
        now_during_run = time.time() + (monitoring.max_stage_minutes + 1) * 60
        results_during = check_health(spark, settings, now=now_during_run)

    stuck_result = {r.stage_name: r for r in results_during}["gold_metrics"]
    assert stuck_result.status == ops.STUCK
    assert "started" in stuck_result.detail and "no outcome" in stuck_result.detail


def test_pipeline_health_job_returns_nonzero_when_critical_failures_exist(spark, tmp_path):
    """The pipeline_health job returns exit code 1 when any stage is in a critical state."""
    from streaming.jobs.pipeline_health import main

    settings = _settings(tmp_path)

    with (
        pytest.raises(ValueError, match="simulated failure"),
        ops.StageRun(spark, settings, "gold_metrics"),
    ):
        raise ValueError("simulated failure")

    exit_code = main([])
    assert exit_code == 1
