from pathlib import Path

import pytest

from streaming.config import ensure_uc_checkpoint_root

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def test_lakeflow_owns_only_protected_bronze():
    source = (ROOT / "databricks" / "pipeline" / "lakeflow_pipeline.py").read_text()
    assert 'name="bronze_telemetry"' in source
    assert '"pipelines.reset.allowed": "false"' in source
    assert 'cluster_by=["ingest_ts"]' in source
    assert "dp.materialized_view" not in source
    assert 'name="silver_telemetry_history"' not in source


def test_cloud_silver_reuses_shared_incremental_job_with_volume_checkpoint():
    source = (ROOT / "databricks" / "notebooks" / "run_silver.py").read_text()
    assert "from streaming.jobs.silver_transform import run" in source
    assert "checkpoint_root=ensure_uc_checkpoint_root(spark, catalog, schema)" in source
    assert "run(settings, once=True)" in source


def test_cloud_plant_reads_bronze_incrementally():
    source = (ROOT / "databricks" / "notebooks" / "run_plant.py").read_text()
    assert "checkpoint_root=ensure_uc_checkpoint_root(spark, catalog, schema)" in source
    assert "silver_run(settings, once=True)" in source
    assert "features_run(settings, once=True)" in source
    assert "settings.read_delta(spark, settings.bronze_path)" not in source


def test_bundle_passes_synced_source_and_unity_catalog_to_every_batch_task():
    bundle = (ROOT / "databricks.yml").read_text()
    assert "bundle.sourcePath: ${workspace.file_path}" in bundle
    assert "pipeline_id: ${resources.pipelines.hvacpulse_pipeline.id}" in bundle
    assert bundle.count("source_path: ${workspace.file_path}") == 8
    assert bundle.count("catalog: ${var.catalog}") >= 8
    assert bundle.count("schema: ${var.schema}") >= 8


def test_every_batch_task_records_the_databricks_run_id():
    bundle = (ROOT / "databricks.yml").read_text()
    batch = bundle[bundle.index("hvacpulse_batch:") : bundle.index("hvacpulse_health:")]
    assert batch.count('run_id: "{{job.run_id}}"') == batch.count("notebook_task:") == 7
    for notebook in (ROOT / "databricks" / "notebooks").glob("run_*.py"):
        source = notebook.read_text()
        if notebook.stem in {"run_all_batch", "run_pipeline_health"}:
            continue
        assert 'run_id=dbutils.widgets.get("run_id") or None' in source, notebook.name


def test_failures_and_unhealthy_stages_notify_the_alert_address():
    bundle = (ROOT / "databricks.yml").read_text()
    batch = bundle[bundle.index("hvacpulse_batch:") : bundle.index("hvacpulse_health:")]
    health = bundle[bundle.index("hvacpulse_health:") :]
    assert "on_failure:\n          - ${var.alert_email}" in batch
    assert "metric: RUN_DURATION_SECONDS" in batch
    assert "on_failure:\n          - ${var.alert_email}" in health
    assert "notebook_path: ./databricks/notebooks/run_pipeline_health.py" in health
    assert "quartz_cron_expression" in health


def test_bundle_runs_gold_after_silver():
    bundle = (ROOT / "databricks.yml").read_text()
    gold = bundle.index("task_key: gold_metrics")
    assert "- task_key: silver" in bundle[gold : bundle.index("notebook_task", gold)]


def test_bundle_applies_semantic_layer_after_every_table_writer():
    bundle = (ROOT / "databricks.yml").read_text()
    semantic = bundle.index("task_key: semantic_layer")
    depends = bundle[semantic : bundle.index("notebook_task", semantic)]
    assert "- task_key: reconciliation" in depends
    assert "- task_key: plant_features_and_shadow_control" in depends


class _RecordingSpark:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


def test_uc_checkpoint_root_creates_volume_under_catalog_and_schema():
    spark = _RecordingSpark()
    assert ensure_uc_checkpoint_root(spark, "hvacpulse_dev", "streaming") == (
        "/Volumes/hvacpulse_dev/streaming/checkpoints"
    )
    assert spark.statements == [
        "CREATE VOLUME IF NOT EXISTS `hvacpulse_dev`.`streaming`.`checkpoints`"
    ]


@pytest.mark.parametrize(("catalog", "schema"), [("", "streaming"), ("hvacpulse_dev", "")])
def test_uc_checkpoint_root_rejects_missing_names(catalog, schema):
    with pytest.raises(ValueError):
        ensure_uc_checkpoint_root(_RecordingSpark(), catalog, schema)
