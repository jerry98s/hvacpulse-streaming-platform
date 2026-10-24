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
    assert bundle.count("source_path: ${workspace.file_path}") == 7
    assert bundle.count("catalog: ${var.catalog}") >= 7
    assert bundle.count("schema: ${var.schema}") >= 7


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
