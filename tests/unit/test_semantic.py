import re
from pathlib import Path

import pytest
import yaml

from streaming.config import Settings
from streaming.jobs.semantic_layer import apply_semantic_model
from streaming.semantic import DICTIONARY_BEGIN
from streaming.semantic import DICTIONARY_END
from streaming.semantic import Column
from streaming.semantic import Table
from streaming.semantic import data_dictionary
from streaming.semantic import load_model
from streaming.semantic import metric_view_ddl
from streaming.semantic import referenced_columns
from streaming.semantic import schema_drift
from streaming.semantic import table_ddl

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def model():
    return load_model()


def test_model_documents_every_table_the_platform_writes(model):
    config = (ROOT / "streaming" / "config.py").read_text(encoding="utf-8")
    written = set(re.findall(r'_delta_target\("([a-z_]+)"\)', config))
    assert written == set(model.tables)


def test_every_metric_view_reads_a_gold_table(model):
    assert model.metric_views
    for view in model.metric_views.values():
        assert model.tables[view.source].layer == "gold"


def test_readme_data_dictionary_is_generated_from_the_model(model):
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    section = readme[readme.index(DICTIONARY_BEGIN) : readme.index(DICTIONARY_END) + len(DICTIONARY_END)]
    assert section == data_dictionary(model), "run: python -m streaming.semantic --write-readme"


def test_lakeflow_bronze_comment_matches_the_model(model):
    pipeline = (ROOT / "databricks" / "pipeline" / "lakeflow_pipeline.py").read_text(encoding="utf-8")
    assert f'comment="{model.tables["bronze_telemetry"].description}"' in pipeline


def test_referenced_columns_ignores_literals_keywords_and_functions():
    expr = "SUM(energy_kwh) FILTER (WHERE window_status IN ('FINAL', 'RESTATED'))"
    assert referenced_columns(expr) == {"energy_kwh", "window_status"}
    assert referenced_columns("DATE_TRUNC('HOUR', window_start)") == {"window_start"}
    assert referenced_columns("COUNT(1)") == set()


def _write_model(tmp_path, measure_expr):
    spec = {
        "owner": "team",
        "tables": {
            "gold_x": {
                "layer": "gold",
                "domain": "hvac",
                "description": "x",
                "grain": "one row per id",
                "keys": ["id"],
                "columns": {"id": "Identity.", "kwh": "Energy."},
            }
        },
        "metric_views": {
            "mv_x": {
                "source": "gold_x",
                "comment": "x",
                "dimensions": [{"name": "id", "expr": "id", "comment": "Identity."}],
                "measures": [{"name": "kwh", "expr": measure_expr, "comment": "Energy."}],
            }
        },
    }
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump(spec), encoding="utf-8")
    return path


def test_metric_view_rejects_unknown_columns(tmp_path):
    with pytest.raises(ValueError, match="not columns of gold_x"):
        load_model(_write_model(tmp_path, "SUM(power_kw)"))


def test_measure_must_aggregate(tmp_path):
    with pytest.raises(ValueError, match="aggregate"):
        load_model(_write_model(tmp_path, "kwh"))


def test_table_keys_must_be_documented(tmp_path):
    path = _write_model(tmp_path, "SUM(kwh)")
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    spec["tables"]["gold_x"]["keys"] = ["missing"]
    path.write_text(yaml.safe_dump(spec), encoding="utf-8")
    with pytest.raises(ValueError, match="keys"):
        load_model(path)


TABLE = Table(
    name="t",
    layer="gold",
    domain="hvac",
    description="It's a table.",
    grain="one row per id",
    keys=("id",),
    columns=(Column("id", "Identity."), Column("ingest_date", "Partition.", optional=True)),
)


def test_schema_drift_reports_undocumented_and_missing_columns():
    assert schema_drift(TABLE, ["id", "ingest_date"]) == []
    assert schema_drift(TABLE, ["id"]) == []
    assert schema_drift(TABLE, ["ingest_date", "extra"]) == [
        "t.extra: column is not documented",
        "t.id: documented column is missing",
    ]


def test_table_ddl_escapes_quotes_and_skips_absent_columns(model):
    statements = table_ddl(model, TABLE, "cat", "sch", ["id"])
    assert statements[0] == "COMMENT ON TABLE `cat`.`sch`.`t` IS 'It\\'s a table. Grain: one row per id.'"
    assert statements[1].startswith("ALTER TABLE `cat`.`sch`.`t` SET TAGS ('layer' = 'gold'")
    assert statements[2:] == ["ALTER TABLE `cat`.`sch`.`t` ALTER COLUMN `id` COMMENT 'Identity.'"]


def test_metric_view_ddl_wraps_a_valid_spec(model):
    view = model.metric_views["mv_hvac_energy"]
    ddl = metric_view_ddl(view, "cat", "sch")
    assert ddl.startswith("CREATE OR REPLACE VIEW `cat`.`sch`.`mv_hvac_energy` WITH METRICS LANGUAGE YAML AS $$\n")
    spec = yaml.safe_load(ddl.split("$$")[1])
    assert spec["version"] == "1.1"
    assert spec["source"] == "cat.sch.gold_hvac_metrics_current"
    assert {m["name"] for m in spec["measures"]} >= {"energy_kwh", "reported_energy_kwh"}


class _Catalog:
    def __init__(self, tables):
        self.tables = tables

    def tableExists(self, name):
        return name in self.tables


class _Frame:
    def __init__(self, columns):
        self.columns = columns


class _RecordingSpark:
    def __init__(self, tables):
        self.catalog = _Catalog(tables)
        self.tables = tables
        self.statements = []

    def table(self, name):
        return _Frame(self.tables[name])

    def sql(self, statement):
        self.statements.append(statement)


def _uc_tables(model, names):
    return {
        f"cat.sch.{name}": [c.name for c in model.tables[name].columns if not c.optional] for name in names
    }


def test_apply_documents_existing_tables_and_creates_their_metric_views(model):
    spark = _RecordingSpark(_uc_tables(model, ["bronze_telemetry", "gold_hvac_metrics_current"]))
    applied = apply_semantic_model(spark, Settings(catalog="cat", schema="sch"), model)

    assert applied == {"tables": 1, "metric_views": 1}
    assert not any("`bronze_telemetry`" in s for s in spark.statements)
    assert any(s.startswith("COMMENT ON TABLE `cat`.`sch`.`gold_hvac_metrics_current`") for s in spark.statements)
    assert sum("WITH METRICS" in s for s in spark.statements) == 1


def test_apply_fails_on_schema_drift_before_any_ddl(model):
    tables = _uc_tables(model, ["gold_compliance"])
    tables["cat.sch.gold_compliance"].append("surprise")
    spark = _RecordingSpark(tables)
    with pytest.raises(ValueError, match="gold_compliance.surprise"):
        apply_semantic_model(spark, Settings(catalog="cat", schema="sch"), model)
    assert spark.statements == []
