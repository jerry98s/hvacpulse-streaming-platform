"""Apply the semantic model to the lakehouse.

Every run checks each existing table against ``reference/semantic_model.yml``
and fails on undocumented or missing columns. On Unity Catalog it then applies
table comments, tags and column comments, and creates the metric views.
Tables not created yet are skipped. Lakeflow-owned tables keep the comment set
by the pipeline, because the pipeline owns their metadata.

Usage: python -m streaming.jobs.semantic_layer
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from streaming.config import Settings
from streaming.config import load_settings
from streaming.ops import StageRun
from streaming.semantic import SemanticModel
from streaming.semantic import load_model
from streaming.semantic import metric_view_ddl
from streaming.semantic import schema_drift
from streaming.semantic import table_ddl
from streaming.spark_session import create_spark


def apply_semantic_model(spark: SparkSession, settings: Settings, model: SemanticModel) -> dict[str, int]:
    present: dict[str, list[str]] = {}
    for name in model.tables:
        target = settings.table_target(name)
        if settings.delta_exists(spark, target):
            present[name] = settings.read_delta(spark, target).columns
        else:
            print(f"skip {name}: not created yet")

    problems = [p for name, columns in present.items() for p in schema_drift(model.tables[name], columns)]
    if problems:
        raise ValueError("Semantic model does not match the tables:\n" + "\n".join(problems))

    applied = {"tables": 0, "metric_views": 0}
    if not settings.uses_unity_catalog:
        print(f"semantic model matches {len(present)} local tables; Unity Catalog DDL skipped")
        return applied

    for name, columns in present.items():
        table = model.tables[name]
        if table.managed_by:
            print(f"skip {name} DDL: metadata owned by {table.managed_by}")
            continue
        for statement in table_ddl(model, table, settings.catalog, settings.schema, columns):
            spark.sql(statement)
        applied["tables"] += 1
    for view in model.metric_views.values():
        if view.source in present:
            spark.sql(metric_view_ddl(view, settings.catalog, settings.schema))
            applied["metric_views"] += 1
    print(f"documented {applied['tables']} tables; created {applied['metric_views']} metric views")
    return applied


def run(settings: Settings) -> None:
    spark = create_spark("hvacpulse-semantic-layer", delta=True)
    with StageRun(spark, settings, "semantic_layer"):
        apply_semantic_model(spark, settings, load_model())


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Apply the semantic model").parse_args(argv)
    run(load_settings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
