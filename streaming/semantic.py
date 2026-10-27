"""Semantic layer: one YAML model drives Unity Catalog comments and tags,
Databricks metric views, and the README data dictionary.

Usage: python -m streaming.semantic --write-readme
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = REPO_ROOT / "reference" / "semantic_model.yml"
README_PATH = REPO_ROOT / "README.md"
DICTIONARY_BEGIN = "<!-- BEGIN DATA DICTIONARY (generated from reference/semantic_model.yml) -->"
DICTIONARY_END = "<!-- END DATA DICTIONARY -->"
LAYERS = ("bronze", "silver", "quarantine", "gold", "audit", "ml", "ops")
METRIC_VIEW_SPEC_VERSION = "1.1"

_SQL_WORDS = {
    "and", "or", "not", "in", "is", "null", "case", "when", "then", "else", "end",
    "distinct", "as", "true", "false", "filter", "where", "like", "between",
}  # fmt: skip
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'")


@dataclass(frozen=True)
class Column:
    name: str
    description: str
    optional: bool = False


@dataclass(frozen=True)
class Table:
    name: str
    layer: str
    domain: str
    description: str
    grain: str
    keys: tuple[str, ...]
    columns: tuple[Column, ...]
    managed_by: str | None = None

    @property
    def column_names(self) -> set[str]:
        return {c.name for c in self.columns}


@dataclass(frozen=True)
class MetricView:
    name: str
    source: str
    comment: str
    dimensions: tuple[dict, ...]
    measures: tuple[dict, ...]
    filter: str | None = None


@dataclass(frozen=True)
class SemanticModel:
    owner: str
    tables: dict[str, Table]
    metric_views: dict[str, MetricView]


def referenced_columns(expr: str) -> set[str]:
    """Bare identifiers in a SQL expression, excluding literals, keywords and function names."""
    text = _STRING_LITERAL.sub("''", expr)
    names = set()
    for match in _IDENTIFIER.finditer(text):
        is_call = text[match.end() :].lstrip().startswith("(")
        if not is_call and match.group().lower() not in _SQL_WORDS:
            names.add(match.group())
    return names


def _required(mapping: dict, key: str, where: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: '{key}' must be a non-empty string")
    return value.strip()


def _parse_column(name: str, spec, where: str) -> Column:
    if isinstance(spec, str):
        spec = {"description": spec}
    if not isinstance(spec, dict):
        raise ValueError(f"{where}.{name}: expected a description or a mapping")
    return Column(name, _required(spec, "description", f"{where}.{name}"), bool(spec.get("optional", False)))


def _parse_table(name: str, spec: dict) -> Table:
    where = f"tables.{name}"
    layer = _required(spec, "layer", where)
    if layer not in LAYERS:
        raise ValueError(f"{where}: layer must be one of {LAYERS}")
    columns = tuple(_parse_column(n, s, where) for n, s in (spec.get("columns") or {}).items())
    if not columns:
        raise ValueError(f"{where}: at least one column is required")
    keys = tuple(spec.get("keys") or ())
    names = {c.name for c in columns}
    if not keys or not set(keys) <= names:
        raise ValueError(f"{where}: keys {keys} must be documented columns")
    return Table(
        name=name,
        layer=layer,
        domain=_required(spec, "domain", where),
        description=_required(spec, "description", where),
        grain=_required(spec, "grain", where),
        keys=keys,
        columns=columns,
        managed_by=spec.get("managed_by"),
    )


def _parse_fields(entries, where: str, *, aggregate: bool) -> tuple[dict, ...]:
    if not entries:
        raise ValueError(f"{where}: at least one entry is required")
    fields = []
    for entry in entries:
        name = _required(entry, "name", where)
        expr = _required(entry, "expr", f"{where}.{name}")
        if aggregate and "(" not in expr:
            raise ValueError(f"{where}.{name}: a measure must use an aggregate function")
        fields.append({"name": name, "expr": expr, "comment": _required(entry, "comment", f"{where}.{name}")})
    return tuple(fields)


def _parse_metric_view(name: str, spec: dict, tables: dict[str, Table]) -> MetricView:
    where = f"metric_views.{name}"
    source = _required(spec, "source", where)
    if source not in tables:
        raise ValueError(f"{where}: unknown source table {source}")
    view = MetricView(
        name=name,
        source=source,
        comment=_required(spec, "comment", where),
        dimensions=_parse_fields(spec.get("dimensions"), f"{where}.dimensions", aggregate=False),
        measures=_parse_fields(spec.get("measures"), f"{where}.measures", aggregate=True),
        filter=spec.get("filter"),
    )
    names = [f["name"] for f in (*view.dimensions, *view.measures)]
    if len(names) != len(set(names)):
        raise ValueError(f"{where}: dimension and measure names must be unique")
    available = tables[source].column_names
    for expr in [view.filter or "", *(f["expr"] for f in (*view.dimensions, *view.measures))]:
        unknown = referenced_columns(expr) - available
        if unknown:
            raise ValueError(f"{where}: {sorted(unknown)} are not columns of {source}")
    return view


def load_model(path: Path = MODEL_PATH) -> SemanticModel:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    tables = {name: _parse_table(name, spec) for name, spec in (raw.get("tables") or {}).items()}
    views = {
        name: _parse_metric_view(name, spec, tables) for name, spec in (raw.get("metric_views") or {}).items()
    }
    return SemanticModel(owner=_required(raw, "owner", "model"), tables=tables, metric_views=views)


def schema_drift(table: Table, actual_columns) -> list[str]:
    """Differences between the documented and the physical columns of a table."""
    actual = set(actual_columns)
    problems = [f"{table.name}.{c}: column is not documented" for c in sorted(actual - table.column_names)]
    problems += [
        f"{table.name}.{c.name}: documented column is missing"
        for c in table.columns
        if c.name not in actual and not c.optional
    ]
    return problems


def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _quoted(catalog: str, schema: str, name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


def table_ddl(model: SemanticModel, table: Table, catalog: str, schema: str, present_columns) -> list[str]:
    """Unity Catalog comment, tag and column-comment statements for one table."""
    target = _quoted(catalog, schema, table.name)
    tags = {"layer": table.layer, "domain": table.domain, "owner": model.owner}
    present = set(present_columns)
    statements = [
        f"COMMENT ON TABLE {target} IS {_sql_string(f'{table.description} Grain: {table.grain}.')}",
        f"ALTER TABLE {target} SET TAGS ("
        + ", ".join(f"{_sql_string(k)} = {_sql_string(v)}" for k, v in tags.items())
        + ")",
    ]
    statements += [
        f"ALTER TABLE {target} ALTER COLUMN `{c.name}` COMMENT {_sql_string(c.description)}"
        for c in table.columns
        if c.name in present
    ]
    return statements


def metric_view_ddl(view: MetricView, catalog: str, schema: str) -> str:
    body = {
        "version": METRIC_VIEW_SPEC_VERSION,
        "comment": view.comment,
        "source": f"{catalog}.{schema}.{view.source}",
    }
    if view.filter:
        body["filter"] = view.filter
    body["dimensions"] = list(view.dimensions)
    body["measures"] = list(view.measures)
    spec = yaml.safe_dump(body, sort_keys=False, width=1000)
    return f"CREATE OR REPLACE VIEW {_quoted(catalog, schema, view.name)} WITH METRICS LANGUAGE YAML AS $$\n{spec}$$"


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def data_dictionary(model: SemanticModel) -> str:
    """Markdown data dictionary for the README."""
    lines = [
        DICTIONARY_BEGIN,
        "",
        "| Table | Layer | Grain | Key | Description |",
        "|---|---|---|---|---|",
    ]
    for table in model.tables.values():
        lines.append(
            f"| `{table.name}` | {table.layer} | {_cell(table.grain)} | "
            f"{', '.join(f'`{k}`' for k in table.keys)} | {_cell(table.description)} |"
        )
    lines += ["", "**Metric views** (Databricks `WITH METRICS`; query with `MEASURE(<name>)`):", ""]
    lines += ["| View | Source | Dimensions | Measures |", "|---|---|---|---|"]
    for view in model.metric_views.values():
        dims = ", ".join(f"`{d['name']}`" for d in view.dimensions)
        measures = "<br>".join(f"`{m['name']}`: {_cell(m['comment'])}" for m in view.measures)
        lines.append(f"| `{view.name}` | `{view.source}` | {dims} | {measures} |")
    lines += [
        "",
        "Column descriptions for every table live in `reference/semantic_model.yml` and are applied",
        "to Unity Catalog as column comments.",
        "",
        DICTIONARY_END,
    ]
    return "\n".join(lines)


def render_readme(readme: str, model: SemanticModel) -> str:
    start = readme.index(DICTIONARY_BEGIN)
    end = readme.index(DICTIONARY_END) + len(DICTIONARY_END)
    return readme[:start] + data_dictionary(model) + readme[end:]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Semantic model tools")
    parser.add_argument("--write-readme", action="store_true", help="Regenerate the README data dictionary")
    args = parser.parse_args(argv)
    model = load_model()
    if args.write_readme:
        readme = README_PATH.read_text(encoding="utf-8")
        README_PATH.write_text(render_readme(readme, model), encoding="utf-8", newline="\n")
        print(f"updated data dictionary in {README_PATH.name}")
    else:
        print(f"semantic model valid: {len(model.tables)} tables, {len(model.metric_views)} metric views")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
