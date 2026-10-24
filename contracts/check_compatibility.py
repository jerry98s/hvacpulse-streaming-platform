"""Validate that every Avro contract is BACKWARD compatible with all older versions.

Azure Event Hubs Schema Registry enforces BACKWARD (new schema reads data written
with the immediately previous schema). This script gives the transitive guarantee:
version N must be able to read data written by *every* retained version < N.

Rules enforced (recursively over nested records):
  * no field removed
  * no field type changed
  * every newly added field has a default (so old payloads still decode)

Usage: python contracts/check_compatibility.py contracts/avro
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


def _version_of(path: Path) -> int:
    match = re.search(r"_v(\d+)\.avsc$", path.name)
    if not match:
        raise ValueError(f"Cannot determine schema version from filename: {path.name}")
    return int(match.group(1))


def _field_map(record: dict) -> dict[str, dict]:
    return {f["name"]: f for f in record.get("fields", [])}


def _check_record(reader: dict, writer: dict, path: str, errors: list[str]) -> None:
    """reader = newer schema, writer = older schema (reader must read writer's data)."""
    writer_fields = _field_map(writer)
    reader_fields = _field_map(reader)
    for name, wfield in writer_fields.items():
        loc = f"{path}.{name}"
        if name not in reader_fields:
            errors.append(f"{loc}: field present in older schema was removed")
            continue
        rfield = reader_fields[name]
        if rfield["type"] != wfield["type"]:
            # Allow recursion into records when both sides are records of the same shape.
            if (
                isinstance(rfield["type"], dict)
                and isinstance(wfield["type"], dict)
                and rfield["type"].get("type") == "record"
                and wfield["type"].get("type") == "record"
            ):
                _check_record(rfield["type"], wfield["type"], loc, errors)
            else:
                errors.append(f"{loc}: type changed from {wfield['type']} to {rfield['type']}")
    for name, rfield in reader_fields.items():
        if name not in writer_fields and "default" not in rfield:
            errors.append(f"{path}.{name}: new field added without a default")


def check_pair(newer: dict, older: dict) -> list[str]:
    errors: list[str] = []
    _check_record(newer, older, newer.get("name", "<root>"), errors)
    return errors


def main(argv: list[str]) -> int:
    schema_dir = Path(argv[1])
    versions = sorted(
        ((p, _version_of(p)) for p in schema_dir.glob("*_v*.avsc")), key=lambda t: t[1]
    )
    if len(versions) < 2:
        print("Fewer than two schema versions found; nothing to check.")
        return 0

    schemas = [(v, json.loads(p.read_text(encoding="utf-8"))) for p, v in versions]
    failed = False
    for i, (new_v, new_schema) in enumerate(schemas):
        for old_v, old_schema in schemas[:i]:
            errors = check_pair(new_schema, old_schema)
            if errors:
                failed = True
                print(f"FAIL v{new_v} cannot read v{old_v} data:")
                for err in errors:
                    print(f"  - {err}")
            else:
                print(f"OK   v{new_v} is backward compatible with v{old_v}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
