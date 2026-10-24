import json

import pytest

from contracts.check_compatibility import check_pair
from ingestion.simulator import CONTRACTS_DIR

pytestmark = pytest.mark.unit


def _load(version):
    return json.loads((CONTRACTS_DIR / f"hvac_telemetry_v{version}.avsc").read_text())


def test_v2_is_backward_compatible_with_v1():
    assert check_pair(_load(2), _load(1)) == []


def test_removing_a_field_breaks_compatibility():
    v1 = _load(1)
    broken_v2 = _load(2)
    broken_v2["fields"] = [f for f in broken_v2["fields"] if f["name"] != "zone_id"]
    errors = check_pair(broken_v2, v1)
    assert any("zone_id" in e and "removed" in e for e in errors)


def test_new_field_without_default_breaks_compatibility():
    v1 = _load(1)
    broken_v2 = _load(2)
    for field in broken_v2["fields"]:
        if field["name"] == "measurements":
            for mf in field["type"]["fields"]:
                if mf["name"] == "co2_ppm":
                    del mf["default"]
    errors = check_pair(broken_v2, v1)
    assert any("co2_ppm" in e and "default" in e for e in errors)
