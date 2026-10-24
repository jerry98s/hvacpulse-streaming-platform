from pathlib import Path

import pytest

from streaming.config import Settings
from streaming.jobs.maintenance import sql_identifier

pytestmark = pytest.mark.unit


def test_local_table_uses_delta_path_reference():
    settings = Settings(root=Path("C:/Temp/hvacpulse-local"))
    assert sql_identifier(settings, "bronze_telemetry") == (
        f"delta.`{Path('C:/Temp/hvacpulse-local')}/delta/bronze_telemetry`"
    )


def test_unity_catalog_table_quotes_each_name_part():
    settings = Settings(catalog="hvacpulse_dev", schema="streaming")
    assert sql_identifier(settings, "bronze_telemetry") == "`hvacpulse_dev`.`streaming`.`bronze_telemetry`"
