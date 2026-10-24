from pathlib import Path

import pytest

from streaming.config import Settings

pytestmark = pytest.mark.unit


def test_local_settings_use_file_uris(tmp_path: Path):
    settings = Settings(root=tmp_path)
    assert settings.silver_current_path.startswith("file:")
    assert not settings.is_table_target(settings.silver_current_path)


def test_cloud_settings_use_unity_catalog_identifiers(tmp_path: Path):
    settings = Settings(root=tmp_path, catalog="hvacpulse_dev", schema="streaming")
    assert settings.silver_current_path == "hvacpulse_dev.streaming.silver_telemetry_current"
    assert settings.gold_history_path == "hvacpulse_dev.streaming.gold_hvac_metrics_history"
    assert settings.is_table_target(settings.gold_history_path)
    assert not (tmp_path / "delta").exists()
