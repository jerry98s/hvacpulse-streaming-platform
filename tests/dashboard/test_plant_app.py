"""Exercise actual Delta-backed Streamlit chart, table, filter and empty states."""

import json
from pathlib import Path

import pandas as pd
import pytest
from deltalake import write_deltalake
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.dashboard


def test_delta_dashboard_charts_tables_and_day_filter(tmp_path, monkeypatch):
    rows = [
        {
            "scope": scope,
            "captured_at": f"2026-09-{day:02d}T00:00:00.000000+00:00",
            "cooling_kw": float(day * 10),
            "power_kw": float(day),
        }
        for scope in ["tenant/site/plant", "other/site/plant"]
        for day in [9, 10]
    ]
    write_deltalake(str(tmp_path / "delta/gold_plant_features"), pd.DataFrame(rows))
    monkeypatch.setenv("HVACPULSE_DATA_ROOT", str(tmp_path))
    app = AppTest.from_file(str(ROOT / "dashboard/app.py"), default_timeout=30).run()
    assert not app.exception and not app.error
    assert any("SYNTHETIC MOCK" in x.value for x in app.warning)
    charts = [*app.get("vega_lite_chart"), *app.get("arrow_vega_lite_chart")]
    assert len(charts) == 2
    assert json.loads(charts[0].proto.spec)["encoding"]["x"]["scale"]["type"] == "utc"
    assert len(app.dataframe) >= 1
    day = next(x for x in app.selectbox if x.label == "Feature day (UTC)")
    day.select_index(0).run()
    assert not app.exception
    assert next(x for x in app.metric if x.label == "Mean cooling load (kW)").value == "90.0"
    scope = next(x for x in app.selectbox if x.label.startswith("Plant ("))
    scope.select_index(1).run()
    assert not app.exception


def test_empty_delta_has_actionable_message(tmp_path, monkeypatch):
    monkeypatch.setenv("HVACPULSE_DATA_ROOT", str(tmp_path))
    app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run()
    assert not app.exception
    assert any("No plant features yet" in x.value for x in app.info)


def test_model_revocation_is_visible(tmp_path, monkeypatch):
    scope = "tenant/site/plant"
    at = "2026-09-10T00:00:00.000000+00:00"
    tables = {
        "gold_plant_features": [
            {"scope": scope, "row_id": "fresh", "captured_at": at, "cooling_kw": 300.0, "power_kw": 100.0}
        ],
        "quarantine_plant_snapshots": [
            {
                "scope": scope,
                "row_id": "bad-training-row",
                "captured_at": at,
                "available_at": at,
                "issues": '["IDEMPOTENCY_CONFLICT"]',
                "completeness": 1.0,
            }
        ],
        "plant_model_registry": [
            {
                "scope": scope,
                "model_id": "model",
                "trained_at": at,
                "approved": True,
                "metrics_json": '{"test":{"rmse_kw":2.0}}',
                "artifact_json": "{}",
                "lineage_json": '{"feature_ids":["bad-training-row"]}',
            }
        ],
        "gold_plant_control_decisions": [
            {
                "scope": scope,
                "model_id": "model",
                "feature_id": "fresh",
                "decision_id": "decision",
                "captured_at": at,
                "decided_at": at,
                "mode": "simulate",
                "state": "BLOCKED",
                "reason": "MODEL_NOT_APPROVED",
                "result_json": '{"state":"BLOCKED"}',
            }
        ],
    }
    for name, rows in tables.items():
        write_deltalake(str(tmp_path / "delta" / name), pd.DataFrame(rows))
    monkeypatch.setenv("HVACPULSE_DATA_ROOT", str(tmp_path))
    app = AppTest.from_file(str(ROOT / "dashboard/app.py"), default_timeout=30).run()
    assert not app.exception
    assert any("Model revoked" in x.value for x in app.warning)
    assert any("MODEL_NOT_APPROVED" in x.value for x in app.markdown)
