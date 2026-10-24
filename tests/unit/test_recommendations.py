from datetime import datetime
from datetime import timezone

import pytest

from streaming import recommendations as rec
from streaming.jobs.recommendations_job import build_restatement_revocations

pytestmark = pytest.mark.unit

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _reading(**overrides) -> rec.WindowReading:
    base = dict(
        device_id="ahu-01",
        site_id="site-1",
        window_start="2026-08-26T11:50:00+00:00",
        window_end="2026-08-26T11:55:00+00:00",
        event_count=5,
        avg_zone_temp_c=20.0,
        avg_setpoint_c=22.0,
        avg_fan_speed_pct=60.0,
        avg_power_kw=4.0,
        equipment_running=True,
        max_event_time="2026-08-26T11:59:30Z",
    )
    base.update(overrides)
    return rec.WindowReading(**base)


def _ctx(**overrides) -> rec.GateContext:
    base = dict(
        now=NOW,
        expected_interval_sec=60.0,
        quarantine_errors_recent=0,
        active_signals=(),
        commissioning_status="ACTIVE",
    )
    base.update(overrides)
    return rec.GateContext(**base)


def test_overcooling_triggers_raise_setpoint():
    result = rec.evaluate(_reading(), _ctx())
    assert result is not None
    assert result["recommendation_type"] == rec.RAISE_SETPOINT
    assert result["mode"] == rec.ADVISORY
    assert result["rule_version"] == rec.RULE_VERSION


def test_undercooling_triggers_increase_cooling():
    result = rec.evaluate(_reading(avg_zone_temp_c=24.5), _ctx())
    assert result["recommendation_type"] == rec.INCREASE_COOLING


def test_comfortable_zone_with_high_fan_triggers_reduce_fan():
    result = rec.evaluate(
        _reading(avg_zone_temp_c=22.2, avg_fan_speed_pct=90.0), _ctx()
    )
    assert result["recommendation_type"] == rec.REDUCE_FAN_SPEED


def test_recommendation_id_is_deterministic():
    a = rec.evaluate(_reading(), _ctx())
    b = rec.evaluate(_reading(), _ctx())
    assert a["recommendation_id"] == b["recommendation_id"]


@pytest.mark.parametrize(
    ("ctx_override", "expected_failure"),
    [
        ({"commissioning_status": "PENDING"}, "EQUIPMENT_NOT_COMMISSIONED"),
        ({"quarantine_errors_recent": 2}, "RECENT_DATA_QUALITY_FAILURES"),
        ({"active_signals": ("NOT_RUNNING_HIGH_POWER",)}, "EQUIPMENT_UNHEALTHY"),
    ],
)
def test_gates_block_recommendation(ctx_override, expected_failure):
    ctx = _ctx(**ctx_override)
    assert expected_failure in rec.gating_failures(_reading(), ctx)
    assert rec.evaluate(_reading(), ctx) is None


def test_insufficient_readings_blocks_recommendation():
    ctx = _ctx()
    reading = _reading(event_count=1)
    assert "INSUFFICIENT_SUPPORTING_READINGS" in rec.gating_failures(reading, ctx)
    assert rec.evaluate(reading, ctx) is None


def test_stale_data_blocks_recommendation():
    ctx = _ctx()
    reading = _reading(max_event_time="2026-08-26T11:00:00Z")
    assert "STALE_DATA" in rec.gating_failures(reading, ctx)
    assert rec.evaluate(reading, ctx) is None


def test_equipment_off_produces_no_recommendation():
    assert rec.evaluate(_reading(equipment_running=False), _ctx()) is None


def test_revocation_is_compensating_event():
    event = rec.build_revocation("rec-123", replacement_recommendation_id="rec-456", now=NOW)
    assert event["event_type"] == rec.RECOMMENDATION_REVOKED
    assert event["recommendation_id"] == "rec-123"
    assert event["replacement_recommendation_id"] == "rec-456"
    assert event["reason"] == rec.SOURCE_METRIC_RESTATED


def test_restatement_replacement_must_match_device_and_window():
    existing = [
        {
            "recommendation_id": "rec-old",
            "device_id": "ahu-01",
            "window_end": "2026-08-26T11:55:00Z",
        }
    ]
    current = [
        {
            "recommendation_id": "rec-unrelated",
            "device_id": "ahu-99",
            "window_end": "2026-08-26T11:55:00Z",
        }
    ]
    restated = {("ahu-01", "2026-08-26T11:55:00")}
    events = build_restatement_revocations(existing, restated, current, NOW)
    assert events[0]["recommendation_id"] == "rec-old"
    assert events[0]["replacement_recommendation_id"] is None


def test_unchanged_recommendation_survives_restatement():
    existing = [
        {
            "recommendation_id": "rec-same",
            "device_id": "ahu-01",
            "window_end": "2026-08-26T11:55:00Z",
        }
    ]
    current = [dict(existing[0])]
    restated = {("ahu-01", "2026-08-26T11:55:00")}
    assert build_restatement_revocations(existing, restated, current, NOW) == []
