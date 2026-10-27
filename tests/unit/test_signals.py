from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from quality import signals

pytestmark = pytest.mark.unit

LIMITS = signals.EquipmentLimits(
    device_id="ahu-001",
    site_id="factory-kl-01",
    equipment_type="AHU",
    rated_power_kw=25.0,
    temp_min_c=18.0,
    temp_max_c=26.0,
    humidity_min_pct=40.0,
    humidity_max_pct=65.0,
    expected_report_interval_sec=5.0,
    commissioning_status="ACTIVE",
)


def _event(zone_temp=23.0, humidity=55.0, running=True, power=10.0,
           fan=70.0, airflow=8000.0, valve=60.0, supply=14.0, ret=23.5):
    return {
        "measurements": {
            "zone_temp_c": zone_temp,
            "relative_humidity_pct": humidity,
            "power_kw": power,
            "fan_speed_pct": fan,
            "airflow_m3h": airflow,
            "chilled_water_valve_pct": valve,
            "supply_air_temp_c": supply,
            "return_air_temp_c": ret,
        },
        "operating_state": {"mode": "AUTO", "equipment_running": running},
    }


def test_nominal_event_has_no_signals():
    assert signals.environmental_violations(_event(), LIMITS) == []
    assert signals.equipment_state_inconsistencies(_event(), LIMITS) == []


def test_zone_temp_above_limit_is_violation_not_corrupt():
    assert signals.environmental_violations(_event(zone_temp=29.0), LIMITS) == [
        "ENVIRONMENTAL_LIMIT_VIOLATION:ZONE_TEMP"
    ]


def test_humidity_below_limit_is_violation():
    assert signals.environmental_violations(_event(humidity=20.0), LIMITS) == [
        "ENVIRONMENTAL_LIMIT_VIOLATION:HUMIDITY"
    ]


def test_not_running_with_high_power_is_inconsistent():
    got = signals.equipment_state_inconsistencies(_event(running=False, power=20.0), LIMITS)
    assert "EQUIPMENT_STATE_INCONSISTENT:NOT_RUNNING_HIGH_POWER" in got


def test_fan_stopped_with_high_airflow_is_inconsistent():
    got = signals.equipment_state_inconsistencies(_event(fan=0.0, airflow=9000.0), LIMITS)
    assert "EQUIPMENT_STATE_INCONSISTENT:FAN_STOPPED_HIGH_AIRFLOW" in got


def test_valve_closed_with_cooling_is_inconsistent():
    got = signals.equipment_state_inconsistencies(_event(valve=0.0, supply=14.0, ret=24.0), LIMITS)
    assert "EQUIPMENT_STATE_INCONSISTENT:VALVE_CLOSED_HIGH_COOLING" in got


def test_stale_detection():
    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    stale = (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    assert not signals.is_stale(fresh, now=now, expected_interval_sec=5)
    assert signals.is_stale(stale, now=now, expected_interval_sec=5)


def test_sequence_gap():
    assert signals.sequence_gap(None, 10) == 0
    assert signals.sequence_gap(10, 11) == 0
    assert signals.sequence_gap(10, 15) == 4
    assert signals.sequence_gap(15, 10) == 0  # out-of-order / restart is not a gap


def test_load_equipment_limits_matches_simulator_devices():
    limits = signals.load_equipment_limits()
    from ingestion.events import DEFAULT_DEVICES

    assert set(limits) == {d.device_id for d in DEFAULT_DEVICES}
    assert limits["ahu-001"].rated_power_kw == 25.0
