import pytest

from quality import rules

pytestmark = pytest.mark.unit

KNOWN = {"ahu-001", "ahu-002", "fcu-201"}


def test_valid_event_passes(valid_event, now):
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) is None


def test_non_dict_payload_is_schema_invalid(now):
    assert rules.validate_event(b"\x00\xff garbage", now=now) == rules.SCHEMA_INVALID


@pytest.mark.parametrize("missing", ["event_id", "event_time", "measurements", "device_id"])
def test_missing_required_field(valid_event, now, missing):
    valid_event[missing] = None
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.SCHEMA_INVALID


def test_unsupported_schema_version(valid_event, now):
    valid_event["schema_version"] = 99
    assert (
        rules.validate_event(valid_event, now=now, known_devices=KNOWN)
        == rules.UNSUPPORTED_SCHEMA_VERSION
    )


def test_unknown_equipment(valid_event, now):
    valid_event["device_id"] = "ahu-999-unknown"
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.EQUIPMENT_UNKNOWN


def test_event_too_late(valid_event, now):
    valid_event["event_time"] = "2020-01-01T00:00:00.000Z"
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.EVENT_TOO_LATE


def test_unparseable_timestamp(valid_event, now):
    valid_event["event_time"] = "not-a-timestamp"
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.TIMESTAMP_INVALID


def test_missing_humidity(valid_event, now):
    valid_event["measurements"]["relative_humidity_pct"] = None
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.HUMIDITY_MISSING


def test_humidity_out_of_range(valid_event, now):
    valid_event["measurements"]["relative_humidity_pct"] = 140.0
    assert (
        rules.validate_event(valid_event, now=now, known_devices=KNOWN)
        == rules.HUMIDITY_OUT_OF_RANGE
    )


def test_negative_power(valid_event, now):
    valid_event["measurements"]["power_kw"] = -1.0
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.NEGATIVE_POWER


@pytest.mark.parametrize("field", ["fan_speed_pct", "chilled_water_valve_pct"])
def test_percent_out_of_range(valid_event, now, field):
    valid_event["measurements"][field] = 250.0
    assert (
        rules.validate_event(valid_event, now=now, known_devices=KNOWN)
        == rules.FAN_VALVE_OUT_OF_RANGE
    )


def test_physically_impossible_temperature(valid_event, now):
    valid_event["measurements"]["zone_temp_c"] = 180.0
    assert (
        rules.validate_event(valid_event, now=now, known_devices=KNOWN)
        == rules.TEMPERATURE_PHYSICALLY_IMPOSSIBLE
    )


def test_non_numeric_measurement_is_schema_invalid(valid_event, now):
    valid_event["measurements"]["power_kw"] = "high"
    assert rules.validate_event(valid_event, now=now, known_devices=KNOWN) == rules.SCHEMA_INVALID


def test_rule_precedence_schema_version_before_plausibility(valid_event, now):
    valid_event["schema_version"] = 99
    valid_event["measurements"]["zone_temp_c"] = 180.0
    assert (
        rules.validate_event(valid_event, now=now, known_devices=KNOWN)
        == rules.UNSUPPORTED_SCHEMA_VERSION
    )


def test_normalize_units_celsius_passthrough(valid_event):
    converted, error = rules.normalize_units(valid_event)
    assert error is None
    assert converted["measurements"]["zone_temp_c"] == valid_event["measurements"]["zone_temp_c"]


def test_normalize_units_fahrenheit_converts(valid_event):
    valid_event["measurements"]["zone_temp_c"] = 212.0  # 100 C in F
    valid_event["measurements"]["temperature_unit"] = "F"
    converted, error = rules.normalize_units(valid_event)
    assert error is None
    assert converted["measurements"]["zone_temp_c"] == pytest.approx(100.0)
    assert "temperature_unit" not in converted["measurements"]


def test_normalize_units_unknown_unit_fails(valid_event):
    valid_event["measurements"]["temperature_unit"] = "K"
    _, error = rules.normalize_units(valid_event)
    assert error == rules.UNIT_CONVERSION_FAILED
