import pytest

from streaming import idempotency

pytestmark = pytest.mark.unit


def test_payload_hash_is_key_order_independent():
    a = {"x": 1, "y": {"b": 2, "a": 1}}
    b = {"y": {"a": 1, "b": 2}, "x": 1}
    assert idempotency.payload_hash(a) == idempotency.payload_hash(b)


def test_payload_hash_changes_with_content():
    a = {"x": 1}
    b = {"x": 2}
    assert idempotency.payload_hash(a) != idempotency.payload_hash(b)


def test_business_key_prefers_event_id():
    assert idempotency.business_event_key({"event_id": "evt-1", "device_id": "d"}) == "evt-1"


def test_business_key_fallback_is_deterministic():
    event = {
        "gateway_id": "gateway-kl-01",
        "device_id": "ahu-001",
        "boot_id": "boot-1",
        "sequence_number": 42,
    }
    assert idempotency.business_event_key(event) == idempotency.business_event_key(dict(event))
    changed = dict(event, sequence_number=43)
    assert idempotency.business_event_key(event) != idempotency.business_event_key(changed)


def test_classify_transitions():
    assert idempotency.classify(None, "h1") == idempotency.INSERT
    assert idempotency.classify("h1", "h1") == idempotency.SAFE_RETRY
    assert idempotency.classify("h1", "h2") == idempotency.IDEMPOTENCY_CONFLICT


def test_select_current_revision_picks_highest():
    revisions = [
        {"event_id": "e1", "logical_event_id": "L", "revision_number": 1},
        {"event_id": "e3", "logical_event_id": "L", "revision_number": 3},
        {"event_id": "e2", "logical_event_id": "L", "revision_number": 2},
    ]
    assert idempotency.select_current_revision(revisions)["event_id"] == "e3"


def test_recommendation_id_is_deterministic():
    kwargs = {
        "equipment_id": "ahu-001",
        "window_end_iso": "2026-08-26T10:16:00Z",
        "recommendation_type": "RAISE_SETPOINT",
        "rule_version": "rules-v3",
    }
    first = idempotency.recommendation_id(**kwargs)
    assert first == idempotency.recommendation_id(**kwargs)
    assert first != idempotency.recommendation_id(**{**kwargs, "window_end_iso": "2026-08-26T10:17:00Z"})
    assert first.startswith("rec-")
