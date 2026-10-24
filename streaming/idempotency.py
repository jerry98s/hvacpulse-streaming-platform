"""Idempotency primitives: payload hashing, business keys, retry classification.

Pure Python reference implementation used by tests and by the simulator; the
streaming jobs apply the same decisions in Spark/Delta form (see
``streaming.jobs.silver_transform``).
"""

from __future__ import annotations

import hashlib
import json

INSERT = "INSERT"
SAFE_RETRY = "SAFE_RETRY"
IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"


def payload_hash(payload: dict) -> str:
    """Deterministic hash of a business payload, independent of key order."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def business_event_key(event: dict) -> str:
    """event_id when present; otherwise a deterministic gateway-derived key.

    ``sensor_id + event_time`` is deliberately not used: timestamps can repeat
    or shift, and sequence numbers restart on reboot.
    """
    if event.get("event_id"):
        return event["event_id"]
    parts = [
        event.get("gateway_id", ""),
        event.get("device_id", ""),
        event.get("boot_id", ""),
        str(event.get("sequence_number", "")),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def classify(existing_hash: str | None, incoming_hash: str) -> str:
    """Merge decision for an incoming event against silver_telemetry_history."""
    if existing_hash is None:
        return INSERT
    if existing_hash == incoming_hash:
        return SAFE_RETRY
    return IDEMPOTENCY_CONFLICT


def select_current_revision(revisions: list[dict]) -> dict:
    """The current business state: highest valid revision of a logical event."""
    if not revisions:
        raise ValueError("revisions must be non-empty")
    return max(revisions, key=lambda e: int(e.get("revision_number", 1)))


def recommendation_id(
    equipment_id: str, window_end_iso: str, recommendation_type: str, rule_version: str
) -> str:
    """Deterministic recommendation identity: reprocessing a window re-derives it."""
    raw = "|".join([equipment_id, window_end_iso, recommendation_type, rule_version])
    return "rec-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
