"""Late-correction classification (pure Python, no Spark).

A correction is "late" when a superseding revision (revision_number > 1)
arrives more than the Silver watermark after the original event time — i.e.
it may land in a Gold window that was already published as FINAL. Those
corrections are tracked explicitly so restatement is auditable.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def is_late_correction(
    revision_number: int,
    event_time: str,
    ingest_ts: str,
    watermark: timedelta,
) -> bool:
    """True when a superseding revision arrived after the watermark horizon."""
    if revision_number <= 1:
        return False
    return (_parse(ingest_ts) - _parse(event_time)) > watermark
