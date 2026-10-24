from datetime import timedelta

import pytest

from streaming.reconciliation import is_late_correction

pytestmark = pytest.mark.unit

WATERMARK = timedelta(minutes=10)


def test_first_revision_is_never_a_late_correction():
    assert not is_late_correction(
        1, "2026-08-26T10:00:00Z", "2026-08-26T12:00:00Z", WATERMARK
    )


def test_revision_within_watermark_is_not_late():
    assert not is_late_correction(
        2, "2026-08-26T10:00:00Z", "2026-08-26T10:05:00Z", WATERMARK
    )


def test_revision_after_watermark_is_late():
    assert is_late_correction(
        2, "2026-08-26T10:00:00Z", "2026-08-26T10:15:01Z", WATERMARK
    )


def test_boundary_is_not_late():
    assert not is_late_correction(
        3, "2026-08-26T10:00:00Z", "2026-08-26T10:10:00Z", WATERMARK
    )
