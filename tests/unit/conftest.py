import random
from datetime import datetime
from datetime import timezone

import pytest

from ingestion.events import DEFAULT_DEVICES
from ingestion.events import build_event


@pytest.fixture
def device():
    return DEFAULT_DEVICES[0]  # ahu-001, schema v1


@pytest.fixture
def now():
    return datetime.now(timezone.utc)


@pytest.fixture
def valid_event(device, now):
    return build_event(
        device,
        rng=random.Random(7),
        sequence_number=1,
        event_time=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    )
