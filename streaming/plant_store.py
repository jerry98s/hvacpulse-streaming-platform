"""Small shared Delta boundaries. No automatic schema evolution or file side channel."""

from __future__ import annotations

import os
from contextlib import contextmanager


@contextmanager
def control_writer(settings, *, simulate=False):
    """Cross-process local lock; UC scheduling owns the single shadow writer."""
    if settings.uses_unity_catalog:
        if simulate:
            raise ValueError("Cloud control is shadow-only")
        yield
        return
    lock_path = settings.root / "locks" / "plant-control.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("Another plant control writer is active") from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def merge_rows(spark, settings, frame, target: str, keys: list[str]) -> None:
    rows = frame.dropDuplicates(keys)
    if rows.isEmpty():
        return
    if not settings.delta_exists(spark, target):
        settings.write_delta(rows, target)
        return
    condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
    settings.delta_table(spark, target).alias("t").merge(
        rows.alias("s"), condition
    ).whenNotMatchedInsertAll().execute()


def bounded_rows(frame, limit: int = 20000) -> list[dict]:
    rows = frame.limit(limit + 1).collect()
    if len(rows) > limit:
        raise ValueError(f"Driver workload exceeds {limit} rows; narrow the training window")
    return [row.asDict(recursive=True) for row in rows]
