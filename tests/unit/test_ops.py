import re
from pathlib import Path

import pytest
import yaml

from streaming import ops
from streaming.semantic import load_model

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
NOW = 1_000_000.0
MINUTE = 60.0


@pytest.fixture(scope="module")
def monitoring():
    return ops.load_monitoring()


def _write_policy(tmp_path, **overrides):
    raw = yaml.safe_load(ops.MONITORING_PATH.read_text(encoding="utf-8"))
    raw.update(overrides)
    path = tmp_path / "monitoring.yml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


def test_every_profile_stage_is_recorded_by_a_job(monitoring):
    jobs = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "streaming" / "jobs").glob("*.py"))
    recorded = set(re.findall(r'StageRun\(\s*spark,\s*settings,\s*"([a-z_]+)"', jobs))
    for stages in monitoring.profiles.values():
        assert {s.name for s in stages} <= recorded


def test_required_tables_are_documented(monitoring):
    tables = set(load_model().tables)
    for stages in monitoring.profiles.values():
        for stage in stages:
            assert set(stage.required_tables) <= tables


def test_quality_stage_requires_all_four_outputs(monitoring):
    for stages in monitoring.profiles.values():
        quality = next(s for s in stages if s.name == "quality_metrics")
        assert len(quality.required_tables) == 4


def test_policy_rejects_unknown_idle_stage(tmp_path):
    with pytest.raises(ValueError, match="idle_stage"):
        ops.load_monitoring(_write_policy(tmp_path, idle_stage="nope"))


def test_policy_rejects_non_positive_threshold(tmp_path):
    with pytest.raises(ValueError, match="max_success_age_minutes"):
        ops.load_monitoring(_write_policy(tmp_path, max_success_age_minutes=0))


def test_policy_requires_both_profiles(tmp_path):
    raw = yaml.safe_load(ops.MONITORING_PATH.read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="profiles"):
        ops.load_monitoring(_write_policy(tmp_path, profiles={"local": raw["profiles"]["local"]}))


def test_rows_written_reads_write_and_merge_metrics():
    assert ops.rows_written({"numOutputRows": "12"}) == 12
    assert ops.rows_written({"numTargetRowsInserted": "3", "numTargetRowsUpdated": "2"}) == 5
    assert ops.rows_written(None) == 0


def test_stream_input_sums_rows_and_keeps_last_offsets():
    progress = [
        {"numInputRows": 4, "sources": [{"endOffset": {"0": 4}}]},
        {"numInputRows": 6, "sources": [{"endOffset": {"0": 10}}]},
    ]
    assert ops.stream_input(progress) == (10, '[{"0": 10}]')
    assert ops.stream_input([]) == (0, None)


def _row(stage, status, *, started, finished=None, run="r1", attempt=1, input_rows=None, error=None):
    return {
        "run_id": run,
        "stage_name": stage,
        "attempt": attempt,
        "status": status,
        "event_at": finished if finished is not None else started,
        "started_at": started,
        "finished_at": finished,
        "input_rows": input_rows,
        "error_message": error,
    }


def _success(stage, minutes_ago, **kwargs):
    started = NOW - minutes_ago * MINUTE - 30
    return [
        _row(stage, ops.STARTED, started=started, **kwargs),
        _row(stage, ops.SUCCEEDED, started=started, finished=NOW - minutes_ago * MINUTE, **kwargs),
    ]


def _status(monitoring, policy, rows, publications=()):
    return ops.evaluate_stage(policy, rows, list(publications), monitoring, NOW).status


def test_recent_success_is_healthy(monitoring):
    assert _status(monitoring, ops.StagePolicy("gold_metrics"), _success("gold_metrics", 2)) == ops.HEALTHY


def test_stage_without_rows_is_missing(monitoring):
    assert _status(monitoring, ops.StagePolicy("gold_metrics"), []) == ops.MISSING


def test_latest_failure_wins_over_earlier_success(monitoring):
    rows = _success("gold_metrics", 6) + [
        _row("gold_metrics", ops.STARTED, started=NOW - 90, run="r2"),
        _row("gold_metrics", ops.FAILED, started=NOW - 90, finished=NOW - 60, run="r2", error="boom"),
    ]
    health = ops.evaluate_stage(ops.StagePolicy("gold_metrics"), rows, [], monitoring, NOW)
    assert (health.status, health.detail) == (ops.FAILED, "boom")


def test_old_success_is_delayed_even_if_output_looks_fine(monitoring):
    rows = _success("quality_metrics", monitoring.max_success_age_minutes + 1)
    assert _status(monitoring, ops.StagePolicy("quality_metrics"), rows) == ops.DELAYED


def test_started_without_outcome_is_running_then_stuck(monitoring):
    recent = _success("gold_metrics", 3) + [_row("gold_metrics", ops.STARTED, started=NOW - 60, run="r2")]
    assert _status(monitoring, ops.StagePolicy("gold_metrics"), recent) == ops.RUNNING
    hung = NOW - (monitoring.max_stage_minutes + 1) * MINUTE
    earlier = _success("gold_metrics", monitoring.max_stage_minutes + 5)
    stuck = earlier + [_row("gold_metrics", ops.STARTED, started=hung, run="r2")]
    assert _status(monitoring, ops.StagePolicy("gold_metrics"), stuck) == ops.STUCK


def test_required_tables_must_all_be_published_by_the_last_success(monitoring):
    policy = ops.StagePolicy("quality_metrics", ("gold_a", "gold_b"))
    rows = _success("quality_metrics", 2)

    def publication(table, status=ops.PUBLISHED):
        return {"run_id": "r1", "attempt": 1, "stage_name": "quality_metrics", "table_name": table, "status": status}

    assert _status(monitoring, policy, rows, [publication("gold_a")]) == ops.NOT_READY
    assert _status(monitoring, policy, rows, [publication("gold_a"), publication("gold_b", ops.BLOCKED)]) == (
        ops.NOT_READY
    )
    assert _status(monitoring, policy, rows, [publication("gold_a"), publication("gold_b")]) == ops.HEALTHY


def test_idle_stage_with_no_new_rows_is_idle(monitoring):
    stage = monitoring.idle_stage
    rows = [r for i in range(monitoring.idle_runs) for r in _success(stage, 1 + i, run=f"r{i}", input_rows=0)]
    assert _status(monitoring, ops.StagePolicy(stage), rows) == ops.IDLE
    rows += _success(stage, 0.5, run="fresh", input_rows=5)
    assert _status(monitoring, ops.StagePolicy(stage), rows) == ops.HEALTHY
