"""A feed file can opt in to partitions; the orchestrator then partitions that feed's asset, resolves {partition_*} tokens
through its steps/sinks/variables, and schedules the latest completed partition."""
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import pytest

ROOT = Path(__file__).resolve().parent.parent
FOLDER = ROOT / "assets/transforms/pyspark_feed_orchestrator"

DAILY_FEED = {
    "name": "daily_events",
    "schedule": "0 6 * * *",
    "partitions": {"type": "daily", "start": "2026-01-01"},
    "variables": {"day": "{partition_key}"},
    "steps": [{"id": "events", "source": {"kind": "parquet", "path": "lake/events/dt={partition_date}"},
               "operations": [{"op": "filter", "predicate": "ts >= '{partition_start}' AND ts < '{partition_end}'"}]}],
    "sinks": [{"from": "events", "asset_key": "daily_events_out", "kind": "parquet", "path": "lake/out/dt={partition_date}", "mode": "overwrite"}],
}
PLAIN_FEED = {
    "name": "static_lookup",
    "steps": [{"id": "lk", "source": {"kind": "parquet", "path": "lake/lookup"}, "operations": []}],
    "sinks": [{"from": "lk", "kind": "parquet", "path": "lake/lookup_out", "mode": "overwrite"}],
}


def _load(monkeypatch):
    spec = importlib.util.spec_from_file_location("orch", FOLDER / "component.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    return mod


def _defs(monkeypatch, tmp_path, *feeds):
    mod = _load(monkeypatch)
    tmp_path = tmp_path / ("-".join(f["name"] for f in feeds))  # one folder per call so feeds never leak between loads
    tmp_path.mkdir(exist_ok=True)
    for f in feeds:
        (tmp_path / f"{f['name']}.json").write_text(json.dumps(f))
    return mod, mod.PySparkFeedOrchestratorComponent(feeds_dir=str(tmp_path)).build_defs(None)


def test_only_the_feed_that_opts_in_is_partitioned(monkeypatch, tmp_path):
    _, defs = _defs(monkeypatch, tmp_path, DAILY_FEED, PLAIN_FEED)
    partitioned = [a for a in defs.assets if hasattr(a, "node_def") and a.partitions_def is not None]
    plain = [a for a in defs.assets if hasattr(a, "node_def") and a.partitions_def is None]
    assert len(partitioned) == 1 and isinstance(partitioned[0].partitions_def, dg.DailyPartitionsDefinition)
    assert len(plain) == 1


def test_a_partitioned_run_resolves_tokens_in_that_feeds_config_only(monkeypatch, tmp_path):
    mod, defs = _defs(monkeypatch, tmp_path, DAILY_FEED, PLAIN_FEED)
    seen = []
    monkeypatch.setattr(mod, "_execute", lambda context, sc, an, steps, sinks, ua, ups, **kw: seen.append((steps, sinks, kw["variables"])))
    asset = next(a for a in defs.assets if hasattr(a, "node_def") and a.partitions_def is not None)
    assert dg.materialize([asset], partition_key="2026-01-03", raise_on_error=False).success
    steps, sinks, variables = seen[-1]
    text = repr((steps, sinks))
    assert "lake/events/dt=2026-01-03" in text and "lake/out/dt=2026-01-03" in text
    assert "ts >= '2026-01-03T00:00:00' AND ts < '2026-01-04T00:00:00'" in text
    assert variables == {"day": "2026-01-03"}
    plain = next(a for a in defs.assets if hasattr(a, "node_def") and a.partitions_def is None)
    assert dg.materialize([plain], raise_on_error=False).success
    assert "lake/lookup" in repr(seen[-1][0])


def test_a_scheduled_partitioned_feed_runs_the_latest_completed_partition(monkeypatch, tmp_path):
    _, defs = _defs(monkeypatch, tmp_path, DAILY_FEED)
    schedule = next(s for s in defs.schedules if s.name == "daily_events_schedule")
    result = schedule(dg.build_schedule_context(scheduled_execution_time=datetime(2026, 1, 5, 6, 0, tzinfo=timezone.utc)))
    assert result.partition_key == "2026-01-04"


def test_an_unscheduled_or_unpartitioned_feed_keeps_the_plain_schedule(monkeypatch, tmp_path):
    scheduled_plain = {**PLAIN_FEED, "name": "nightly_lookup", "schedule": "0 1 * * *"}
    _, defs = _defs(monkeypatch, tmp_path, scheduled_plain)
    sched = next(s for s in defs.schedules if s.name == "nightly_lookup_schedule")
    assert sched.cron_schedule == "0 1 * * *"


def test_misconfigured_partitions_are_refused_at_load(monkeypatch, tmp_path):
    never_uses_a_token = {**DAILY_FEED, "name": "lazy", "variables": {}, "steps": PLAIN_FEED["steps"], "sinks": PLAIN_FEED["sinks"]}
    with pytest.raises(ValueError, match="never uses a"):
        _defs(monkeypatch, tmp_path, never_uses_a_token)
    static_scheduled = {**DAILY_FEED, "name": "st", "partitions": {"type": "static", "values": "a,b"}}
    with pytest.raises(ValueError, match="time partition"):
        _defs(monkeypatch, tmp_path, static_scheduled)
    unnamed = {**DAILY_FEED, "name": "unnamed", "sinks": [{"from": "events", "kind": "parquet", "path": "lake/out/dt={partition_date}", "mode": "overwrite"}]}
    with pytest.raises(ValueError, match="explicit"):
        _defs(monkeypatch, tmp_path, unnamed)
    typo = {**DAILY_FEED, "name": "typo", "partitions": {"type": "daily", "start": "2026-01-01", "strat": "x"}}
    with pytest.raises(ValueError, match="unknown partitions keys"):
        _defs(monkeypatch, tmp_path, typo)
