import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dagster as dg
import pytest

ROOT = Path(__file__).parents[1]
T0 = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_implicit", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


class Clock:
    def __init__(self, mod, monkeypatch):
        self.now = T0
        monkeypatch.setattr(mod.ContextReceiptStore, "_timestamp", staticmethod(lambda: self.now.isoformat()))

    def at(self, seconds):
        self.now = T0 + timedelta(seconds=seconds)


def _retrieve(store, clock, t, *, session="s1", caller="agent", query="q1", item="a"):
    clock.at(t)
    return store.record_retrieval(
        context_name="c", caller_id=caller, mcp_session_id=session, query_hash=query,
        items=[{"item_id": item, "source_asset_key": "docs", "source_version": "v1"}])


def _outcomes(store, rid):
    return [e["payload"] for e in store.get_receipt(rid)["events"] if e["event_type"] == "outcome"]


def test_reformulation_marks_the_earlier_receipt_and_is_idempotent(mod, tmp_path, monkeypatch):
    store, clock = mod.ContextReceiptStore(str(tmp_path / "i.db")), None
    clock = Clock(mod, monkeypatch)
    first = _retrieve(store, clock, 0, query="refund policy", item="a")
    second = _retrieve(store, clock, 30, query="how do refunds work", item="b")
    assert store.derive_implicit_outcomes(window_seconds=120) == 1
    out = _outcomes(store, first)
    assert out == [{"signal": "reformulated", "value": -0.2, "source": "implicit",
                    "outcome_ref": f"implicit:reformulated:{second}"}]
    assert _outcomes(store, second) == []  # the follow-up is not blamed
    assert store.derive_implicit_outcomes(window_seconds=120) == 0  # idempotent
    store.refresh_item_scores(prior_strength=0)
    assert store.get_item_scores(["a"])["a"]["prior"] < 0  # the failed retrieval's chunk is demoted


@pytest.mark.parametrize("second", [
    dict(t=500, session="s1", caller="agent", query="q2"),   # too late: not a retry
    dict(t=30, session="s1", caller="agent", query="q1"),    # identical query: not a reformulation
    dict(t=30, session="s2", caller="agent", query="q2"),    # different session
    dict(t=30, session="s1", caller="other", query="q2"),    # different caller
])
def test_no_signal_when_it_is_not_a_reformulation(mod, tmp_path, monkeypatch, second):
    store = mod.ContextReceiptStore(str(tmp_path / "n.db"))
    clock = Clock(mod, monkeypatch)
    first = _retrieve(store, clock, 0)
    _retrieve(store, clock, second.pop("t"), **second)
    assert store.derive_implicit_outcomes(window_seconds=120) == 0
    assert _outcomes(store, first) == []


def test_receipts_without_session_or_query_hash_are_ignored(mod, tmp_path, monkeypatch):
    store = mod.ContextReceiptStore(str(tmp_path / "x.db"))
    clock = Clock(mod, monkeypatch)
    clock.at(0)
    store.record_retrieval(context_name="c", items=[])
    clock.at(5)
    store.record_retrieval(context_name="c", items=[])
    assert store.derive_implicit_outcomes() == 0


def test_sensor_derives_implicit_signals_only_when_enabled(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()

    @dg.asset(name="docs")
    def docs():
        return dg.MaterializeResult(data_version=dg.DataVersion("v1"))

    dg.materialize([docs], instance=instance)

    def run(enabled):
        store = mod.ContextReceiptStore(str(tmp_path / f"s{enabled}.db"))
        for query, item in (("one", "a"), ("two", "b")):  # real timestamps: milliseconds apart, same session
            store.record_retrieval(
                context_name="c", caller_id="agent", mcp_session_id="s1", query_hash=query,
                items=[{"item_id": item, "source_asset_key": "docs", "source_version": "v1"}])
        sensor = mod.build_usage_sensor(
            store, name="s", interval_seconds=60, rebuild_asset_keys=[], stale_reads_threshold=99,
            prior_strength=1.0, implicit_signals=enabled)
        return sensor(dg.build_sensor_context(instance=instance))

    on = run(True)
    assert on.asset_events[0].metadata["context_receipts/outcomes"].value == 1
    assert on.asset_events[0].metadata["context_receipts/mean_outcome"].value == -0.2
    assert run(False).asset_events[0].metadata["context_receipts/outcomes"].value == 0


def test_component_exposes_the_switch(mod, tmp_path):
    comp = mod.ContextReceiptStoreComponent(
        database_path=str(tmp_path / "c.db"), usage_sensor_enabled=True, implicit_signals_enabled=True,
        reformulation_window_seconds=30)
    assert [s.name for s in comp.build_defs(None).sensors] == ["context_receipts_usage_sensor"]
