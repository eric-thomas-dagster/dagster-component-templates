import importlib.util
import json
import random
from pathlib import Path

import dagster as dg
import pytest


def _load():
    path = Path(__file__).parents[1] / "component.py"
    spec = importlib.util.spec_from_file_location("ctx_receipt_feedback", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


@pytest.fixture()
def store(mod, tmp_path):
    return mod.ContextReceiptStore(str(tmp_path / "r.sqlite3"))


def _receipt(store, items, **kw):
    return store.record_retrieval(
        context_name="c", items=[{"item_id": i, "source_asset_key": "docs", "source_version": "v1"} for i in items], **kw
    )


def test_client_side_ids_are_time_sortable_and_idempotent(mod, store):
    ids = []
    for _ in range(3):
        ids.append(mod.new_receipt_id())
    assert all(len(i) == 26 for i in ids)
    rid = store.record_retrieval(context_name="c", items=[], receipt_id=ids[0])
    assert rid == ids[0]
    with pytest.raises(mod.DuplicateReceiptError):
        store.record_retrieval(context_name="c", items=[], receipt_id=ids[0])


def test_outcome_validation_and_supersede(mod, store):
    rid = _receipt(store, ["a"])
    with pytest.raises(ValueError, match="explicit value"):
        store.record_outcome(rid, signal="custom")
    with pytest.raises(ValueError, match=r"\[-1, 1\]"):
        store.record_outcome(rid, signal="custom", value=3)
    with pytest.raises(KeyError):
        store.record_outcome("nope", signal="thumbs_up")
    # user flips thumbs: same outcome_ref -> counted once, as the latest value
    store.record_outcome(rid, signal="thumbs_up", outcome_ref="user-1")
    store.record_outcome(rid, signal="thumbs_down", outcome_ref="user-1")
    store.refresh_item_scores(prior_strength=0)
    assert store.get_item_scores(["a"])["a"]["prior"] == pytest.approx(-1.0)


def test_scores_weight_by_evidence_and_shrink(mod, store):
    good = _receipt(store, ["x", "y"])
    bad = _receipt(store, ["x", "z"])
    store.record_outcome(good, signal="thumbs_up")
    store.record_outcome(bad, signal="thumbs_down")
    # only z was verified as used in the bad answer -> z takes the blame, x only weakly
    store.add_evidence(bad, level="verified", details={"item_ids": ["z"]})
    store.refresh_item_scores(prior_strength=0)
    s = store.get_item_scores(["x", "y", "z"])
    assert s["y"]["prior"] == pytest.approx(1.0)
    assert s["z"]["prior"] == pytest.approx(-1.0)
    # x: +1*0.5 (retrieved-only) and -1*0.5 -> 0.0
    assert s["x"]["prior"] == pytest.approx(0.0)
    assert s["x"]["retrievals"] == 2
    store.refresh_item_scores(prior_strength=5)
    assert abs(store.get_item_scores(["z"])["z"]["prior"]) < 0.3  # little evidence -> shrunk


def test_rerank_prefers_learned_priors_and_reserves_exploration(mod):
    cands = [{"item_id": c, "score": s} for c, s in [("a", 0.9), ("b", 0.8), ("c", 0.7), ("d", 0.1)]]
    scores = {"b": {"prior": 1.0}, "a": {"prior": -1.0}}
    out = mod.rerank(cands, scores, prior_weight=0.5, explore_fraction=0)
    assert [c["item_id"] for c in out] == ["b", "c", "a", "d"]  # b 0.8+.5, c .7, a .9-.5, d .1
    assert all(c["selection"] == "ranked" for c in out)
    forced = mod.rerank(cands, scores, top_k=2, explore_fraction=1, rng=random.Random(0))
    assert len(forced) == 2 and forced[-1]["selection"] == "explore"
    assert forced[-1]["item_id"] in {"c", "d", "a"} - {forced[0]["item_id"]}
    never = mod.rerank(cands, scores, top_k=2, explore_fraction=0)
    assert all(c["selection"] == "ranked" for c in never)
    with pytest.raises(ValueError):
        mod.rerank(cands, scores, explore_fraction=2)


def test_exploration_flag_persists_on_receipt(mod, store):
    rid = store.record_retrieval(
        context_name="c", items=[{"item_id": "a", "selection": "explore"}, {"item_id": "b"}]
    )
    sel = {i["item_id"]: i["selection"] for i in store.get_receipt(rid)["items"]}
    assert sel == {"a": "explore", "b": "ranked"}
    with pytest.raises(ValueError, match="selection"):
        store.record_retrieval(context_name="c", items=[{"item_id": "a", "selection": "x"}])


def test_async_writer_returns_immediately_orders_ops_and_dead_letters(mod, store):
    w = mod.AsyncReceiptWriter(store, max_retries=1, backoff_seconds=0.01)
    rid = w.record_retrieval(context_name="c", items=[{"item_id": "a"}])
    w.attach_output(rid, output_id="o", model="m", output_hash="h")  # references a not-yet-durable row
    w.record_outcome(rid, signal="thumbs_up")
    w.record_outcome("missing", signal="thumbs_up")  # can never succeed
    w.flush()
    receipt = store.get_receipt(rid)
    assert [e["event_type"] for e in receipt["events"]] == ["output_attached", "outcome"]
    assert len(w.dead_letters) == 1 and w.dead_letters[0][0] == "record_outcome"


def test_async_writer_retries_transient_errors(mod, store, monkeypatch):
    calls = {"n": 0}
    real = store.record_retrieval

    def flaky(**kw):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("db hiccup")
        return real(**kw)

    monkeypatch.setattr(store, "record_retrieval", flaky)
    w = mod.AsyncReceiptWriter(store, max_retries=5, backoff_seconds=0.001)
    rid = w.record_retrieval(context_name="c", items=[])
    w.flush()
    assert not w.dead_letters and store.get_receipt(rid) is not None and calls["n"] == 3


def _usage_setup(mod, tmp_path, **sensor_kw):
    store = mod.ContextReceiptStore(str(tmp_path / "s.sqlite3"))

    @dg.asset(name="docs")
    def docs():
        return dg.MaterializeResult(data_version=dg.DataVersion("v2"))

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([docs], instance=instance)
    sensor = mod.build_usage_sensor(
        store, name="s", interval_seconds=60, rebuild_asset_keys=sensor_kw.get("rebuild", []),
        stale_reads_threshold=sensor_kw.get("threshold", 3), prior_strength=1.0,
    )
    return store, instance, sensor


def _stale_receipt(store, version):
    return store.record_retrieval(
        context_name="c",
        items=[{"item_id": f"i-{version}", "source_asset_key": "docs", "source_version": version}],
        excluded=[{"item_id": "e", "reason": "stale", "source_asset_key": "docs"}],
    )


def test_sensor_aggregates_usage_outcomes_and_resumes_from_cursor(mod, tmp_path):
    store, instance, sensor = _usage_setup(mod, tmp_path)
    r1 = _stale_receipt(store, "v1")
    _stale_receipt(store, "v2")
    result = sensor(dg.build_sensor_context(instance=instance))
    assert len(result.asset_events) == 1  # aggregated: one observation, not one per receipt
    md = result.asset_events[0].metadata
    assert md["context_receipts/retrievals"].value == 2
    assert md["context_receipts/stale_reads"].value == 1
    assert md["context_receipts/current_data_version"].value == "v2"
    assert md["context_receipts/excluded_by_reason"].value == {"stale": 2}
    # nothing new -> skip
    again = sensor(dg.build_sensor_context(instance=instance, cursor=result.cursor))
    assert isinstance(again, dg.SkipReason)
    # a late outcome on an old receipt is picked up via the event cursor, with no new receipts
    store.record_outcome(r1, signal="thumbs_down")
    late = sensor(dg.build_sensor_context(instance=instance, cursor=result.cursor))
    lmd = late.asset_events[0].metadata
    assert lmd["context_receipts/outcomes"].value == 1
    assert lmd["context_receipts/mean_outcome"].value == -1.0
    assert "context_receipts/retrievals" in lmd and lmd["context_receipts/retrievals"].value == 0
    # priors were refreshed as a side effect (write-back to the gateway's scoring table)
    assert store.get_item_scores(["i-v1"])["i-v1"]["prior"] < 0


def test_sensor_requests_rebuild_only_for_allowed_assets_past_threshold(mod, tmp_path):
    store, instance, sensor = _usage_setup(mod, tmp_path, rebuild=["docs"], threshold=3)
    for _ in range(2):
        _stale_receipt(store, "v1")
    first = sensor(dg.build_sensor_context(instance=instance))
    assert first.run_requests == []
    _stale_receipt(store, "v1")  # third stale read crosses the threshold (accumulated via cursor)
    second = sensor(dg.build_sensor_context(instance=instance, cursor=first.cursor))
    assert [r.asset_selection for r in second.run_requests] == [[dg.AssetKey("docs")]]
    assert second.run_requests[0].run_key == "context-rebuild:docs:None:v2"
    # not listed in rebuild_asset_keys -> observe only
    store2, inst2, sensor2 = _usage_setup(mod, tmp_path / "x", rebuild=[], threshold=1)
    _stale_receipt(store2, "v1")
    assert sensor2(dg.build_sensor_context(instance=inst2)).run_requests == []


def test_component_builds_sensor_when_enabled(mod, tmp_path):
    comp = mod.ContextReceiptStoreComponent(
        database_path=str(tmp_path / "c.sqlite3"), usage_sensor_enabled=True, rebuild_asset_keys=["docs"]
    )
    defs = comp.build_defs(None)
    assert [s.name for s in defs.sensors] == ["context_receipts_usage_sensor"]
    off = mod.ContextReceiptStoreComponent(database_path=str(tmp_path / "d.sqlite3")).build_defs(None)
    assert not list(off.sensors or [])
