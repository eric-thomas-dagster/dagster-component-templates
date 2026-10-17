import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

ROOT = Path(__file__).parents[1]
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_governance", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


def _bundle():
    return pd.DataFrame({"chunk_id": ["a"], "chunk_text": ["t"]})


# ── downstream consumption of out-of-policy context ─────────────────────────

def _graph(mod, clearance="internal", consumer_tags=(), via_intermediate=False, upstream_policy=None):
    policy = upstream_policy or {"sensitivity": "confidential"}

    @mod.context_asset(**policy)
    @dg.asset(name="tickets_ctx")
    def tickets_ctx():
        return _bundle()

    assets = [tickets_ctx]
    parent = "tickets_ctx"
    if via_intermediate:
        @dg.asset(name="summary", deps=[tickets_ctx])
        def summary(): ...
        assets.append(summary)
        parent = "summary"

    @mod.context_consumer(clearance=clearance, access_tags=consumer_tags)
    @dg.asset(name="public_faq", deps=[parent], group_name="g", owners=["a@b.com"])
    def public_faq(): ...

    @dg.asset(name="unrelated")
    def unrelated(): ...

    assets += [public_faq, unrelated]
    return dg.Definitions(assets=assets)


def test_consumer_cleared_for_the_context_is_allowed(mod):
    assert mod.validate_context_consumption(_graph(mod, clearance="confidential")) == []
    mod.check_context_consumption(_graph(mod, clearance="restricted"))  # does not raise


def test_consumer_below_clearance_is_a_violation_even_through_intermediate_assets(mod):
    for via in (False, True):
        violations = mod.validate_context_consumption(_graph(mod, clearance="internal", via_intermediate=via))
        assert [(v.consumer, v.context_asset) for v in violations] == [("public_faq", "tickets_ctx")]
        assert "confidential" in violations[0].reason and "internal" in violations[0].reason
    with pytest.raises(mod.ContextPolicyViolation, match="public_faq"):
        mod.check_context_consumption(_graph(mod, clearance="internal"))


def test_access_tags_must_be_held_by_the_consumer(mod):
    policy = {"sensitivity": "internal", "access_tags": ["support"]}
    assert mod.validate_context_consumption(_graph(mod, clearance="internal", consumer_tags=("support",), upstream_policy=policy)) == []
    violations = mod.validate_context_consumption(_graph(mod, clearance="internal", consumer_tags=("sales",), upstream_policy=policy))
    assert len(violations) == 1 and "support" in violations[0].reason


def test_only_declared_consumers_are_checked_and_decorator_preserves_the_asset(mod):
    defs = _graph(mod, clearance="confidential")
    graph = defs.resolve_asset_graph()
    spec = [a for a in defs.assets if dg.AssetKey("public_faq") in a.keys][0].get_asset_spec(dg.AssetKey("public_faq"))
    assert spec.group_name == "g" and list(spec.owners) == ["a@b.com"] and spec.tags["context/consumer-clearance"] == "confidential"
    assert dg.AssetKey("tickets_ctx") in [d.asset_key for d in spec.deps]
    with pytest.raises(ValueError, match="clearance"):
        mod.context_consumer(clearance="root")
    with pytest.raises(TypeError, match="after @dg.asset"):
        mod.context_consumer()(lambda: None)
    assert mod.validate_context_consumption(graph) == []


# ── retention ───────────────────────────────────────────────────────────────

class Clock:
    def __init__(self, mod, monkeypatch):
        self.now = T0
        monkeypatch.setattr(mod.ContextReceiptStore, "_timestamp", staticmethod(lambda: self.now.isoformat()))


def _r(store, *assets):
    return store.record_retrieval(
        context_name="c", items=[{"item_id": f"{a}-i", "source_asset_key": a} for a in assets],
        excluded=[{"item_id": "x", "reason": "stale"}] if assets else [])


def test_retention_uses_the_shortest_applicable_period_and_keeps_unretained(mod, tmp_path, monkeypatch):
    store = mod.ContextReceiptStore(str(tmp_path / "ret.db"))
    Clock(mod, monkeypatch)
    only_a, only_b, mixed, none_declared, no_assets = (
        _r(store, "a"), _r(store, "b"), _r(store, "a", "b"), _r(store, "c"), _r(store))
    store.record_outcome(only_a, signal="thumbs_up")
    store.attach_output(mixed, output_id="o", model="m", output_hash="h")
    limits = {"a": 30, "b": 90, "c": None}
    day = lambda n: T0 + timedelta(days=n)
    assert store.enforce_retention(limits, now=day(10)) == {"receipts_deleted": 0}
    assert store.enforce_retention(limits, now=day(31)) == {"receipts_deleted": 2}  # only_a, and mixed (30 beats 90)
    assert store.get_receipt(only_a) is None and store.get_receipt(mixed) is None
    assert store.get_receipt(only_b) is not None
    assert store.enforce_retention(limits, now=day(91)) == {"receipts_deleted": 1}  # only_b
    assert store.get_receipt(none_declared) is not None and store.get_receipt(no_assets) is not None  # kept forever
    with store._connect() as conn:  # nothing orphaned
        for table in ("context_receipt_items", "context_receipt_excluded", "context_receipt_events"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table} WHERE receipt_id IN (?, ?, ?)", (only_a, only_b, mixed)).fetchone()[0] == 0
    assert store.enforce_retention({}, now=day(999)) == {"receipts_deleted": 0}


def test_sensor_enforces_retention_from_declared_policies(mod, tmp_path, monkeypatch):
    @mod.context_asset(retention_days=30)
    @dg.asset(name="short_lived")
    def short_lived():
        return _bundle()

    @mod.context_asset()
    @dg.asset(name="kept")
    def kept():
        return _bundle()

    defs = dg.Definitions(assets=[short_lived, kept])
    store = mod.ContextReceiptStore(str(tmp_path / "s.db"))
    Clock(mod, monkeypatch)
    old, forever = _r(store, "short_lived"), _r(store, "kept")
    monkeypatch.undo()  # sensor runs "now" (months after T0)
    sensor = mod.build_usage_sensor(store, name="s", interval_seconds=60, rebuild_asset_keys=[],
                                    stale_reads_threshold=99, prior_strength=1.0, enforce_retention=True)
    sensor(dg.build_sensor_context(definitions=defs, instance=dg.DagsterInstance.ephemeral()))
    assert store.get_receipt(old) is None and store.get_receipt(forever) is not None
    off = mod.ContextReceiptStore(str(tmp_path / "off.db"))
    kept_id = _r(off, "short_lived")
    sensor2 = mod.build_usage_sensor(off, name="s2", interval_seconds=60, rebuild_asset_keys=[],
                                     stale_reads_threshold=99, prior_strength=1.0)
    sensor2(dg.build_sensor_context(definitions=defs, instance=dg.DagsterInstance.ephemeral()))
    assert off.get_receipt(kept_id) is not None  # opt-in


# ── grounding check and context health ──────────────────────────────────────

@dg.asset(name="docs")
def docs():
    return dg.MaterializeResult(data_version=dg.DataVersion("v2"), metadata={})


def _run_checks(mod, store, **kw):
    checks = mod.build_usage_checks(store, "docs", window_hours=24, max_stale_share=0.1, min_mean_outcome=-0.2,
                                    min_verified_share=0.5, min_checked_receipts=3, **kw)
    result = dg.Definitions(assets=[docs], asset_checks=checks).get_implicit_global_asset_job_def().execute_in_process(
        instance=dg.DagsterInstance.ephemeral())
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def _served(store, version="v2"):
    return store.record_retrieval(context_name="c", items=[
        {"item_id": f"i{version}", "source_asset_key": "docs", "source_version": version}])


def test_grounding_check_reads_claim_counts_a_verifier_reports(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "g.db"))
    assert _run_checks(mod, store)["context_grounding"].passed  # nothing reported: nothing to judge
    for total, bad in ((10, 0), (10, 4)):
        rid = _served(store)
        store.add_evidence(rid, level="verified", details={"claims_total": total, "claims_ungrounded": bad})
    ev = _run_checks(mod, store)["context_grounding"]
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.metadata["ungrounded_rate"].value == 0.2 and ev.metadata["claims"].value == 20
    assert _run_checks(mod, store, max_ungrounded_rate=0.25)["context_grounding"].passed
    # a later report for the same receipt replaces the earlier one
    rid = _served(store)
    store.add_evidence(rid, level="verified", details={"claims_total": 5, "claims_ungrounded": 5})
    store.add_evidence(rid, level="verified", details={"claims_total": 5, "claims_ungrounded": 0})
    assert _run_checks(mod, store)["context_grounding"].metadata["claims"].value == 25


def test_context_health_status_matches_the_checks(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([docs], instance=instance)
    store = mod.ContextReceiptStore(str(tmp_path / "h.db"))
    health = mod.context_health(store, instance, ["docs"])["docs"]
    assert health["status"] == "healthy" and health["failing"] == []
    assert health["current_data_version"] == "v2" and health["current_code_version"]
    for rid in (_served(store), _served(store)):
        store.record_outcome(rid, signal="thumbs_down")
    for _ in range(3):
        store.record_outcome(_served(store), signal="thumbs_down")
    degraded = mod.context_health(store, instance, ["docs"])["docs"]
    assert degraded["status"] == "degraded" and degraded["failing"] == ["context_outcome_quality"]
    for _ in range(4):
        _served(store, "v1")  # stale: current is v2
    bad = mod.context_health(store, instance, ["docs"])["docs"]
    assert bad["status"] == "unhealthy" and "context_staleness" in bad["failing"]
    # same numbers as the executable check
    ev = _run_checks(mod, store)["context_staleness"]
    assert ev.metadata["stale_share"].value == bad["checks"]["context_staleness"]["metadata"]["stale_share"]
    assert mod.context_health(store, instance, ["docs"], thresholds=mod.HealthThresholds(max_stale_share=0.99,
           min_mean_outcome=-1.0))["docs"]["status"] == "healthy"
