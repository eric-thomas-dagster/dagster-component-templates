import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

ROOT = Path(__file__).parents[1]
NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_policy", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


# ── policy evaluation ───────────────────────────────────────────────────────

def test_evaluate_reasons_in_order_and_fail_closed(mod):
    policy = mod.ContextPolicy(scope="account", sensitivity="confidential", access_tags=("support",), max_age_hours=48)
    ok = mod.CallerContext(clearance="confidential", access_tags=("support",), scope_value="acme", now=NOW)
    fresh = {"account": "acme", "updated_at": (NOW - timedelta(hours=1)).isoformat()}
    assert policy.evaluate(fresh, ok) is None
    assert policy.evaluate({**fresh, "account": "globex"}, ok) == "out_of_scope"
    assert policy.evaluate(fresh, mod.CallerContext(clearance="confidential", access_tags=("support",), now=NOW)) == "out_of_scope"  # no scope_value: closed
    assert policy.evaluate(fresh, mod.CallerContext(clearance="confidential", access_tags=("sales",), scope_value="acme", now=NOW)) == "policy"
    assert policy.evaluate(fresh, mod.CallerContext(clearance="internal", access_tags=("support",), scope_value="acme", now=NOW)) == "policy"
    assert policy.evaluate({**fresh, "sensitivity": "pii"}, ok) == "pii"
    assert policy.evaluate({**fresh, "sensitivity": "pii"}, mod.CallerContext(**{**ok.__dict__, "allow_pii": True})) is None
    assert policy.evaluate({**fresh, "updated_at": (NOW - timedelta(hours=72)).isoformat()}, ok) == "stale"


def test_valid_until_date_is_inclusive_through_end_of_day(mod):
    policy = mod.ContextPolicy()
    caller = mod.CallerContext(now=datetime(2026, 10, 20, 23, 0, tzinfo=timezone.utc))
    assert policy.evaluate({"valid_until": "2026-10-20"}, caller) is None  # still valid that day
    assert policy.evaluate({"valid_until": "2026-10-19"}, caller) == "stale"
    assert policy.evaluate({"valid_until": "2026-10-20T10:00:00+00:00"}, caller) == "stale"  # exact instants are exact


def test_apply_budget_is_greedy_and_excluded_feeds_record_retrieval(mod, tmp_path):
    policy = mod.ContextPolicy(token_budget=10, chars_per_token=1)
    caller = mod.CallerContext(now=NOW)
    cands = [
        {"item_id": "a", "text": "x" * 6, "source_asset_key": "docs", "source_version": "v1"},
        {"item_id": "b", "text": "x" * 6, "source_asset_key": "docs", "source_version": "v1"},  # 12 > 10
        {"item_id": "c", "text": "x" * 4, "source_asset_key": "docs", "source_version": "v1"},  # still fits
        {"item_id": "d", "text": "x", "metadata": {"valid_until": "2020-01-01"}},
    ]
    kept, excluded = policy.apply(cands, caller)
    assert [c["item_id"] for c in kept] == ["a", "c"]
    assert [(e["item_id"], e["reason"]) for e in excluded] == [("b", "over_budget"), ("d", "stale")]
    store = mod.ContextReceiptStore(str(tmp_path / "p.db"))
    rid = store.record_retrieval(
        context_name="c", items=[{"item_id": c["item_id"], "source_asset_key": "docs"} for c in kept], excluded=excluded)
    assert {e["item_id"]: e["reason"] for e in store.get_receipt(rid)["excluded"]} == {"b": "over_budget", "d": "stale"}


def test_policy_validation_and_dict_round_trip(mod):
    with pytest.raises(ValueError, match="sensitivity"):
        mod.ContextPolicy(sensitivity="secret")
    with pytest.raises(ValueError, match="token_budget"):
        mod.ContextPolicy(token_budget=0)
    with pytest.raises(ValueError, match="clearance"):
        mod.CallerContext(clearance="root")
    with pytest.raises(ValueError, match="unknown context policy"):
        mod.ContextPolicy.from_dict({"sensitivity": "internal", "bogus": 1})
    p = mod.ContextPolicy(scope="account", access_tags=("a", "b"), token_budget=100, retention_days=30)
    assert mod.ContextPolicy.from_dict(p.to_dict()) == p


# ── @context_asset on real Dagster definitions ──────────────────────────────

DAILY = dg.DailyPartitionsDefinition("2026-10-01")


def _bundle(rows):
    return pd.DataFrame(rows, columns=["chunk_id", "chunk_text", "account"])


def _defs(mod, rows_fn, **kw):
    @dg.asset
    def upstream():
        return 1

    @mod.context_asset(scope="account", sensitivity="confidential", token_budget=4000, max_age_hours=24, **kw)
    @dg.asset(name="account_context", group_name="ctx", owners=["team@example.com"], tags={"team": "support"},
              ins={"up": dg.AssetIn("upstream")})
    def account_context(context: dg.AssetExecutionContext, up):
        return rows_fn()

    return dg.Definitions(assets=[upstream, account_context]), account_context


def test_decorator_records_policy_and_tags_and_preserves_asset_attributes(mod):
    defs, asset_def = _defs(mod, lambda: _bundle([("c1", "text", "acme")]))
    spec = asset_def.get_asset_spec(dg.AssetKey("account_context"))
    assert spec.group_name == "ctx" and list(spec.owners) == ["team@example.com"]
    assert spec.tags["team"] == "support" and spec.tags["context/sensitivity"] == "confidential"
    assert spec.tags["context/scope"] == "account"
    assert "context" in spec.kinds
    assert dg.AssetKey("upstream") in asset_def.keys_by_input_name.values()  # lineage preserved
    policies = mod.context_policies(defs)
    assert list(policies) == ["account_context"]
    assert policies["account_context"] == mod.ContextPolicy(
        scope="account", sensitivity="confidential", token_budget=4000, max_age_hours=24)


def test_output_contract_validates_and_emits_content_derived_version(mod):
    state = {"rows": [("c1", "alpha", "acme"), ("c2", "beta", "acme")]}
    defs, _ = _defs(mod, lambda: _bundle(state["rows"]))
    instance = dg.DagsterInstance.ephemeral()

    def version():
        return instance.get_latest_data_version_record(dg.AssetKey("account_context")).asset_materialization.tags["dagster/data_version"]

    assert dg.materialize(list(defs.assets), instance=instance).success
    v1 = version()
    dg.materialize(list(defs.assets), instance=instance)
    assert version() == v1  # no-op rebuild: same version (Dagster's default would differ)
    state["rows"] = [("c1", "alpha", "acme"), ("c2", "beta CHANGED", "acme")]
    dg.materialize(list(defs.assets), instance=instance)
    assert version() != v1
    event = dg.materialize(list(defs.assets), instance=instance).asset_materializations_for_node("account_context")[0]
    assert event.metadata["chunks"].value == 2 and event.metadata["context_version"].value == version()


@pytest.mark.parametrize("rows,match", [
    ([("c1", "a", "x"), ("c1", "b", "x")], "unique"),
    ([("c1", "  ", "x")], "text|chunk_text"),
    ([(None, "a", "x")], "null/empty"),
])
def test_output_contract_rejects_bad_bundles(mod, rows, match):
    defs, _ = _defs(mod, lambda: _bundle(rows))
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


def test_missing_column_and_non_dataframe_results(mod):
    defs, _ = _defs(mod, lambda: pd.DataFrame({"chunk_id": ["c1"], "other": ["x"]}))
    assert not dg.materialize(list(defs.assets), raise_on_error=False).success
    defs2, _ = _defs(mod, lambda: {"not": "a frame"})
    assert dg.materialize(list(defs2.assets)).success  # passthrough: nothing to validate


def test_custom_column_names_and_partitioned_asset(mod):
    @mod.context_asset(id_column="id", text_column="body")
    @dg.asset(partitions_def=DAILY, name="parted")
    def parted():
        return pd.DataFrame({"id": ["a"], "body": ["text"]})

    instance = dg.DagsterInstance.ephemeral()
    assert dg.materialize([parted], partition_key="2026-10-05", instance=instance).success
    assert parted.partitions_def == DAILY


def test_decorator_must_follow_dg_asset(mod):
    with pytest.raises(TypeError, match="after @dg.asset"):
        mod.context_asset()(lambda: None)


def test_decorator_keeps_both_function_inputs_and_lineage_only_deps(mod):
    @dg.asset(name="source_a")
    def source_a():
        return 1

    @dg.asset(name="source_b")
    def source_b():
        return 2

    @mod.context_asset()
    @dg.asset(name="mixed", ins={"a": dg.AssetIn("source_a")}, deps=[source_b])
    def mixed(a):
        return _bundle([("c1", f"text {a}", "acme")])

    spec = mixed.get_asset_spec(dg.AssetKey("mixed"))
    assert {d.asset_key.to_user_string() for d in spec.deps} == {"source_a", "source_b"}
    result = dg.materialize([source_a, source_b, mixed])
    assert result.success and result.output_for_node("mixed")["chunk_text"].iloc[0] == "text 1"
