"""Tests for LineageToCastordocComponent.

Covers: the pure `_transform` shaping, `_resolve_table_id`'s exact
case-insensitive name match (Castor's `getTables(nameContains:)` is a
server-side substring search), `_push`'s enrichment calls
(updateTableDescriptions / attachTags / upsertDataQualities /
upsertLineages) gated correctly on which Dagster assets actually resolve
to an existing Castor table, the `Token` (not Bearer) auth header, and a
full `dg.materialize()` of the component's own sink asset including the
payload-hash change-detection skip.

Mocks only the network boundary (`requests.post`, as the real `requests`
module object imported into the component module -- see
`conftest.load_component_module`) -- everything else runs for real.
"""
import os

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _op_from_url(url):
    return url.split("op=", 1)[1]


def _sample_payload():
    return {
        "source_system": {"platform": "dagster", "dagster_ui_url": ""},
        "sync_metadata": {"payload_hash": "abc123", "total_nodes": 2, "total_edges": 1},
        "nodes": [
            {
                "asset_key": ["staging", "orders"],
                "asset_key_string": "staging/orders",
                "group": "staging",
                "kinds": ["sql"],
                "description": "Raw orders",
                "metadata": {},
                "freshness_policy": None,
            },
            {
                "asset_key": ["ml_model"],
                "asset_key_string": "ml_model",
                "group": "ml",
                "kinds": ["python"],
                "description": "Pure python model, no warehouse table",
                "metadata": {},
                "freshness_policy": None,
            },
        ],
        "edges": [{"upstream": "staging/orders", "downstream": "ml_model"}],
    }


# ── _transform (pure) ──────────────────────────────────────────────────────

def test_transform_uses_last_asset_key_segment_as_match_name(mod):
    transformed = mod._transform(_sample_payload())
    assert transformed["nodes"][0]["match_name"] == "orders"
    assert transformed["nodes"][1]["match_name"] == "ml_model"


def test_transform_passes_through_edges_unchanged(mod):
    transformed = mod._transform(_sample_payload())
    assert transformed["edges"] == [{"upstream": "staging/orders", "downstream": "ml_model"}]


def test_transform_does_not_hit_network(mod):
    # No requests.* monkeypatch here on purpose -- a real network call would error.
    mod._transform(_sample_payload())


# ── _resolve_table_id ──────────────────────────────────────────────────────

def test_resolve_table_id_filters_substring_decoy_to_exact_match(mod):
    def fake_post(url, headers=None, json=None, timeout=None):
        assert _op_from_url(url) == "getTables"
        assert headers["Authorization"] == "Token sekret"
        assert json["variables"]["scope"] == {"nameContains": "orders"}
        return FakeResponse({
            "data": {"getTables": {"data": [
                {"id": "t-decoy", "name": "stg_orders"},
                {"id": "t-real", "name": "Orders"},
            ]}}
        })

    mod.requests.post = fake_post
    table_id = mod._resolve_table_id("https://api.castordoc.com/public/graphql", {"Authorization": "Token sekret"}, "orders")
    assert table_id == "t-real"


def test_resolve_table_id_returns_none_when_no_exact_match(mod):
    def fake_post(url, headers=None, json=None, timeout=None):
        return FakeResponse({"data": {"getTables": {"data": [{"id": "t-decoy", "name": "stg_orders"}]}}})

    mod.requests.post = fake_post
    assert mod._resolve_table_id("https://x", {}, "orders") is None


def test_resolve_table_id_raises_on_graphql_errors(mod):
    def fake_post(url, headers=None, json=None, timeout=None):
        return FakeResponse({"errors": [{"message": "boom"}]})

    mod.requests.post = fake_post
    with pytest.raises(RuntimeError):
        mod._resolve_table_id("https://x", {}, "orders")


# ── _push ───────────────────────────────────────────────────────────────────

def _fake_tables():
    # keyed by match_name (last asset-key segment), not the full asset_key_string
    return {"orders": "t-orders"}  # "ml_model" deliberately absent -> no Castor table


def _dispatch_fake_post(calls, tables_by_match_name):
    def fake_post(url, headers=None, json=None, timeout=None):
        op = _op_from_url(url)
        calls.append((op, json["variables"]))
        if op == "getTables":
            name = json["variables"]["scope"]["nameContains"]
            table_id = tables_by_match_name.get(name)
            rows = [{"id": table_id, "name": name}] if table_id else []
            return FakeResponse({"data": {"getTables": {"data": rows}}})
        if op == "updateTableDescriptions":
            return FakeResponse({"data": {"updateTableDescriptions": [{"id": d["id"]} for d in json["variables"]["data"]]}})
        if op == "attachTags":
            return FakeResponse({"data": {"attachTags": True}})
        if op == "upsertDataQualities":
            return FakeResponse({"data": {"upsertDataQualities": [{"id": "qc-1"}]}})
        if op == "upsertLineages":
            return FakeResponse({"data": {"upsertLineages": [{"id": "l-1"}]}})
        raise AssertionError(f"unexpected op {op}")
    return fake_post


def test_push_raises_without_token_env(mod):
    os.environ.pop("CASTORDOC_MISSING_TOKEN", None)
    with pytest.raises(RuntimeError, match="CASTORDOC_MISSING_TOKEN"):
        mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_MISSING_TOKEN")


class _FakeLog:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass


def test_push_uses_token_auth_header_not_bearer(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        assert headers["Authorization"] == "Token sekret"
        calls.append(url)
        return FakeResponse({"data": {_op_from_url(url): {"data": []}}})

    mod.requests.post = fake_post
    mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN", push_lineage_edges=False)
    assert calls  # at least the getTables lookups happened


def test_push_pushes_descriptions_and_tags_only_for_matched_assets(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN")

    assert result["matched"] == 1
    assert result["unmatched"] == 1
    assert result["descriptions_pushed"] == 1  # only staging/orders matched

    ops_seen = [c[0] for c in calls]
    assert "updateTableDescriptions" in ops_seen
    assert "attachTags" in ops_seen


def test_push_skips_lineage_edge_with_unmatched_endpoint(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN", push_lineage_edges=True)

    # staging/orders -> ml_model edge has an unmatched downstream (ml_model), so it must be skipped
    assert result["lineage_edges_pushed"] == 0
    assert result["lineage_edges_skipped"] == 1
    ops_seen = [c[0] for c in calls]
    assert "upsertLineages" not in ops_seen  # nothing to push, mutation never called


def test_push_lineage_edges_pushed_when_both_endpoints_matched(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    both_matched = {"orders": "t-orders", "ml_model": "t-ml"}
    mod.requests.post = _dispatch_fake_post(calls, both_matched)

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN", push_lineage_edges=True)

    assert result["lineage_edges_pushed"] == 1
    assert result["lineage_edges_skipped"] == 0
    ops_seen = [c[0] for c in calls]
    assert "upsertLineages" in ops_seen
    lineage_call = next(v for op, v in calls if op == "upsertLineages")
    assert lineage_call["data"] == [{"parentTableId": "t-orders", "childTableId": "t-ml"}]


def test_push_lineage_edges_disabled_never_calls_upsert_lineages(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    both_matched = {"orders": "t-orders", "ml_model": "t-ml"}
    mod.requests.post = _dispatch_fake_post(calls, both_matched)

    mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN", push_lineage_edges=False)
    ops_seen = [c[0] for c in calls]
    assert "upsertLineages" not in ops_seen


def test_push_data_quality_off_by_default(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN")
    assert result["quality_records_pushed"] == 0
    ops_seen = [c[0] for c in calls]
    assert "upsertDataQualities" not in ops_seen


def test_push_data_quality_enabled_pushes_one_record_per_matched_table(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN", push_data_quality=True)
    assert result["quality_records_pushed"] == 1  # only staging/orders matched
    quality_calls = [v for op, v in calls if op == "upsertDataQualities"]
    assert len(quality_calls) == 1
    assert quality_calls[0]["data"]["tableId"] == "t-orders"
    assert quality_calls[0]["data"]["qualityChecks"][0]["status"] == "SUCCESS"


def test_push_tags_disabled_skips_attach_tags_call(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_TEST_TOKEN", push_tags=False)
    ops_seen = [c[0] for c in calls]
    assert "attachTags" not in ops_seen


# ── Full asset materialize (real dg.materialize) ────────────────────────────

def _component(mod, **overrides):
    kwargs = dict(
        asset_name="lineage_to_castordoc",
        upstream_asset_key="lineage_graph",
        catalog_url="https://api.castordoc.com/public/graphql",
        api_token_env="CASTORDOC_TEST_TOKEN",
    )
    kwargs.update(overrides)
    return mod.LineageToCastordocComponent(**kwargs)


def test_sink_asset_pushes_and_reports_match_counts(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    component = _component(mod, push_lineage_edges=False)
    defs = component.build_defs(None)
    sink_asset = next(iter(defs.assets))

    @dg.asset(key=dg.AssetKey("lineage_graph"))
    def _fake_upstream():
        return _sample_payload()

    result = dg.materialize([_fake_upstream, sink_asset], raise_on_error=False)
    assert result.success
    mats = result.asset_materializations_for_node("lineage_to_castordoc")
    md = mats[0].metadata
    assert md["assets_matched"].value == 1
    assert md["assets_unmatched"].value == 1


def test_sink_asset_skips_push_when_hash_unchanged(mod):
    os.environ["CASTORDOC_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, _fake_tables())

    component = _component(mod, only_push_on_change=True, push_lineage_edges=False)
    defs = component.build_defs(None)
    sink_asset = next(iter(defs.assets))

    @dg.asset(key=dg.AssetKey("lineage_graph"))
    def _fake_upstream():
        return _sample_payload()

    instance = dg.DagsterInstance.ephemeral()
    result1 = dg.materialize([_fake_upstream, sink_asset], instance=instance, raise_on_error=False)
    assert result1.success
    assert len(calls) > 0

    calls.clear()
    result2 = dg.materialize([_fake_upstream, sink_asset], instance=instance, raise_on_error=False)
    assert result2.success
    mats = result2.asset_materializations_for_node("lineage_to_castordoc")
    assert mats[0].metadata["skipped"].value is True
    assert calls == []  # no Castor network calls made on the unchanged-hash run
