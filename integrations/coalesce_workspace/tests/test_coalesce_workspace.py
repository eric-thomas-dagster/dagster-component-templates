"""Tests for CoalesceWorkspaceComponent (formerly CoalesceProjectComponent).

Covers: the rename itself (import sanity), node discovery / dependency
resolution (pre-existing logic, now under the new name), the new
test-failure -> AssetCheckResult surfacing (passing and failing cases,
plus `fail_run_on_test_failure`), the corrected run-status vocabulary
(`completed`/`failed`/`canceled`, not the old `succeeded`/`cancelled`/
`error` that never matched a real Coalesce response), and the optional
Catalog (Castor) metadata enrichment.

Mocks only the network boundary (`requests.post`/`requests.get`, as the
real `requests` module object imported into the component module -- see
`conftest.load_component_module`) -- everything else runs for real,
including a full `dg.materialize()` of the component's own `multi_asset`.
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


# ── Rename sanity ──────────────────────────────────────────────────────────

def test_renamed_class_imports_cleanly(mod):
    assert hasattr(mod, "CoalesceWorkspaceComponent")
    assert not hasattr(mod, "CoalesceProjectComponent")
    assert mod.CoalesceWorkspaceComponent.__name__ == "CoalesceWorkspaceComponent"


def test_coalesce_resource_still_present(mod):
    assert hasattr(mod, "CoalesceResource")


# ── Node discovery / dependency resolution ─────────────────────────────────

def test_build_defs_resolves_dependencies_from_source_node_ids(mod):
    nodes = [
        {"nodeId": "n1", "name": "stg_orders", "sourceNodeIds": [], "type": "stage"},
        {"nodeId": "n2", "name": "fct_orders", "sourceNodeIds": ["n1"], "type": "fact"},
    ]
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="TOK",
        api_base_url="https://x", asset_name_prefix=None, group_name="coalesce",
        poll_interval=0, timeout=5,
    )
    assets_def = next(iter(defs.assets))
    assert assets_def.keys == {dg.AssetKey(["stg_orders"]), dg.AssetKey(["fct_orders"])}
    fct_spec = assets_def.specs_by_key[dg.AssetKey(["fct_orders"])]
    assert list(fct_spec.deps)[0].asset_key == dg.AssetKey(["stg_orders"])
    stg_spec = assets_def.specs_by_key[dg.AssetKey(["stg_orders"])]
    assert stg_spec.deps == []


def test_build_defs_applies_asset_name_prefix(mod):
    nodes = [{"nodeId": "n1", "name": "stg_orders", "sourceNodeIds": [], "type": "stage"}]
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="TOK",
        api_base_url="https://x", asset_name_prefix="coalesce",
        group_name=None, poll_interval=0, timeout=5,
    )
    assets_def = next(iter(defs.assets))
    assert dg.AssetKey(["coalesce", "stg_orders"]) in assets_def.keys


def test_build_defs_applies_assets_by_node_name_overrides(mod):
    nodes = [{"nodeId": "n1", "name": "customer_rollup", "sourceNodeIds": [], "type": "fact"}]
    overrides = {
        "customer_rollup": [
            {"key": "finance/mrr", "description": "MRR"},
            {"key": "finance/arr", "description": "ARR"},
        ]
    }
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="TOK",
        api_base_url="https://x", asset_name_prefix=None, group_name="coalesce",
        poll_interval=0, timeout=5, assets_by_node_name=overrides,
    )
    assets_def = next(iter(defs.assets))
    assert dg.AssetKey(["finance", "mrr"]) in assets_def.keys
    assert dg.AssetKey(["finance", "arr"]) in assets_def.keys


def test_no_nodes_returns_empty_definitions(mod):
    defs = mod._build_coalesce_defs(
        nodes=[], environment_id="env1", api_token_env_var="TOK",
        api_base_url="https://x", asset_name_prefix=None, group_name="coalesce",
        poll_interval=0, timeout=5,
    )
    assert not defs.assets


# ── Test-check emission (Bug 2) ────────────────────────────────────────────

def _single_node_defs(mod, **kwargs):
    nodes = [{"nodeId": "n1", "name": "stg_orders", "sourceNodeIds": [], "type": "stage"}]
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="COALESCE_TEST_TOK",
        api_base_url="https://coalesce.example", asset_name_prefix=None,
        group_name="coalesce", poll_interval=0, timeout=5, **kwargs,
    )
    return next(iter(defs.assets))


def test_check_specs_created_by_default(mod):
    assets_def = _single_node_defs(mod)
    check_keys = set(assets_def.check_keys)
    assert dg.AssetCheckKey(dg.AssetKey(["stg_orders"]), "coalesce_node_tests") in check_keys


def test_check_specs_omitted_when_disabled(mod):
    assets_def = _single_node_defs(mod, emit_test_checks=False)
    assert list(assets_def.check_keys) == []


def _run_materialize(mod, assets_def, *, run_status="completed", has_test_failures=False,
                      token_env_var="COALESCE_TEST_TOK", results_called=None):
    os.environ[token_env_var] = "secret-token"
    calls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(("POST", url))
        if url.endswith("/scheduler/startRun"):
            return FakeResponse({"runCounter": "123"})
        raise AssertionError(f"unexpected POST {url}")

    def fake_get(url, headers=None, params=None, timeout=None):
        calls.append(("GET", url))
        if url.endswith("/scheduler/runStatus"):
            return FakeResponse({"runStatus": run_status})
        if url.endswith("/api/v1/runs/123/results"):
            if results_called is not None:
                results_called.append(True)
            return FakeResponse({"data": [{"nodeID": "n1", "hasTestFailures": has_test_failures}]})
        raise AssertionError(f"unexpected GET {url}")

    mod.requests.post = fake_post
    mod.requests.get = fake_get
    result = dg.materialize([assets_def], raise_on_error=False)
    return result, calls


def test_materialize_surfaces_passing_test_check(mod):
    assets_def = _single_node_defs(mod)
    result, _ = _run_materialize(mod, assets_def, has_test_failures=False)
    assert result.success
    evals = result.get_asset_check_evaluations()
    assert len(evals) == 1
    assert evals[0].passed is True


def test_materialize_surfaces_failing_test_check_without_failing_run(mod):
    """The real Coalesce gating fact this encodes: a node can finish
    `runStatus: completed` while still carrying `hasTestFailures: true`
    (Coalesce's own 'yellow' run state) -- so by default the Dagster run
    still succeeds, with the failure visible only on the check."""
    assets_def = _single_node_defs(mod)  # fail_run_on_test_failure defaults False
    result, _ = _run_materialize(mod, assets_def, has_test_failures=True)
    assert result.success
    evals = result.get_asset_check_evaluations()
    assert len(evals) == 1
    assert evals[0].passed is False


def test_fail_run_on_test_failure_hard_fails_the_run(mod):
    assets_def = _single_node_defs(mod, fail_run_on_test_failure=True)
    result, _ = _run_materialize(mod, assets_def, has_test_failures=True)
    assert not result.success


def test_emit_test_checks_disabled_skips_results_call(mod):
    assets_def = _single_node_defs(mod, emit_test_checks=False)
    results_called = []
    result, _ = _run_materialize(mod, assets_def, results_called=results_called)
    assert result.success
    assert results_called == []  # /results never fetched -- no checks to feed


# ── Run-status vocabulary fix regression ───────────────────────────────────
# The component used to poll for "succeeded" (which Coalesce's API never
# returns -- real terminal values are "completed"/"failed"/"canceled") and
# treat "cancelled"/"error" as failure, missing the real one-L "canceled".

def test_completed_status_breaks_the_poll_loop_and_succeeds(mod):
    assets_def = _single_node_defs(mod)
    result, _ = _run_materialize(mod, assets_def, run_status="completed")
    assert result.success


def test_canceled_one_l_status_is_recognized_as_failure(mod):
    assets_def = _single_node_defs(mod)
    result, _ = _run_materialize(mod, assets_def, run_status="canceled")
    assert not result.success


def test_unrecognized_status_eventually_times_out(mod):
    """A status that never resolves (e.g. stuck "running") must raise once
    `timeout_seconds` elapses, not silently fall through and report
    success -- the pre-fix behavior when polling never found "succeeded"."""
    nodes = [{"nodeId": "n1", "name": "stg_orders", "sourceNodeIds": [], "type": "stage"}]
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="COALESCE_TEST_TOK",
        api_base_url="https://coalesce.example", asset_name_prefix=None,
        group_name="coalesce", poll_interval=0.01, timeout=0.02,
    )
    assets_def = next(iter(defs.assets))
    result, _ = _run_materialize(mod, assets_def, run_status="running")
    assert not result.success


# ── Catalog (Castor) metadata enrichment ───────────────────────────────────

def _catalog_resource(mod, token_env_var="CATALOG_TOK"):
    os.environ[token_env_var] = "catalog-secret"
    return mod.CoalesceResource(
        api_token_env_var="COALESCE_TEST_TOK",
        environment_id="env1",
        catalog_api_token_env_var=token_env_var,
    )


def test_get_catalog_table_metadata_returns_none_without_token(mod):
    resource = mod.CoalesceResource(api_token_env_var="X", environment_id="env1")
    assert resource.get_catalog_table_metadata("orders") is None


def test_get_catalog_table_metadata_picks_exact_case_insensitive_match(mod):
    resource = _catalog_resource(mod)

    def fake_post(url, headers=None, json=None, timeout=None):
        assert "op=getTables" in url
        assert headers["Authorization"] == "Token catalog-secret"
        # nameContains is a substring match server-side -- simulate it
        # returning a decoy alongside the real exact (differently-cased) match.
        return FakeResponse({
            "data": {
                "getTables": {
                    "data": [
                        {"id": "t-decoy", "name": "STG_ORDERS", "descriptionMarkdown": "decoy"},
                        {
                            "id": "t-real", "name": "Orders",
                            "descriptionMarkdown": "Customer orders",
                            "ownerEntities": [{"user": {"fullName": "Jane Doe", "email": "jane@x.com"}}],
                            "tagEntities": [{"tag": {"label": "pii"}}],
                        },
                    ]
                }
            }
        })

    mod.requests.post = fake_post
    meta = resource.get_catalog_table_metadata("orders")
    assert meta == {
        "table_id": "t-real",
        "description": "Customer orders",
        "owners": ["jane@x.com"],  # email preferred -- see docstring: Dagster rejects bare display names
        "tags": ["pii"],
    }


def test_get_catalog_table_metadata_returns_none_on_error(mod):
    resource = _catalog_resource(mod)

    def fake_post(url, headers=None, json=None, timeout=None):
        return FakeResponse({"errors": [{"message": "boom"}]})

    mod.requests.post = fake_post
    with pytest.raises(RuntimeError):
        resource.get_catalog_table_metadata("orders")


def test_get_catalog_column_lineage_resolves_parent_names(mod):
    resource = _catalog_resource(mod)
    calls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(url)
        if "op=getColumns" in url and json["variables"]["scope"].get("tableId"):
            return FakeResponse({"data": {"getColumns": {"data": [{"id": "c1", "name": "order_id"}]}}})
        if "op=getFieldLineages" in url:
            return FakeResponse({"data": {"getFieldLineages": {"data": [{"parentColumnId": "pc1"}]}}})
        if "op=getColumns" in url and json["variables"]["scope"].get("ids"):
            return FakeResponse({
                "data": {"getColumns": {"data": [{"id": "pc1", "name": "id", "table": {"name": "raw_orders"}}]}}
            })
        raise AssertionError(f"unexpected call {url}")

    mod.requests.post = fake_post
    lineage = resource.get_catalog_column_lineage("t-real")
    assert lineage == {"order_id": ["raw_orders.id"]}


def test_get_catalog_column_lineage_fails_soft(mod):
    resource = _catalog_resource(mod)

    def fake_post(url, headers=None, json=None, timeout=None):
        raise RuntimeError("network down")

    mod.requests.post = fake_post
    assert resource.get_catalog_column_lineage("t-real") == {}


def test_enrich_nodes_with_catalog_metadata_is_best_effort(mod):
    resource = _catalog_resource(mod)
    nodes = [
        {"nodeId": "n1", "name": "good_table"},
        {"nodeId": "n2", "name": "bad_table"},
    ]

    def fake_post(url, headers=None, json=None, timeout=None):
        name = json["variables"]["scope"]["nameContains"]
        if name == "bad_table":
            raise RuntimeError("catalog down for this one")
        return FakeResponse({
            "data": {"getTables": {"data": [
                {"id": "t1", "name": "good_table", "descriptionMarkdown": "desc",
                 "ownerEntities": [], "tagEntities": []},
            ]}}
        })

    def fake_get(*a, **k):
        raise AssertionError("column lineage should not be fetched without a table_id follow-up call issue")

    mod.requests.post = fake_post
    mod._enrich_nodes_with_catalog_metadata(nodes, resource)

    assert nodes[0]["_catalog"]["description"] == "desc"
    assert "_catalog" not in nodes[1]  # soft-failed, left alone


def test_build_defs_includes_catalog_description_and_owners(mod):
    nodes = [{
        "nodeId": "n1", "name": "stg_orders", "sourceNodeIds": [], "type": "stage",
        "_catalog": {"description": "Catalog desc", "owners": ["jane@x.com"], "tags": ["pii"],
                     "column_lineage": {"order_id": ["raw.id"]}},
    }]
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="TOK",
        api_base_url="https://x", asset_name_prefix=None, group_name="coalesce",
        poll_interval=0, timeout=5,
    )
    assets_def = next(iter(defs.assets))
    spec = assets_def.specs_by_key[dg.AssetKey(["stg_orders"])]
    assert spec.description == "Catalog desc"
    assert spec.owners == ["jane@x.com"]
    assert spec.metadata["coalesce/catalog_tags"].value == ["pii"]
    assert spec.metadata["coalesce/column_lineage"].value == {"order_id": ["raw.id"]}


def test_invalid_catalog_owner_is_dropped_not_crashed(mod):
    """A Catalog owner with no email on file (only a display name) must be
    dropped, not passed straight into AssetSpec(owners=...) where Dagster
    raises DagsterInvalidDefinitionError."""
    nodes = [{
        "nodeId": "n1", "name": "stg_orders", "sourceNodeIds": [], "type": "stage",
        "_catalog": {"description": None, "owners": ["Jane Doe"], "tags": [], "column_lineage": {}},
    }]
    defs = mod._build_coalesce_defs(
        nodes=nodes, environment_id="env1", api_token_env_var="TOK",
        api_base_url="https://x", asset_name_prefix=None, group_name="coalesce",
        poll_interval=0, timeout=5,
    )
    assets_def = next(iter(defs.assets))
    spec = assets_def.specs_by_key[dg.AssetKey(["stg_orders"])]
    assert not spec.owners
