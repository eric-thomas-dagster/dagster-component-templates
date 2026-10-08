"""Committed regression tests for DltRestApiSourceComponent.

The one external call this component makes -- the dlt pipeline run against
whatever REST API the caller configured, and the follow-up sql_client()
query-back -- is monkeypatched wholesale via `install_fake_dlt` (see
conftest.py). Everything the component actually owns is exercised for
real: config validation (`_validate_rest_api_config`), verbatim passthrough
of the user-supplied `client` / `resources` config into
`rest_api_source(config)` (simple config, auth config, and a dlt `resolve`-
based dependent resource), and the shared destination/partition
boilerplate (ported from reclaim_ingestion's tests, which cover the same
underlying helpers byte-for-byte).
"""
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, raise_on_error=True):
    import dagster as dg

    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _validate_rest_api_config: pure logic, no mocking needed ----------------------

def test_missing_base_url_raises_clear_error(mod):
    with pytest.raises(ValueError, match="client.base_url"):
        mod._validate_rest_api_config({}, [{"name": "x", "endpoint": {"path": "x"}}])


def test_base_url_present_but_empty_string_still_raises(mod):
    with pytest.raises(ValueError, match="client.base_url"):
        mod._validate_rest_api_config(
            {"base_url": ""}, [{"name": "x", "endpoint": {"path": "x"}}]
        )


def test_empty_resources_raises_clear_error(mod):
    with pytest.raises(ValueError, match="resources"):
        mod._validate_rest_api_config({"base_url": "https://api.example.com"}, [])


def test_resource_missing_name_raises_clear_error(mod):
    with pytest.raises(ValueError, match="'name'"):
        mod._validate_rest_api_config(
            {"base_url": "https://api.example.com"},
            [{"endpoint": {"path": "customers"}}],
        )


def test_resource_missing_endpoint_raises_clear_error(mod):
    with pytest.raises(ValueError, match="endpoint"):
        mod._validate_rest_api_config(
            {"base_url": "https://api.example.com"},
            [{"name": "customers"}],
        )


def test_resource_endpoint_wrong_type_raises_clear_error(mod):
    # A malformed `endpoint` (a string instead of a mapping) should raise a
    # clear ValueError naming the offending resource, not a raw dlt/pydantic
    # traceback several layers down.
    with pytest.raises(ValueError, match="malformed 'endpoint'"):
        mod._validate_rest_api_config(
            {"base_url": "https://api.example.com"},
            [{"name": "customers", "endpoint": "customers"}],
        )


def test_resource_endpoint_missing_path_raises_clear_error(mod):
    with pytest.raises(ValueError, match="'path'"):
        mod._validate_rest_api_config(
            {"base_url": "https://api.example.com"},
            [{"name": "customers", "endpoint": {"data_selector": "$"}}],
        )


def test_resource_not_a_dict_raises_clear_error(mod):
    with pytest.raises(ValueError, match="must be a mapping"):
        mod._validate_rest_api_config(
            {"base_url": "https://api.example.com"},
            ["not_a_dict"],
        )


def test_valid_config_does_not_raise(mod):
    mod._validate_rest_api_config(
        {"base_url": "https://api.example.com"},
        [{"name": "customers", "endpoint": {"path": "customers"}}],
    )


def test_build_defs_raises_on_empty_resources(mod):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[],
    )
    with pytest.raises(ValueError, match="resources"):
        component.build_defs(context=None)


def test_build_defs_raises_on_missing_base_url(mod):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
    )
    with pytest.raises(ValueError, match="base_url"):
        component.build_defs(context=None)


# --- passthrough: simple config -----------------------------------------------------

def test_simple_config_passes_through_verbatim(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="simple_out",
        client={"base_url": "https://mysite.chargify.com"},
        resources=[
            {"name": "customers", "endpoint": {"path": "customers.json", "data_selector": "$"}},
        ],
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"customers": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"] == {"base_url": "https://mysite.chargify.com"}
    assert config["resources"] == [
        {"name": "customers", "endpoint": {"path": "customers.json", "data_selector": "$"}},
    ]


def test_multiple_resources_pass_through_in_order(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="multi_out",
        client={"base_url": "https://api.example.com"},
        resources=[
            {"name": "customers", "endpoint": {"path": "customers"}},
            {"name": "orders", "endpoint": {"path": "orders"}},
        ],
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={
            "customers": pd.DataFrame({"id": [1]}),
            "orders": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    names = [r["name"] for r in captured["config"]["resources"]]
    assert names == ["customers", "orders"]


# --- passthrough: auth config --------------------------------------------------------

def test_bearer_auth_config_passes_through_verbatim(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="bearer_out",
        client={
            "base_url": "https://api.example.com/v1",
            "auth": {"type": "bearer", "token": "tok_abc123"},
        },
        resources=[{"name": "items", "endpoint": {"path": "items", "data_selector": "results"}}],
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"items": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert captured["config"]["client"]["auth"] == {"type": "bearer", "token": "tok_abc123"}


def test_http_basic_auth_config_passes_through_verbatim(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="basic_out",
        client={
            "base_url": "https://mysite.chargify.com",
            "auth": {"type": "http_basic", "username": "key_abc", "password": "x"},
        },
        resources=[{"name": "customers", "endpoint": {"path": "customers.json"}}],
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"customers": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert captured["config"]["client"]["auth"] == {
        "type": "http_basic", "username": "key_abc", "password": "x",
    }


def test_paginator_config_passes_through_verbatim(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="paginated_out",
        client={"base_url": "https://api.example.com/v1", "auth": {"type": "bearer", "token": "t"}},
        resources=[{
            "name": "items",
            "endpoint": {
                "path": "items",
                "data_selector": "results",
                "paginator": {"type": "cursor", "cursor_param": "cursor", "cursor_path": "next_cursor"},
                "params": {"limit": 100},
            },
        }],
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"items": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    endpoint = captured["config"]["resources"][0]["endpoint"]
    assert endpoint["paginator"] == {"type": "cursor", "cursor_param": "cursor", "cursor_path": "next_cursor"}
    assert endpoint["params"] == {"limit": 100}


# --- passthrough: resolve-based dependent resource ------------------------------------

def test_resolve_based_dependent_resource_passes_through_verbatim(mod, monkeypatch):
    # Mirrors hotjar_ingestion's real surveys -> survey_responses N+1 chaining.
    component = mod.DltRestApiSourceComponent(
        asset_name="parent_child_out",
        client={"base_url": "https://api.hotjar.io/v1", "auth": {"type": "bearer", "token": "t"}},
        resources=[
            {
                "name": "surveys",
                "endpoint": {"path": "sites/123/surveys", "data_selector": "results"},
            },
            {
                "name": "survey_responses",
                "endpoint": {
                    "path": "sites/123/surveys/{survey_id}/responses",
                    "data_selector": "results",
                    "params": {
                        "survey_id": {"type": "resolve", "resource": "surveys", "field": "id"},
                        "limit": 100,
                    },
                },
            },
        ],
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={
            "surveys": pd.DataFrame({"id": [1, 2]}),
            "survey_responses": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    resources = captured["config"]["resources"]
    assert resources[0]["name"] == "surveys"
    child_params = resources[1]["endpoint"]["params"]
    assert child_params["survey_id"] == {"type": "resolve", "resource": "surveys", "field": "id"}


# --- destination / boilerplate (ported from reclaim_ingestion) -----------------------

def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[
            {"name": "customers", "endpoint": {"path": "customers"}},
            {"name": "orders", "endpoint": {"path": "orders"}},
        ],
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "customers": pd.DataFrame({"id": [1]}),
            "orders": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "out")
    assert out["resources_requested"] == ["customers", "orders"]
    assert set(out["resources_loaded"]) == {"customers", "orders"}
    assert out["row_count"] == 2


def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"customers": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "out")
    assert out["row_count"] == 2
    assert out["rows_customers"] == 2


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"customers": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"customers": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []


def test_non_sql_destination_emits_materialize_result(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
        destination="filesystem",
        bucket_url="file:///tmp/dlt_rest_api_source_test",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "out")
    assert out["destination"] == "filesystem"


def test_partition_type_requires_start_date(mod):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
        partition_type="daily",
    )
    with pytest.raises(ValueError, match="partition_start"):
        component.build_defs(context=None)


def test_default_destination_is_duckdb_in_memory(mod, monkeypatch):
    component = mod.DltRestApiSourceComponent(
        asset_name="out",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"customers": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.pipeline_kwargs["destination"] == "duckdb"
