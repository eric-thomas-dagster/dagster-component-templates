"""Committed regression tests for ApolloIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Apollo.io REST API, and the follow-up sql_client() query-back --
is monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-
config construction (`_build_resources_config`), the POST + JSON-body
endpoint shape, the 'x-api-key' auth header shape, and the per-resource
DataFrame combination + metadata shape.
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


# --- _build_resources_config: pure logic, no mocking needed ------------------------

def test_default_resources_expands_to_three_dlt_resources(mod):
    config_resources = mod._build_resources_config("people,organizations,contacts", 25)
    names = [r["name"] for r in config_resources]
    assert names == ["people", "organizations", "contacts"]


def test_each_resource_is_a_post_with_json_body_and_matching_data_selector(mod):
    config_resources = mod._build_resources_config("people,organizations,contacts", 50)
    for r in config_resources:
        assert r["endpoint"]["method"] == "POST"
        assert r["endpoint"]["json"] == {"page": 1, "per_page": 50}
        assert r["endpoint"]["data_selector"] == r["name"]


def test_people_resource_uses_search_path(mod):
    config_resources = mod._build_resources_config("people", 25)
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "people/search"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("people,not_a_real_resource", 25)
    assert [r["name"] for r in config_resources] == ["people"]


# --- end-to-end: auth header shape + base URL + full resource set -----------------

def test_source_config_uses_x_api_key_header_and_real_base_url(mod, monkeypatch):
    component = mod.ApolloIngestionComponent(
        asset_name="apollo_out",
        api_key="apollo_tok_abc",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"people": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.apollo.io/api/v1"
    assert config["client"]["auth"] == {
        "type": "api_key",
        "api_key": "apollo_tok_abc",
        "name": "x-api-key",
        "location": "header",
    }


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.ApolloIngestionComponent(
        asset_name="apollo_out",
        api_key="apollo_tok_abc",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "people": pd.DataFrame({"id": [1]}),
            "organizations": pd.DataFrame({"id": [1]}),
            "contacts": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "apollo_out")
    assert out["resources_requested"] == ["people", "organizations", "contacts"]
    assert set(out["resources_loaded"]) == {"people", "organizations", "contacts"}
    assert out["row_count"] == 3


def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.ApolloIngestionComponent(
        asset_name="apollo_out",
        api_key="apollo_tok_abc",
        resources="people,organizations",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "people": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "organizations": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "apollo_out")
    assert out["row_count"] == 3
    assert out["rows_people"] == 2
    assert out["rows_organizations"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.ApolloIngestionComponent(
        asset_name="apollo_out",
        api_key="apollo_tok_abc",
        resources="people",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"people": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "apollo_out")
    assert "row_count" not in out


def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.ApolloIngestionComponent(
        asset_name="apollo_out",
        api_key="apollo_tok_abc",
        resources="people",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"people": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []


def test_default_kinds_tag_apollo_and_python(mod):
    component = mod.ApolloIngestionComponent(asset_name="x", api_key="apollo_tok_abc")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(mod.AssetKey("x"))
    assert "dagster/kind/apollo" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_page_size_configures_per_page_in_json_body(mod, monkeypatch):
    component = mod.ApolloIngestionComponent(
        asset_name="apollo_out",
        api_key="apollo_tok_abc",
        resources="people",
        page_size=100,
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"people": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert captured["config"]["resources"][0]["endpoint"]["json"]["per_page"] == 100
