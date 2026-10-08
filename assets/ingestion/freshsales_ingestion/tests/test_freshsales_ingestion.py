"""Committed regression tests for FreshsalesIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Freshsales (Freshworks CRM) REST API, and the follow-up
sql_client() query-back -- is monkeypatched wholesale via `install_fake_dlt`
(see conftest.py). Everything the component actually owns is exercised for
real: resource-config construction (`_build_resources_config`), the
'Token token=<key>' auth header shape, the subdomain-based base URL, and
the per-resource DataFrame combination + metadata shape.
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

def test_default_resources_expands_to_four_dlt_resources(mod):
    config_resources = mod._build_resources_config("contacts,leads,deals,sales_accounts")
    names = [r["name"] for r in config_resources]
    assert names == ["contacts", "leads", "deals", "sales_accounts"]


def test_each_resource_uses_matching_path_and_data_selector(mod):
    config_resources = mod._build_resources_config("contacts,leads,deals,sales_accounts")
    for r in config_resources:
        assert r["endpoint"]["path"] == r["name"]
        assert r["endpoint"]["data_selector"] == r["name"]


def test_single_resource_subset(mod):
    config_resources = mod._build_resources_config("deals")
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "deals"
    assert config_resources[0]["endpoint"]["path"] == "deals"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("contacts,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["contacts"]


# --- end-to-end: auth header shape + base URL + full resource set -----------------

def test_source_config_uses_token_auth_header_and_subdomain_base_url(mod, monkeypatch):
    component = mod.FreshsalesIngestionComponent(
        asset_name="freshsales_out",
        domain="widgetz",
        api_key="abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"contacts": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://widgetz.myfreshworks.com/crm/sales/api"
    assert config["client"]["auth"] == {
        "type": "api_key",
        "api_key": "Token token=abc123",
        "name": "Authorization",
        "location": "header",
    }


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.FreshsalesIngestionComponent(
        asset_name="freshsales_out",
        domain="widgetz",
        api_key="abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "contacts": pd.DataFrame({"id": [1]}),
            "leads": pd.DataFrame({"id": [1]}),
            "deals": pd.DataFrame({"id": [1]}),
            "sales_accounts": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "freshsales_out")
    assert out["resources_requested"] == ["contacts", "leads", "deals", "sales_accounts"]
    assert set(out["resources_loaded"]) == {"contacts", "leads", "deals", "sales_accounts"}
    assert out["row_count"] == 4


def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.FreshsalesIngestionComponent(
        asset_name="freshsales_out",
        domain="widgetz",
        api_key="abc123",
        resources="contacts,deals",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "contacts": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "deals": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "freshsales_out")
    assert out["row_count"] == 3
    assert out["rows_contacts"] == 2
    assert out["rows_deals"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.FreshsalesIngestionComponent(
        asset_name="freshsales_out",
        domain="widgetz",
        api_key="abc123",
        resources="contacts",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"contacts": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "freshsales_out")
    assert "row_count" not in out


def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.FreshsalesIngestionComponent(
        asset_name="freshsales_out",
        domain="widgetz",
        api_key="abc123",
        resources="contacts",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"contacts": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []


def test_default_kinds_tag_freshsales_and_python(mod):
    component = mod.FreshsalesIngestionComponent(asset_name="x", domain="widgetz", api_key="abc123")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(mod.AssetKey("x"))
    assert "dagster/kind/freshsales" in spec.tags
    assert "dagster/kind/python" in spec.tags
