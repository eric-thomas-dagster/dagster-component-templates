"""Committed regression tests for TealiumIngestionComponent.

The two external calls this component makes -- the v3 login POST that
mints a (token, host) pair, and the dlt pipeline run against the real
Tealium Visitor Profile API (plus the follow-up sql_client() query-back)
-- are monkeypatched wholesale via `install_fake_auth` / `install_fake_dlt`
(see conftest.py). Everything the component actually owns is exercised for
real: resource-config construction (`_build_resources_config`), the
per-identifier fan-out (one resource per lookup value, doubled when
include_historical is set), sanitized resource-name suffixing
(`_safe_resource_suffix`), the login request shape, the dynamic
host-from-auth-response base URL, and the per-resource DataFrame
combination + metadata shape.
"""
import pandas as pd
import pytest

from .conftest import install_fake_auth, install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, token="fake-token-abc", host="us-east-1-platform.tealiumapis.com", raise_on_error=True):
    import dagster as dg

    auth_captured = install_fake_auth(monkeypatch, token=token, host=host)
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured, auth_captured


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _safe_resource_suffix / _build_resources_config: pure logic, no mocking needed ----

def test_safe_resource_suffix_replaces_non_alnum_chars(mod):
    assert mod._safe_resource_suffix("cust@example.com") == "cust_example_com"


def test_safe_resource_suffix_truncates_long_values(mod):
    long_value = "x" * 100
    assert len(mod._safe_resource_suffix(long_value)) == 40


def test_build_resources_one_per_lookup_value(mod):
    config_resources = mod._build_resources_config("acct", "prof", "crm_id", "cust_1,cust_2,cust_3", False)
    names = [r["name"] for r in config_resources]
    assert names == ["visitor_0_cust_1", "visitor_1_cust_2", "visitor_2_cust_3"]


def test_build_resources_uses_live_path_and_params(mod):
    config_resources = mod._build_resources_config("acct", "prof", "crm_id", "cust_1", False)
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "v3/customer/visitor/accounts/acct/profiles/prof"
    assert endpoint["params"] == {"attributeId": "crm_id", "attributeValue": "cust_1"}


def test_include_historical_doubles_resources_with_historical_path(mod):
    config_resources = mod._build_resources_config("acct", "prof", "crm_id", "cust_1,cust_2", True)
    names = [r["name"] for r in config_resources]
    assert names == ["visitor_0_cust_1", "visitor_0_cust_1_historical", "visitor_1_cust_2", "visitor_1_cust_2_historical"]
    historical = config_resources[1]
    assert historical["endpoint"]["path"] == "v3/customer/visitor/historical/accounts/acct/profiles/prof"
    assert historical["endpoint"]["params"] == {"attributeId": "crm_id", "attributeValue": "cust_1"}


# --- end-to-end: login call shape + dynamic host-based base URL -----------------------

def test_login_call_posts_username_and_key_as_form_data(mod, monkeypatch):
    component = mod.TealiumIngestionComponent(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1",
    )
    _result, _pipeline, captured, auth_captured = _materialize(
        mod, component,
        table_rows={"visitor_0_cust_1": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert auth_captured["url"] == "https://platform.tealiumapis.com/v3/auth/accounts/mycompany/profiles/main"
    assert auth_captured["data"] == {"username": "user@example.com", "key": "key_abc123"}


def test_source_config_uses_dynamic_host_from_auth_response(mod, monkeypatch):
    component = mod.TealiumIngestionComponent(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1",
    )
    _result, _pipeline, captured, _auth = _materialize(
        mod, component,
        table_rows={"visitor_0_cust_1": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
        token="minted-token-789",
        host="eu-west-1-platform.tealiumapis.com",
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://eu-west-1-platform.tealiumapis.com"
    assert config["client"]["auth"] == {"type": "bearer", "token": "minted-token-789"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.TealiumIngestionComponent(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1,cust_2",
    )
    result, _pipeline, _captured, _auth = _materialize(
        mod, component,
        table_rows={
            "visitor_0_cust_1": pd.DataFrame({"id": [1]}),
            "visitor_1_cust_2": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "tealium_out")
    assert out["resources_requested"] == ["visitor_0_cust_1", "visitor_1_cust_2"]
    assert out["row_count"] == 2


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.TealiumIngestionComponent(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1,cust_2",
    )
    result, _pipeline, _captured, _auth = _materialize(
        mod, component,
        table_rows={
            "visitor_0_cust_1": pd.DataFrame({"id": [1], "name": ["a"]}),
            "visitor_1_cust_2": pd.DataFrame({"id": [1, 2], "name": ["b", "c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "tealium_out")
    assert out["row_count"] == 3
    assert out["rows_visitor_0_cust_1"] == 1
    assert out["rows_visitor_1_cust_2"] == 2


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.TealiumIngestionComponent(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1",
    )
    result, _pipeline, _captured, _auth = _materialize(
        mod, component,
        table_rows={"visitor_0_cust_1": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "tealium_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.TealiumIngestionComponent(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1",
        persist_only=True,
    )
    result, fake_pipeline, _captured, _auth = _materialize(
        mod, component,
        table_rows={"visitor_0_cust_1": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []
