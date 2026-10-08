"""Committed regression tests for PardotIngestionComponent.

The two external calls this component makes -- the Salesforce OAuth2
refresh_token exchange, and the dlt pipeline run against the real Pardot v5
REST API (plus the follow-up sql_client() query-back) -- are monkeypatched
wholesale via `install_fake_oauth_token` / `install_fake_dlt` (see
conftest.py). Everything the component actually owns is exercised for real:
resource-config construction (`_build_resources_config`), the sandbox host
switch (both the OAuth login host AND the Pardot API host), the required
Pardot-Business-Unit-Id header, the refresh_token request shape, and the
per-resource DataFrame combination + metadata shape.
"""
import pandas as pd
import pytest

from .conftest import install_fake_dlt, install_fake_oauth_token, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, access_token="fake-access-token", raise_on_error=True):
    import dagster as dg

    oauth_captured = install_fake_oauth_token(monkeypatch, access_token=access_token)
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured, oauth_captured


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _build_resources_config: pure logic, no mocking needed ------------------------

def test_default_resources_expands_to_prospects_and_campaigns(mod):
    config_resources = mod._build_resources_config("prospects,campaigns")
    names = [r["name"] for r in config_resources]
    assert names == ["prospects", "campaigns"]


def test_all_four_resources_recognized_with_real_paths(mod):
    config_resources = mod._build_resources_config("prospects,campaigns,lists,visitors")
    paths = [r["endpoint"]["path"] for r in config_resources]
    assert paths == ["objects/prospects", "objects/campaigns", "objects/lists", "objects/visitors"]


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("prospects,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["prospects"]


# --- end-to-end: OAuth2 refresh_token exchange + Business-Unit-Id header -----------------------

def test_refresh_token_exchange_posts_to_production_login_host(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
    )
    _result, _pipeline, captured, oauth_captured = _materialize(
        mod, component,
        table_rows={"prospects": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    assert oauth_captured["url"] == "https://login.salesforce.com/services/oauth2/token"
    assert oauth_captured["data"] == {
        "grant_type": "refresh_token",
        "refresh_token": "refresh_123",
        "client_id": "client_abc",
        "client_secret": "secret_xyz",
    }


def test_sandbox_mode_switches_both_login_host_and_api_host(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
        use_sandbox=True,
    )
    _result, _pipeline, captured, oauth_captured = _materialize(
        mod, component,
        table_rows={"prospects": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert oauth_captured["url"] == "https://test.salesforce.com/services/oauth2/token"
    assert captured["config"]["client"]["base_url"] == "https://pi.demo.pardot.com/api/v5"


def test_source_config_sends_business_unit_id_header_and_bearer_auth(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
    )
    _result, _pipeline, captured, _oauth = _materialize(
        mod, component,
        table_rows={"prospects": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
        access_token="minted-token-456",
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://pi.pardot.com/api/v5"
    assert config["client"]["auth"] == {"type": "bearer", "token": "minted-token-456"}
    assert config["client"]["headers"] == {"Pardot-Business-Unit-Id": "0Uv000000000001AAA"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
    )
    result, _pipeline, _captured, _oauth = _materialize(
        mod, component,
        table_rows={
            "prospects": pd.DataFrame({"id": [1]}),
            "campaigns": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "pardot_out")
    assert out["resources_requested"] == ["prospects", "campaigns"]
    assert out["row_count"] == 2


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
        resources="lists,visitors",
    )
    result, _pipeline, _captured, _oauth = _materialize(
        mod, component,
        table_rows={
            "lists": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "visitors": pd.DataFrame({"id": [1], "ip": ["1.2.3.4"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "pardot_out")
    assert out["row_count"] == 3
    assert out["rows_lists"] == 2
    assert out["rows_visitors"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
        resources="prospects",
    )
    result, _pipeline, _captured, _oauth = _materialize(
        mod, component,
        table_rows={"prospects": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "pardot_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.PardotIngestionComponent(
        asset_name="pardot_out",
        business_unit_id="0Uv000000000001AAA",
        client_id="client_abc",
        client_secret="secret_xyz",
        refresh_token="refresh_123",
        resources="prospects",
        persist_only=True,
    )
    result, fake_pipeline, _captured, _oauth = _materialize(
        mod, component,
        table_rows={"prospects": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []
