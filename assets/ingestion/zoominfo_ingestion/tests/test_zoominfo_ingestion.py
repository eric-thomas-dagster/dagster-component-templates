"""Committed regression tests for ZoomInfoIngestionComponent.

The external calls this component makes -- the OAuth2 Client Credentials
token POST, the dlt pipeline run against the real ZoomInfo Data API, and
the follow-up sql_client() query-back -- are monkeypatched wholesale via
`install_fake_requests_post` / `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real:
resource-config construction (`_build_resources_config`), the OAuth2
token-request shape (Basic auth + grant_type=client_credentials), the
bearer-token auth shape built from the fetched access token, and the
per-resource DataFrame combination + metadata shape.
"""
import pandas as pd
import pytest

from .conftest import install_fake_dlt, install_fake_requests_post, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, access_token="fake-access-token", raise_on_error=True):
    import dagster as dg

    token_captured = install_fake_requests_post(monkeypatch, mod, access_token=access_token)
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured, token_captured


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _get_zoominfo_access_token: pure logic (requests.post mocked) ----------------

def test_get_access_token_posts_client_credentials_grant(mod, monkeypatch):
    captured = install_fake_requests_post(monkeypatch, mod, access_token="tok_xyz")
    token = mod._get_zoominfo_access_token("my_client_id", "my_client_secret")
    assert token == "tok_xyz"
    assert captured["url"] == "https://api.zoominfo.com/gtm/oauth/v1/token"
    assert captured["auth"] == ("my_client_id", "my_client_secret")
    assert captured["data"] == {"grant_type": "client_credentials"}


# --- _build_resources_config: pure logic, no mocking needed ------------------------

def test_default_resources_expands_to_two_dlt_resources(mod):
    config_resources = mod._build_resources_config("contacts_search,companies_search", 25)
    names = [r["name"] for r in config_resources]
    assert names == ["contacts_search", "companies_search"]


def test_each_resource_is_a_post_with_json_body(mod):
    config_resources = mod._build_resources_config("contacts_search,companies_search", 50)
    for r in config_resources:
        assert r["endpoint"]["method"] == "POST"
        assert r["endpoint"]["json"] == {"page": 1, "per_page": 50}


def test_contacts_search_uses_real_data_v1_path(mod):
    config_resources = mod._build_resources_config("contacts_search", 25)
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "data/v1/contacts/search"


def test_companies_search_uses_real_data_v1_path(mod):
    config_resources = mod._build_resources_config("companies_search", 25)
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "data/v1/companies/search"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("contacts_search,not_a_real_resource", 25)
    assert [r["name"] for r in config_resources] == ["contacts_search"]


def test_enrich_is_not_a_valid_resource_name(mod):
    # Enrich endpoints require per-record identifiers (a lookup, not a
    # bulk listing) and are intentionally excluded -- see component
    # docstring/README.
    config_resources = mod._build_resources_config("contacts_enrich", 25)
    assert config_resources == []


# --- end-to-end: bearer auth built from fetched token + base URL -----------------

def test_source_config_uses_bearer_auth_from_fetched_token_and_real_base_url(mod, monkeypatch):
    component = mod.ZoomInfoIngestionComponent(
        asset_name="zoominfo_out",
        client_id="cid",
        client_secret="csecret",
    )
    _result, _pipeline, captured, token_captured = _materialize(
        mod, component,
        table_rows={"contacts_search": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
        access_token="tok_from_oauth",
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.zoominfo.com/gtm"
    assert config["client"]["auth"] == {"type": "bearer", "token": "tok_from_oauth"}
    assert token_captured["auth"] == ("cid", "csecret")


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.ZoomInfoIngestionComponent(
        asset_name="zoominfo_out",
        client_id="cid",
        client_secret="csecret",
    )
    result, _pipeline, _captured, _token = _materialize(
        mod, component,
        table_rows={
            "contacts_search": pd.DataFrame({"id": [1]}),
            "companies_search": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "zoominfo_out")
    assert out["resources_requested"] == ["contacts_search", "companies_search"]
    assert set(out["resources_loaded"]) == {"contacts_search", "companies_search"}
    assert out["row_count"] == 2


def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.ZoomInfoIngestionComponent(
        asset_name="zoominfo_out",
        client_id="cid",
        client_secret="csecret",
    )
    result, _pipeline, _captured, _token = _materialize(
        mod, component,
        table_rows={
            "contacts_search": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "companies_search": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "zoominfo_out")
    assert out["row_count"] == 3
    assert out["rows_contacts_search"] == 2
    assert out["rows_companies_search"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.ZoomInfoIngestionComponent(
        asset_name="zoominfo_out",
        client_id="cid",
        client_secret="csecret",
        resources="contacts_search",
    )
    result, _pipeline, _captured, _token = _materialize(
        mod, component,
        table_rows={"contacts_search": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "zoominfo_out")
    assert "row_count" not in out


def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.ZoomInfoIngestionComponent(
        asset_name="zoominfo_out",
        client_id="cid",
        client_secret="csecret",
        resources="contacts_search",
        persist_only=True,
    )
    result, fake_pipeline, _captured, _token = _materialize(
        mod, component,
        table_rows={"contacts_search": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []


def test_default_kinds_tag_zoominfo_and_python(mod):
    component = mod.ZoomInfoIngestionComponent(asset_name="x", client_id="cid", client_secret="csecret")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(mod.AssetKey("x"))
    assert "dagster/kind/zoominfo" in spec.tags
    assert "dagster/kind/python" in spec.tags
