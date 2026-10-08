"""Committed regression tests for HotjarIngestionComponent.

The two external calls this component makes -- the OAuth2 client_credentials
token mint against the real Hotjar API, and the dlt pipeline run (+
follow-up sql_client() query-back) -- are monkeypatched wholesale via
`install_fake_token_mint` / `install_fake_dlt` (see conftest.py). Everything
the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`, including dependent-resource
'resolve' chaining for survey_details/survey_responses and the implicit
inclusion of 'surveys'), the bearer-auth config shape built from the
(faked) minted token, and the per-resource DataFrame combination + metadata
shape.
"""

import pandas as pd
import pytest

from .conftest import install_fake_dlt, install_fake_token_mint, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, raise_on_error=True, token="fake-minted-token"):
    import dagster as dg

    mint_calls = install_fake_token_mint(monkeypatch, mod, token=token)
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured, mint_calls


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _build_resources_config: pure logic, no mocking needed ------------------------

def test_default_resources_expands_to_surveys_plus_survey_responses(mod):
    config_resources = mod._build_resources_config("surveys,survey_responses", "site123", 100)
    names = [r["name"] for r in config_resources]
    assert names == ["surveys", "survey_responses"]


def test_surveys_resource_uses_real_path_and_cursor_paginator(mod):
    config_resources = mod._build_resources_config("surveys", "site123", 50)
    assert len(config_resources) == 1
    endpoint = config_resources[0]["endpoint"]
    assert endpoint["path"] == "sites/site123/surveys"
    assert endpoint["data_selector"] == "results"
    assert endpoint["paginator"] == {
        "type": "cursor",
        "cursor_param": "cursor",
        "cursor_path": "next_cursor",
    }
    assert endpoint["params"] == {"limit": 50}


def test_survey_responses_implicitly_includes_surveys(mod):
    # Requesting only 'survey_responses' (not 'surveys' explicitly) must
    # still produce a 'surveys' parent resource, since dlt's 'resolve'
    # mechanism needs it present to chain off of.
    config_resources = mod._build_resources_config("survey_responses", "site123", 100)
    names = [r["name"] for r in config_resources]
    assert names == ["surveys", "survey_responses"]


def test_survey_details_implicitly_includes_surveys(mod):
    config_resources = mod._build_resources_config("survey_details", "site123", 100)
    names = [r["name"] for r in config_resources]
    assert names == ["surveys", "survey_details"]


def test_surveys_not_duplicated_when_explicitly_requested_alongside_dependents(mod):
    config_resources = mod._build_resources_config("surveys,survey_details,survey_responses", "site123", 100)
    names = [r["name"] for r in config_resources]
    assert names == ["surveys", "survey_details", "survey_responses"]
    assert names.count("surveys") == 1


def test_survey_responses_resolve_params_reference_surveys_id(mod):
    config_resources = mod._build_resources_config("survey_responses", "site123", 100)
    responses_resource = next(r for r in config_resources if r["name"] == "survey_responses")
    params = responses_resource["endpoint"]["params"]
    assert params["survey_id"] == {"type": "resolve", "resource": "surveys", "field": "id"}
    assert params["limit"] == 100
    assert responses_resource["endpoint"]["path"] == "sites/site123/surveys/{survey_id}/responses"


def test_survey_details_resolve_params_reference_surveys_id(mod):
    config_resources = mod._build_resources_config("survey_details", "site123", 100)
    details_resource = next(r for r in config_resources if r["name"] == "survey_details")
    params = details_resource["endpoint"]["params"]
    assert params["survey_id"] == {"type": "resolve", "resource": "surveys", "field": "id"}
    assert details_resource["endpoint"]["path"] == "sites/site123/surveys/{survey_id}"
    # Singleton object response, not a paginated list -- no paginator, whole-body selector.
    assert details_resource["endpoint"]["data_selector"] == "$"
    assert "paginator" not in details_resource["endpoint"]


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("surveys,not_a_real_resource", "site123", 100)
    assert [r["name"] for r in config_resources] == ["surveys"]


def test_empty_resources_yields_empty_list(mod):
    assert mod._build_resources_config("", "site123", 100) == []


# --- end-to-end: minted-token bearer auth + real base URL + full resource set ------

def test_source_config_uses_minted_bearer_token_and_real_base_url(mod, monkeypatch):
    component = mod.HotjarIngestionComponent(
        asset_name="hotjar_out",
        site_id="site123",
        client_id="cid_abc",
        client_secret="secret_xyz",
    )
    _result, _pipeline, captured, mint_calls = _materialize(
        mod, component,
        table_rows={"surveys": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
        token="minted-tok-999",
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.hotjar.io/v1"
    assert config["client"]["auth"] == {"type": "bearer", "token": "minted-tok-999"}
    # The real client_id/client_secret were handed to the (faked) token mint.
    assert mint_calls == [("cid_abc", "secret_xyz")]


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.HotjarIngestionComponent(
        asset_name="hotjar_out",
        site_id="site123",
        client_id="cid_abc",
        client_secret="secret_xyz",
    )
    result, _pipeline, _captured, _mint_calls = _materialize(
        mod, component,
        table_rows={
            "surveys": pd.DataFrame({"id": [1]}),
            "survey_responses": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "hotjar_out")
    assert out["resources_requested"] == ["surveys", "survey_responses"]
    assert set(out["resources_loaded"]) == {"surveys", "survey_responses"}
    assert out["row_count"] == 2


def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.HotjarIngestionComponent(
        asset_name="hotjar_out",
        site_id="site123",
        client_id="cid_abc",
        client_secret="secret_xyz",
        resources="surveys,survey_details,survey_responses",
    )
    result, _pipeline, _captured, _mint_calls = _materialize(
        mod, component,
        table_rows={
            "surveys": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "survey_details": pd.DataFrame({"id": [1], "questions": ["q1"]}),
            "survey_responses": pd.DataFrame({"id": [1], "response": ["r1"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "hotjar_out")
    assert out["row_count"] == 4
    assert out["rows_surveys"] == 2
    assert out["rows_survey_details"] == 1
    assert out["rows_survey_responses"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.HotjarIngestionComponent(
        asset_name="hotjar_out",
        site_id="site123",
        client_id="cid_abc",
        client_secret="secret_xyz",
        resources="surveys",
    )
    result, _pipeline, _captured, _mint_calls = _materialize(
        mod, component,
        table_rows={"surveys": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "hotjar_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.HotjarIngestionComponent(
        asset_name="hotjar_out",
        site_id="site123",
        client_id="cid_abc",
        client_secret="secret_xyz",
        resources="surveys",
        persist_only=True,
    )
    result, fake_pipeline, _captured, _mint_calls = _materialize(
        mod, component,
        table_rows={"surveys": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []


# --- token-mint helper, exercised for real (not mocked) ----------------------------

def test_mint_hotjar_token_real_function_posts_form_encoded_and_parses_access_token(mod, monkeypatch):
    """Exercises `_mint_hotjar_token` itself for real (the one place in this
    component that is allowed to be a genuine network call target in
    production) by faking only `requests.post`, to confirm the request
    shape (form-encoded client_credentials grant) and response parsing."""

    captured_request = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "tok_real_456", "expires_in": 3600}

    def fake_post(url, data=None, headers=None, timeout=None):
        captured_request["url"] = url
        captured_request["data"] = data
        captured_request["headers"] = headers
        return FakeResponse()

    import requests
    monkeypatch.setattr(requests, "post", fake_post)

    token = mod._mint_hotjar_token("cid_abc", "secret_xyz")

    assert token == "tok_real_456"
    assert captured_request["url"] == "https://api.hotjar.io/v1/oauth/token"
    assert captured_request["data"] == {
        "grant_type": "client_credentials",
        "client_id": "cid_abc",
        "client_secret": "secret_xyz",
    }
    assert captured_request["headers"] == {"Content-Type": "application/x-www-form-urlencoded"}


def test_mint_hotjar_token_raises_on_missing_access_token(mod, monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"error": "invalid_client"}

    def fake_post(url, data=None, headers=None, timeout=None):
        return FakeResponse()

    import requests
    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(ValueError, match="no access_token"):
        mod._mint_hotjar_token("cid_abc", "secret_xyz")
