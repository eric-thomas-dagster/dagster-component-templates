"""Committed regression tests for ChorusByZoomInfoIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Chorus by ZoomInfo REST API, and the follow-up sql_client()
query-back -- is monkeypatched wholesale via `install_fake_dlt` (see
conftest.py). Everything the component actually owns is exercised for
real: resource-config construction (`_build_resources_config`), the
raw-token (no 'Bearer ' prefix) auth header shape, and the per-resource
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

def test_default_resources_expands_to_two_dlt_resources(mod):
    config_resources = mod._build_resources_config("engagements,users")
    names = [r["name"] for r in config_resources]
    assert names == ["engagements", "users"]


def test_engagements_resource_uses_real_v3_path(mod):
    config_resources = mod._build_resources_config("engagements")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "v3/engagements"


def test_users_resource_uses_v3_path(mod):
    config_resources = mod._build_resources_config("users")
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "v3/users"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("engagements,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["engagements"]


def test_transcripts_is_not_a_valid_resource_name(mod):
    # Transcript retrieval is tied to a specific engagement_id (a per-
    # record lookup, not a bulk listing) and the exact nested path was
    # not confirmed with enough confidence to ship -- see component
    # docstring/README.
    config_resources = mod._build_resources_config("transcripts")
    assert config_resources == []


# --- end-to-end: raw-token auth header shape (no Bearer prefix) + base URL -------

def test_source_config_uses_raw_token_auth_header_and_real_base_url(mod, monkeypatch):
    component = mod.ChorusByZoomInfoIngestionComponent(
        asset_name="chorus_out",
        api_token="raw_tok_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"engagements": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://chorus.ai"
    assert config["client"]["auth"] == {
        "type": "api_key",
        "api_key": "raw_tok_abc123",
        "name": "Authorization",
        "location": "header",
    }
    # No "Bearer " prefix anywhere in the configured auth value.
    assert "Bearer" not in config["client"]["auth"]["api_key"]


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.ChorusByZoomInfoIngestionComponent(
        asset_name="chorus_out",
        api_token="raw_tok_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "engagements": pd.DataFrame({"id": [1]}),
            "users": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "chorus_out")
    assert out["resources_requested"] == ["engagements", "users"]
    assert set(out["resources_loaded"]) == {"engagements", "users"}
    assert out["row_count"] == 2


def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.ChorusByZoomInfoIngestionComponent(
        asset_name="chorus_out",
        api_token="raw_tok_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "engagements": pd.DataFrame({"id": [1, 2], "subject": ["a", "b"]}),
            "users": pd.DataFrame({"id": [1], "subject": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "chorus_out")
    assert out["row_count"] == 3
    assert out["rows_engagements"] == 2
    assert out["rows_users"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.ChorusByZoomInfoIngestionComponent(
        asset_name="chorus_out",
        api_token="raw_tok_abc123",
        resources="engagements",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"engagements": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "chorus_out")
    assert "row_count" not in out


def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.ChorusByZoomInfoIngestionComponent(
        asset_name="chorus_out",
        api_token="raw_tok_abc123",
        resources="engagements",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"engagements": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []


def test_default_kinds_tag_chorus_and_python(mod):
    component = mod.ChorusByZoomInfoIngestionComponent(asset_name="x", api_token="raw_tok_abc123")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(mod.AssetKey("x"))
    assert "dagster/kind/chorus" in spec.tags
    assert "dagster/kind/python" in spec.tags
