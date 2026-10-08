"""Committed regression tests for AttentiveIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Attentive REST API, and the follow-up sql_client() query-back -- is
monkeypatched wholesale via `install_fake_dlt` (see conftest.py). Everything
the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the v2-vs-v1 per-resource version
prefix quirk, the bearer-auth config shape, and the per-resource DataFrame
combination + metadata shape.
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

def test_default_resources_expands_to_segments_and_webhooks(mod):
    config_resources = mod._build_resources_config("segments,webhooks")
    names = [r["name"] for r in config_resources]
    assert names == ["segments", "webhooks"]


def test_segments_uses_v2_prefix_not_v1(mod):
    config_resources = mod._build_resources_config("segments")
    assert config_resources[0]["endpoint"]["path"] == "v2/segments"


def test_webhooks_and_catalog_uploads_use_v1_prefix(mod):
    config_resources = mod._build_resources_config("webhooks,product_catalog_uploads")
    paths = [r["endpoint"]["path"] for r in config_resources]
    assert paths == ["v1/webhooks", "v1/product-catalog/uploads"]


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("segments,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["segments"]


# --- end-to-end: bearer auth + base URL -----------------------

def test_source_config_uses_bearer_auth_and_real_base_url(mod, monkeypatch):
    component = mod.AttentiveIngestionComponent(
        asset_name="attentive_out",
        api_key="key_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"segments": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.attentivemobile.com"
    assert config["client"]["auth"] == {"type": "bearer", "token": "key_abc123"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.AttentiveIngestionComponent(
        asset_name="attentive_out",
        api_key="key_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "segments": pd.DataFrame({"id": [1]}),
            "webhooks": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "attentive_out")
    assert out["resources_requested"] == ["segments", "webhooks"]
    assert set(out["resources_loaded"]) == {"segments", "webhooks"}
    assert out["row_count"] == 2


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.AttentiveIngestionComponent(
        asset_name="attentive_out",
        api_key="key_abc123",
        resources="segments,product_catalog_uploads",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "segments": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "product_catalog_uploads": pd.DataFrame({"uploadId": [1], "status": ["completed"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "attentive_out")
    assert out["row_count"] == 3
    assert out["rows_segments"] == 2
    assert out["rows_product_catalog_uploads"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.AttentiveIngestionComponent(
        asset_name="attentive_out",
        api_key="key_abc123",
        resources="segments",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"segments": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "attentive_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.AttentiveIngestionComponent(
        asset_name="attentive_out",
        api_key="key_abc123",
        resources="segments",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"segments": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    assert fake_pipeline.sql_client_instance.queries == []
