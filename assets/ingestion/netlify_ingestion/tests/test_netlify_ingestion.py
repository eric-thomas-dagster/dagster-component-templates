"""Committed regression tests for NetlifyIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Netlify REST API, and the follow-up sql_client() query-back -- is
monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real:
resource-config construction (`_build_resources_config`), the
site_id-required-for-deploys/forms/submissions validation, the bearer-auth
config shape and base URL, and the per-resource DataFrame combination +
metadata shape.
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

def test_default_sites_resource_has_no_site_id_requirement(mod):
    config_resources = mod._build_resources_config("sites", None)
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "sites"
    assert config_resources[0]["endpoint"]["path"] == "sites"


def test_deploys_without_site_id_raises_clear_error(mod):
    with pytest.raises(ValueError, match="site_id"):
        mod._build_resources_config("deploys", None)


def test_forms_without_site_id_raises_clear_error(mod):
    with pytest.raises(ValueError, match="site_id"):
        mod._build_resources_config("forms", None)


def test_submissions_without_site_id_raises_clear_error(mod):
    with pytest.raises(ValueError, match="site_id"):
        mod._build_resources_config("submissions", None)


def test_multiple_site_scoped_resources_without_site_id_lists_all_in_error(mod):
    with pytest.raises(ValueError) as exc_info:
        mod._build_resources_config("deploys,forms,submissions", None)
    msg = str(exc_info.value)
    assert "deploys" in msg and "forms" in msg and "submissions" in msg


def test_site_scoped_resources_with_site_id_build_correct_paths(mod):
    config_resources = mod._build_resources_config("deploys,forms,submissions", "abc123")
    paths = {r["name"]: r["endpoint"]["path"] for r in config_resources}
    assert paths["deploys"] == "sites/abc123/deploys"
    assert paths["forms"] == "sites/abc123/forms"
    assert paths["submissions"] == "sites/abc123/submissions"


def test_sites_resource_ignores_site_id_when_mixed_with_scoped_resources(mod):
    config_resources = mod._build_resources_config("sites,deploys", "abc123")
    names = [r["name"] for r in config_resources]
    assert names == ["sites", "deploys"]
    sites_entry = next(r for r in config_resources if r["name"] == "sites")
    assert sites_entry["endpoint"]["path"] == "sites"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("sites,not_a_real_resource", None)
    assert [r["name"] for r in config_resources] == ["sites"]


# --- end-to-end: bearer auth + base URL + full resource set -----------------------

def test_source_config_uses_bearer_auth_and_real_base_url(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"sites": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.netlify.com/api/v1/"
    assert config["client"]["auth"] == {"type": "bearer", "token": "tok_abc123"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"sites": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "netlify_out")
    assert out["resources_requested"] == ["sites"]
    assert set(out["resources_loaded"]) == {"sites"}
    assert out["row_count"] == 1


def test_site_scoped_resources_end_to_end_with_site_id(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
        site_id="my-site.netlify.app",
        resources="deploys,forms",
    )
    result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={
            "deploys": pd.DataFrame({"id": [1, 2]}),
            "forms": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    config_resources = captured["config"]["resources"]
    paths = {r["name"]: r["endpoint"]["path"] for r in config_resources}
    assert paths["deploys"] == "sites/my-site.netlify.app/deploys"
    assert paths["forms"] == "sites/my-site.netlify.app/forms"
    out = _metadata_for(result, "netlify_out")
    assert out["row_count"] == 3


def test_materializing_without_site_id_for_scoped_resource_raises(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
        resources="deploys",
    )
    with pytest.raises(Exception, match="site_id"):
        _materialize(mod, component, table_rows={}, monkeypatch=monkeypatch)


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
        site_id="site1",
        resources="sites,deploys",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "sites": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "deploys": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "netlify_out")
    assert out["row_count"] == 3
    assert out["rows_sites"] == 2
    assert out["rows_deploys"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
        resources="sites",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"sites": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "netlify_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.NetlifyIngestionComponent(
        asset_name="netlify_out",
        access_token="tok_abc123",
        resources="sites",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"sites": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
