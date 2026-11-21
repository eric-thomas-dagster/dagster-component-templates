"""Real, committed test for ADF-converter feedback ask 8: fabric_workspace's item_parameters/
default_parameters are static YAML, so a Fabric notebook whose parameters come from an upstream
asset's output or a sensor (not a value known ahead of time) couldn't be a component asset at all
-- the converter had to run it inline through FabricWorkspaceResource instead. Adds a run_config
`parameters` override (FabricRunConfig), merged OVER every other parameter source (static config,
partition key, load_date) -- the same run_config-overrides-static-config convention this registry
already uses elsewhere (e.g. adls_to_database_asset's container_name/blob_name).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fabric_workspace_run_config_parameters", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


@pytest.fixture()
def mod():
    return _mod


def _component(mod, **overrides):
    attrs = dict(
        workspace=mod.FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_lakehouses=False, import_warehouses=False,
        import_notebooks=True, import_pipelines=False,
        notebooks=[mod.FabricRunnableItemDeclaration(item_id="nb-1", display_name="scoring")],
    )
    attrs.update(overrides)
    return mod.FabricWorkspaceComponent(**attrs)


@pytest.fixture
def auth(mod, monkeypatch):
    monkeypatch.setattr(mod.FabricResource, "get_token", lambda self: "fake-token")


def test_run_config_parameters_override_static_default_parameters(mod, auth, monkeypatch):
    captured = {}

    def _fake_trigger_item_run(self, context, item_id, item_type, parameters, extra_execution_data=None):
        captured["parameters"] = parameters
        return {"id": "job-1", "status": "Completed"}

    monkeypatch.setattr(mod.FabricWorkspaceComponent, "_trigger_item_run", _fake_trigger_item_run)

    component = _component(mod, default_parameters={"load_date": "2024-01-01", "mode": "incremental"})
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        run_config={"ops": {asset_def.op.name: {"config": {"parameters": {"mode": "full_reload"}}}}},
        raise_on_error=True,
    )
    assert result.success
    assert captured["parameters"] == {"load_date": "2024-01-01", "mode": "full_reload"}


def test_no_run_config_falls_back_to_static_parameters_unchanged(mod, auth, monkeypatch):
    captured = {}

    def _fake_trigger_item_run(self, context, item_id, item_type, parameters, extra_execution_data=None):
        captured["parameters"] = parameters
        return {"id": "job-1", "status": "Completed"}

    monkeypatch.setattr(mod.FabricWorkspaceComponent, "_trigger_item_run", _fake_trigger_item_run)

    component = _component(mod, default_parameters={"mode": "incremental"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success
    assert captured["parameters"] == {"mode": "incremental"}
