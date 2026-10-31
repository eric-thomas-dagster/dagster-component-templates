"""Real, committed tests for two more gaps a demo-builder reported against
fabric_workspace v1.1.0:

1. `translation:` silently dropped `deps`, `automation_condition`,
   `freshness_policy`, and `description` from the returned AssetSpec --
   `_apply_translation` only ever copied key/metadata/tags/kinds/owners/
   group_name. Setting `automation_condition` in a `translation:` callable
   did nothing, with no error raised. This is also what made
   `upstream_asset_keys` apply uniformly to every imported item with no way
   to vary deps per item -- fixed by the same change (a `translation:`
   callable can now add a per-item dependency).

2. No way to pass job parameters (a notebook load date, a one-off backfill
   override) -- fixed via `default_parameters` (applies to every runnable
   item) and `item_parameters` (per-item override, keyed by display name,
   merged over the default).

3. Two FabricWorkspaceComponent instances over the SAME workspace_id with
   different settings previously hashed to the same state-file key and
   silently shared one state file -- fixed via an optional `key:` field
   folded into the state-key hash.
"""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fabric_workspace_translation", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FabricWorkspaceComponent = _mod.FabricWorkspaceComponent
FabricResource = _mod.FabricResource


def _component(**overrides):
    attrs = dict(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_pipelines=True,
    )
    attrs.update(overrides)
    return FabricWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


def _one_pipeline_state(name="nightly_ingest"):
    return {"pipelines": [{"id": "item-1", "displayName": name, "workspaceId": "ws-1"}]}


def test_translation_can_add_a_per_item_dep_without_losing_upstream_asset_keys(tmp_path):
    def _translate(base_asset_spec, props):
        return base_asset_spec.merge_attributes(deps=[dg.AssetKey(["raw", "orders"])])

    comp = _component(
        upstream_asset_keys=["shared_upstream"],
        translation=_translate,
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    spec = asset_def.get_asset_spec(next(iter(asset_def.keys)))
    dep_keys = {d.asset_key for d in spec.deps}
    assert dg.AssetKey(["shared_upstream"]) in dep_keys  # not lost
    assert dg.AssetKey(["raw", "orders"]) in dep_keys  # added by translation:


def test_translation_automation_condition_is_no_longer_silently_dropped(tmp_path):
    cond = dg.AutomationCondition.eager()

    def _translate(base_asset_spec, props):
        return base_asset_spec.replace_attributes(automation_condition=cond)

    comp = _component(translation=_translate)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    spec = asset_def.get_asset_spec(next(iter(asset_def.keys)))
    assert spec.automation_condition is cond


def test_translation_description_is_applied(tmp_path):
    def _translate(base_asset_spec, props):
        return base_asset_spec.replace_attributes(description="custom description from translation")

    comp = _component(translation=_translate)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    spec = asset_def.get_asset_spec(next(iter(asset_def.keys)))
    assert spec.description == "custom description from translation"


def test_default_parameters_apply_to_every_runnable_item(tmp_path, monkeypatch):
    comp = _component(default_parameters={"load_date": "2024-01-01"})
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))

    captured = {}

    def _fake_trigger(self, context, item_id, item_type, parameters=None):
        captured["parameters"] = parameters
        return {"status": "Completed"}

    monkeypatch.setattr(FabricWorkspaceComponent, "_trigger_item_run", _fake_trigger)
    dg.materialize(list(defs.assets), raise_on_error=True)
    assert captured["parameters"] == {"load_date": "2024-01-01"}


def test_item_parameters_override_default_parameters_for_that_item_only(tmp_path, monkeypatch):
    state = {"pipelines": [
        {"id": "item-1", "displayName": "part_a", "workspaceId": "ws-1"},
        {"id": "item-2", "displayName": "part_b", "workspaceId": "ws-1"},
    ]}
    comp = _component(
        default_parameters={"load_date": "2024-01-01"},
        item_parameters={"part_a": {"load_date": "2024-06-01", "extra": True}},
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, state))

    captured = {}

    def _fake_trigger(self, context, item_id, item_type, parameters=None):
        captured[item_id] = parameters
        return {"status": "Completed"}

    monkeypatch.setattr(FabricWorkspaceComponent, "_trigger_item_run", _fake_trigger)
    dg.materialize(list(defs.assets), raise_on_error=True)

    assert captured["item-1"] == {"load_date": "2024-06-01", "extra": True}
    assert captured["item-2"] == {"load_date": "2024-01-01"}


def test_state_key_disambiguator_separates_two_instances_on_the_same_workspace():
    base = dict(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"))
    comp_a = FabricWorkspaceComponent(**base, key="part_a")
    comp_b = FabricWorkspaceComponent(**base, key="part_b")
    comp_default = FabricWorkspaceComponent(**base)

    key_a = comp_a.defs_state_config.key
    key_b = comp_b.defs_state_config.key
    key_default = comp_default.defs_state_config.key
    assert len({key_a, key_b, key_default}) == 3  # all three distinct, not colliding
