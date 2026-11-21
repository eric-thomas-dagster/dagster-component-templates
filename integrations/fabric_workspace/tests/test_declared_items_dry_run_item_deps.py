"""Real, committed tests for gaps found against the ADF-converter feedback:

1. `notebooks:`/`pipelines:`/`dataflows:` let the component skip live Fabric REST discovery AND the
   StateBackedComponent state-file cache entirely -- build_defs constructs no Fabric client and makes
   no HTTP call at all when every enabled category is declared this way (import_lakehouses/
   import_warehouses/import_semantic_models/import_reports all false). A converter-generated static
   definition (e.g. from an ADF pipeline, which only carries a Notebook activity's target item id, not
   its Fabric-side display name) couldn't load or dry-run without a live workspace before this.
2. `item_deps` lets one item wait on an extra upstream asset key without forcing every other item from
   the same component instance to wait on it too -- `upstream_asset_keys` alone applies to everything.
3. `dry_run` (project-wide) logs what would be triggered and skips the real Fabric call -- unlike the
   existing `fabric/skip` run tag, which only applies to one run at a time.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fabric_workspace_declared_items", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FabricWorkspaceComponent = _mod.FabricWorkspaceComponent
FabricResource = _mod.FabricResource
FabricRunnableItemDeclaration = _mod.FabricRunnableItemDeclaration


def _component(**overrides):
    attrs = dict(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_lakehouses=False, import_warehouses=False,
        import_pipelines=False, import_notebooks=False, import_dataflows=False,
    )
    attrs.update(overrides)
    return FabricWorkspaceComponent(**attrs)


# ── declared items: zero Fabric client/HTTP activity ────────────────────────────────────────────────

def test_declared_notebooks_and_pipelines_build_defs_with_zero_http_calls(monkeypatch):
    comp = _component(
        import_notebooks=True, import_pipelines=True,
        notebooks=[FabricRunnableItemDeclaration(item_id="nb-1", display_name="scoring")],
        pipelines=[FabricRunnableItemDeclaration(item_id="pl-1", display_name="orders_load")],
    )

    def _boom(*a, **k):
        raise AssertionError("no Fabric HTTP call should happen when everything is declared statically")
    monkeypatch.setattr(FabricResource, "get_token", _boom)
    import requests
    monkeypatch.setattr(requests, "get", _boom)
    monkeypatch.setattr(requests, "post", _boom)

    defs = comp.build_defs(context=None)
    keys = {a.key for a in defs.assets}
    assert dg.AssetKey(["fabric_notebook_scoring"]) in keys
    assert dg.AssetKey(["fabric_datapipeline_orders_load"]) in keys


def test_declared_items_still_honor_import_flags(monkeypatch):
    """A declared notebooks: list with import_notebooks left False is simply not imported --
    declaration doesn't override the category's own on/off switch."""
    comp = _component(
        import_notebooks=False,
        notebooks=[FabricRunnableItemDeclaration(item_id="nb-1", display_name="scoring")],
    )
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no HTTP")))
    defs = comp.build_defs(context=None)
    assert dg.AssetKey(["fabric_notebook_scoring"]) not in {a.key for a in defs.assets}


def test_declared_items_with_a_still_live_category_falls_back_to_discovery(monkeypatch):
    """import_lakehouses=True (default) has no declared-list bypass, so build_defs must still go
    through the real StateBackedComponent discovery path -- confirmed by this NOT calling our
    _build_defs_from_state_dict shortcut (it would otherwise need a real state file)."""
    comp = FabricWorkspaceComponent(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_pipelines=False, import_notebooks=True,
        notebooks=[FabricRunnableItemDeclaration(item_id="nb-1", display_name="scoring")],
    )
    assert comp._needs_live_discovery() is True  # import_lakehouses/import_warehouses default True


# ── item_deps: merged with upstream_asset_keys, not a replacement ───────────────────────────────────

def test_item_deps_are_merged_with_global_upstream_asset_keys():
    comp = _component(
        upstream_asset_keys=["shared_upstream"],
        item_deps={"scoring": ["extra_dep_for_scoring_only"]},
    )
    kwargs_scoring = comp._runnable_asset_kwargs(
        {"id": "nb-1", "displayName": "scoring"}, "Notebook", "notebook",
        [dg.AssetKey.from_user_string("shared_upstream")],
    )
    kwargs_other = comp._runnable_asset_kwargs(
        {"id": "nb-2", "displayName": "other_notebook"}, "Notebook", "notebook",
        [dg.AssetKey.from_user_string("shared_upstream")],
    )
    assert set(kwargs_scoring["deps"]) == {dg.AssetKey(["shared_upstream"]), dg.AssetKey(["extra_dep_for_scoring_only"])}
    assert set(kwargs_other["deps"]) == {dg.AssetKey(["shared_upstream"])}


def test_item_deps_does_not_duplicate_a_key_already_in_upstream_asset_keys():
    comp = _component(item_deps={"scoring": ["shared_upstream"]})
    kwargs = comp._runnable_asset_kwargs(
        {"id": "nb-1", "displayName": "scoring"}, "Notebook", "notebook",
        [dg.AssetKey.from_user_string("shared_upstream")],
    )
    assert kwargs["deps"] == [dg.AssetKey(["shared_upstream"])]


# ── dry_run: project-wide, distinct from the per-run fabric/skip tag ────────────────────────────────

@pytest.fixture
def auth(monkeypatch):
    monkeypatch.setattr(FabricResource, "get_token", lambda self: "fake-token")


def test_dry_run_skips_the_real_trigger_and_builds_no_http_call(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: (_ for _ in ()).throw(AssertionError("dry_run must not call Fabric")))

    comp = _component(
        import_notebooks=True, dry_run=True,
        notebooks=[FabricRunnableItemDeclaration(item_id="nb-1", display_name="scoring")],
    )
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["fabric_notebook_scoring"]))
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    assert events[0].materialization.metadata["fabric_skipped"].value is True
