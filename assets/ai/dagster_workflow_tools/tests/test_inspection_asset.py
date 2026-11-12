"""inspection_asset_name: a declare-only asset (the tool set is fixed Python code, not a configurable server, so
there's nothing to materialize) listing the real tool names + descriptions as metadata -- so a UI can show them
without spawning the MCP server to ask."""


def test_inspection_asset_lists_the_real_fixed_tool_set(mod):
    component = mod.DagsterWorkflowToolsComponent(resource_key="wt", definitions_module="fixture_defs", inspection_asset_name="wt_tools")
    defs = component.build_defs(None)
    specs = defs.resolve_all_asset_specs()
    assert len(specs) == 1
    spec = specs[0]
    assert spec.key.to_user_string() == "wt_tools"
    md = {k: getattr(v, "value", v) for k, v in spec.metadata.items()}
    assert md["tool_count"] == 2
    assert set(md["tool_names"]) == {"materialize_assets", "run_job"}
    assert "materializ" in md["tool_descriptions"]["materialize_assets"].lower()
    assert "run" in md["tool_descriptions"]["run_job"].lower()


def test_inspection_asset_respects_allow(mod):
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wt", definitions_module="fixture_defs", inspection_asset_name="wt_tools", allow=["run_job"],
    )
    md = {k: getattr(v, "value", v) for k, v in component.build_defs(None).resolve_all_asset_specs()[0].metadata.items()}
    assert md["tool_names"] == ["run_job"]


def test_no_inspection_asset_by_default(mod):
    component = mod.DagsterWorkflowToolsComponent(resource_key="wt", definitions_module="fixture_defs")
    assert not component.build_defs(None).resolve_all_asset_specs()
