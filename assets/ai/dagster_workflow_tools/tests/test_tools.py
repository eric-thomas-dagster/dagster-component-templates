"""DagsterWorkflowTools: materialize_assets / run_job, each a REAL, synchronous dagster.execute_job
run against a real on-disk DagsterInstance (dagster.execute_job itself is not mocked -- only the
run_config forces in-process execution, a test-only accommodation; see conftest.py)."""
import pytest

from .conftest import IN_PROCESS_RUN_CONFIG


def test_materialize_assets_real_run(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec)
    result = tools.call("materialize_assets", {"asset_keys": ["a", "b"], "run_config": IN_PROCESS_RUN_CONFIG})
    assert result["success"] is True
    assert result["asset_keys"] == ["a", "b"]
    run = instance.get_run_by_id(result["run_id"])
    assert run is not None
    assert run.tags.get("dagster-community/workflow-tool") == "true"


def test_run_job_real_run(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec)
    result = tools.call("run_job", {"job_name": "named_job", "run_config": IN_PROCESS_RUN_CONFIG})
    assert result["success"] is True
    assert result["job_name"] == "named_job"
    run = instance.get_run_by_id(result["run_id"])
    assert run.job_name == "named_job"


def test_a_failing_asset_reports_success_false_not_an_exception(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec)
    result = tools.call("materialize_assets", {"asset_keys": ["boom"], "run_config": IN_PROCESS_RUN_CONFIG})
    assert result["success"] is False  # execute_job(raise_on_error=False is the default) -- never raises up through call()


def test_default_tags_and_caller_tags_both_land_on_the_run(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec, default_tags={"team": "support"})
    result = tools.call("materialize_assets", {
        "asset_keys": ["a"], "run_config": IN_PROCESS_RUN_CONFIG, "tags": {"triggered_by": "agent-123"},
    })
    run = instance.get_run_by_id(result["run_id"])
    assert run.tags.get("team") == "support"
    assert run.tags.get("triggered_by") == "agent-123"
    assert run.tags.get("dagster-community/workflow-tool") == "true"


def test_allow_assets_blocks_unlisted_asset_keys(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec, allow_assets=["a"])
    result = tools.call("materialize_assets", {"asset_keys": ["b"], "run_config": IN_PROCESS_RUN_CONFIG})
    assert "error" in result
    assert "allow_assets" in result["error"]


def test_allow_jobs_blocks_unlisted_job_names(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec, allow_jobs=["some_other_job"])
    result = tools.call("run_job", {"job_name": "named_job", "run_config": IN_PROCESS_RUN_CONFIG})
    assert "error" in result
    assert "allow_jobs" in result["error"]


def test_tool_allow_list_hides_disallowed_tools(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec, allow=["materialize_assets"])
    assert tools.tool_names() == ["materialize_assets"]
    result = tools.call("run_job", {"job_name": "named_job"})
    assert "error" in result and "not enabled" in result["error"]


def test_materialize_assets_requires_non_empty_asset_keys(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec)
    result = tools.call("materialize_assets", {"asset_keys": []})
    assert "error" in result


def test_redacts_secrets_in_error_messages(mod, instance, defs_module_spec):
    tools = mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec)
    result = tools.call("run_job", {"job_name": "nonexistent_job"})
    assert "error" in result  # resolve_job_def raises -> caught, surfaced as data, never raised up


def test_definitions_module_is_required(mod):
    with pytest.raises(ValueError, match="definitions_module"):
        mod.load_definitions("")


def test_unknown_tool_in_allow_raises_at_construction(mod, instance, defs_module_spec):
    with pytest.raises(ValueError, match="unknown tool"):
        mod.DagsterWorkflowTools(instance, definitions_module=defs_module_spec, allow=["not_a_real_tool"])


def test_component_build_defs_registers_resource(mod):
    component = mod.DagsterWorkflowToolsComponent(definitions_module="fixture_defs:defs")
    defs = component.build_defs(context=None)
    assert "dagster_workflow_tools" in defs.resources


def test_component_rejects_unknown_tool_in_allow(mod):
    with pytest.raises(ValueError, match="unknown tool"):
        mod.DagsterWorkflowToolsComponent(definitions_module="fixture_defs:defs", allow=["not_real"]).build_defs(context=None)
