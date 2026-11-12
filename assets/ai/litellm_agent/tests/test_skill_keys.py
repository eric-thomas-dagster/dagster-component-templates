"""skill_keys on LiteLLMAgentComponent: a PARALLEL, independent mechanism to mcp_servers
for the shared SkillResource (registered by SkillResourceComponent, resources/skill_resource).
Each listed skill's name/description is shown to the model up front; the full SKILL.md
only loads if the model actually calls load_skill -- the same progressive-disclosure shape
Claude Code's own Skill tool uses.

A tiny REAL local skill fixture (SKILL.md + one bundled file) is built inline with
tmp_path and registered through the REAL SkillResourceComponent/SkillResource (loaded the
same importlib-by-file-path way resources/skill_resource/tests/conftest.py loads its own
component.py) -- this component's tests stay self-contained and don't reach into
resources/skill_resource/tests/fixtures/. Only the paid model call (litellm.acompletion)
is faked, mirroring tests/test_mcp_real_server.py's `drive()` pattern.
"""
import importlib.util
import json
import sys
import types
from pathlib import Path

import dagster as dg
import pytest

from .conftest import load_component_module

SKILL_MD = """---
name: pdf-report
description: Generate a formatted PDF report from a DataFrame.
---

## Instructions

1. Load the DataFrame.
2. Render it as a styled table.
3. Export to PDF.
"""

BUNDLED_FILE_NAME = "template.txt"
BUNDLED_FILE_CONTENT = "Report template: {title}\n"


def _load_skill_resource_module():
    here = Path(__file__).resolve()
    repo_root = here.parents[4]  # .../dagster-component-templates
    path = repo_root / "resources" / "skill_resource" / "component.py"
    spec = importlib.util.spec_from_file_location("_skill_resource_for_litellm_agent_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def skill_resource_mod():
    return _load_skill_resource_module()


@pytest.fixture()
def skill_dir(tmp_path):
    d = tmp_path / "pdf-report-skill"
    d.mkdir()
    (d / "SKILL.md").write_text(SKILL_MD)
    (d / BUNDLED_FILE_NAME).write_text(BUNDLED_FILE_CONTENT)
    return d


@pytest.fixture()
def skill_resource(skill_resource_mod, skill_dir):
    """A REAL SkillResource, built the same way SkillResourceComponent.build_defs builds
    one -- registered under resource_key 'pdf_report_skill'."""
    component = skill_resource_mod.SkillResourceComponent(
        resource_key="pdf_report_skill", local_path=str(skill_dir),
    )
    defs = component.build_defs(None)
    return defs.resources["pdf_report_skill"]


def _run(assets, resources):
    return dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager(), **resources})


class _Msg:
    def __init__(self, content, tool_calls=None):
        self.content, self.tool_calls = content, tool_calls

    def model_dump(self, exclude_none=True):
        d = {"role": "assistant", "content": self.content}
        return {k: v for k, v in d.items() if v is not None}


def _fake_litellm_load_skill_then_answer(seen, skill_name="pdf-report"):
    """turn 1: tool-call for load_skill(name=skill_name). turn 2: final text."""
    state = {"n": 0}

    async def acompletion(messages, **kw):
        state["n"] += 1
        seen["last_tools"] = kw.get("tools")
        seen["last_messages"] = messages
        if state["n"] == 1:
            tc = types.SimpleNamespace(
                id="call_1", type="function",
                function=types.SimpleNamespace(name="load_skill", arguments=json.dumps({"name": skill_name})),
            )
            return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg(None, [tc]))])
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg("done, used the skill"))])

    return acompletion


def _install_fake_litellm(monkeypatch, acompletion):
    fake = types.ModuleType("litellm")
    fake.acompletion = acompletion
    fake.drop_params = False
    monkeypatch.setitem(sys.modules, "litellm", fake)


# ---------------------------------------------------------------------------------------


def test_skill_keys_flows_into_required_resource_keys(mod):
    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    assert "pdf_report_skill" in asset_def.required_resource_keys


def test_no_skill_keys_means_no_skill_tag_and_no_extra_resource_keys(mod):
    component = mod.LiteLLMAgentComponent(asset_name="agent_out", prompt="hi")
    asset_def = component.build_defs(None).assets[0]
    assert asset_def.required_resource_keys <= {"io_manager"}
    key = dg.AssetKey.from_user_string("agent_out")
    assert "dagster-community/skill" not in asset_def.specs_by_key[key].tags


def test_skill_tag_is_comma_joined_skill_keys(mod):
    """Dagster's own strict asset-tag-value validation disallows ',' (alpha-numeric/_/-/.
    only, <=63 chars) -- so the tag value is the comma-joined skill_keys with any
    disallowed character (the commas themselves) swapped for '-', not a literal join."""
    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill", "other_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    key = dg.AssetKey.from_user_string("agent_out")
    assert asset_def.specs_by_key[key].tags["dagster-community/skill"] == "pdf_report_skill-other_skill"


def test_unresolvable_skill_key_raises_naming_the_key(mod, monkeypatch):
    """No resource at all is registered under 'does_not_exist'. Because skill_keys feeds
    into required_resource_keys, Dagster's own job-construction-time resource-requirement
    check (earlier than this component's own runtime code) is what actually raises here --
    it still fails loudly and names the exact key, which is the behavior this test cares
    about. The complementary case -- a resource IS registered under the key, but isn't a
    SkillResource -- is covered by test_skill_resource_registered_under_wrong_shape_raises,
    which exercises this component's own getattr/describe()/load() check."""
    monkeypatch.setattr(mod, "_run_agent", lambda **kw: (_ for _ in ()).throw(AssertionError("should not be called")))
    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["does_not_exist"],
    )
    with pytest.raises((ValueError, dg.DagsterInvalidDefinitionError), match="does_not_exist"):
        dg.materialize(
            component.build_defs(None).assets,
            resources={"io_manager": dg.InMemoryIOManager()},
        )


def test_skill_resource_registered_under_wrong_shape_raises(mod):
    class NotASkill:
        pass

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["bogus"],
    )
    with pytest.raises(ValueError, match="bogus") as exc_info:
        dg.materialize(
            component.build_defs(None).assets,
            resources={"io_manager": dg.InMemoryIOManager(), "bogus": NotASkill()},
        )
    assert "not a SkillResource" in str(exc_info.value)


def test_skills_available_metadata_reflects_configured_skill(mod, skill_resource, monkeypatch):
    async def fake_run_agent(*, skill_resources, **kwargs):
        return {
            "final_answer": "ok", "iterations": 1, "tool_calls_made": 0,
            "tool_call_details": [], "transcript": [], "stopped_reason": "final_answer",
            "model": "m", "mcp_servers": [],
        }

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"],
    )
    result = _run(component.build_defs(None).assets, {"pdf_report_skill": skill_resource})
    assert result.success
    mats = result.asset_materializations_for_node("agent_out")
    md = mats[0].metadata
    available = md["skills_available"].value
    assert available == [{
        "key": "pdf_report_skill", "name": "pdf-report",
        "description": "Generate a formatted PDF report from a DataFrame.",
    }]


def test_full_loop_load_skill_then_final_answer(mod, skill_resource, monkeypatch):
    """Real _run_agent loop (only litellm.acompletion faked): turn 1 is a load_skill tool
    call, turn 2 is the final text answer. Asserts the loop continues correctly past the
    tool call, and that skills_loaded / skill_load_count end up right in both the raw
    result dict and the asset's output metadata."""
    seen = {}
    _install_fake_litellm(monkeypatch, _fake_litellm_load_skill_then_answer(seen))

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="make me a report",
        system_prompt="You are a helpful assistant.",
        skill_keys=["pdf_report_skill"], api_key_env_var=None,
    )
    result = _run(component.build_defs(None).assets, {"pdf_report_skill": skill_resource})
    assert result.success

    output = result.output_for_node("agent_out")
    assert output["stopped_reason"] == "final_answer"
    assert output["final_answer"] == "done, used the skill"
    assert output["loaded_skill_names"] == ["pdf-report"]
    assert output["skill_load_count"] == 1
    # load_skill's result (the full SKILL.md content) was fed back as the tool message.
    load_call = output["tool_call_details"][0]
    assert load_call["tool"] == "load_skill"
    assert load_call["is_error"] is False
    assert "Render it as a styled table" in load_call["result_preview"]

    mats = result.asset_materializations_for_node("agent_out")
    md = mats[0].metadata
    assert md["skills_loaded"].value == ["pdf-report"]
    assert md["skill_load_count"].value == 1

    # The skill's name/description were visible to the model on turn 1, before it chose
    # to call load_skill -- i.e. in the system message of the FIRST completion call.
    first_system_msg = next(m for m in seen["last_messages"] if m.get("role") == "system")
    # (last_messages is overwritten each turn; re-derive turn 1's system message from the
    # initial call is awkward here, so instead assert the loop's own transcript carries it.)
    transcript_system = next(m for m in output["transcript"] if m.get("role") == "system")
    assert "pdf-report" in transcript_system["content"]
    assert "Generate a formatted PDF report" in transcript_system["content"]

    # load_skill / read_skill_file were offered as tools alongside (none, here) MCP tools.
    tool_names = {t["function"]["name"] for t in seen["last_tools"]}
    assert {"load_skill", "read_skill_file"} <= tool_names


def test_read_skill_file_returns_bundled_file_content(mod, skill_resource, monkeypatch):
    seen = {}
    state = {"n": 0}

    async def acompletion(messages, **kw):
        state["n"] += 1
        seen["last_tools"] = kw.get("tools")
        if state["n"] == 1:
            tc = types.SimpleNamespace(
                id="call_1", type="function",
                function=types.SimpleNamespace(
                    name="read_skill_file",
                    arguments=json.dumps({"name": "pdf-report", "relative_path": BUNDLED_FILE_NAME}),
                ),
            )
            return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg(None, [tc]))])
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg("got the template"))])

    _install_fake_litellm(monkeypatch, acompletion)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"], api_key_env_var=None,
    )
    result = _run(component.build_defs(None).assets, {"pdf_report_skill": skill_resource})
    assert result.success
    output = result.output_for_node("agent_out")

    read_call = output["tool_call_details"][0]
    assert read_call["tool"] == "read_skill_file"
    assert read_call["is_error"] is False
    assert "Report template" in read_call["result_preview"]
    # read_skill_file does NOT count toward skill_load_count -- only load_skill does.
    assert output["skill_load_count"] == 0
    assert output["loaded_skill_names"] == []


def test_read_skill_file_escaping_path_is_reported_as_error_not_a_crash(mod, skill_resource, monkeypatch):
    state = {"n": 0}

    async def acompletion(messages, **kw):
        state["n"] += 1
        if state["n"] == 1:
            tc = types.SimpleNamespace(
                id="call_1", type="function",
                function=types.SimpleNamespace(
                    name="read_skill_file",
                    arguments=json.dumps({"name": "pdf-report", "relative_path": "../../etc/passwd"}),
                ),
            )
            return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg(None, [tc]))])
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg("handled"))])

    _install_fake_litellm(monkeypatch, acompletion)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"], api_key_env_var=None,
    )
    result = _run(component.build_defs(None).assets, {"pdf_report_skill": skill_resource})
    assert result.success  # the loop itself must not crash
    output = result.output_for_node("agent_out")
    bad_call = output["tool_call_details"][0]
    assert bad_call["is_error"] is True
    assert "escapes the skill folder" in bad_call["result_preview"]


def test_load_skill_unknown_name_is_reported_as_error_not_a_crash(mod, skill_resource, monkeypatch):
    state = {"n": 0}

    async def acompletion(messages, **kw):
        state["n"] += 1
        if state["n"] == 1:
            tc = types.SimpleNamespace(
                id="call_1", type="function",
                function=types.SimpleNamespace(name="load_skill", arguments=json.dumps({"name": "nonexistent-skill"})),
            )
            return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg(None, [tc]))])
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg("handled"))])

    _install_fake_litellm(monkeypatch, acompletion)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"], api_key_env_var=None,
    )
    result = _run(component.build_defs(None).assets, {"pdf_report_skill": skill_resource})
    assert result.success
    output = result.output_for_node("agent_out")
    bad_call = output["tool_call_details"][0]
    assert bad_call["tool"] == "load_skill"
    assert bad_call["is_error"] is True
    assert "nonexistent-skill" in bad_call["result_preview"]
    assert output["skill_load_count"] == 0


def test_skill_keys_csv_string_coerced_to_list(mod):
    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys="pdf_report_skill, other_skill",
    )
    assert component.skill_keys == ["pdf_report_skill", "other_skill"]
