"""skill_keys: a PARALLEL mechanism to mcp_servers/server_resource_key -- lists SkillResource
resource_keys (from the real SkillResourceComponent) whose name/description get injected into
the agent's instructions up front, and whose full SKILL.md only loads when the model calls the
new `load_skill` tool (mirroring MCP's own progressive-disclosure story). Only the OpenAI client
call is faked (same `_Completions.create` fake-client pattern test_mcp_real_server.py already
uses); the real SkillResourceComponent is loaded via importlib the same way this test dir already
loads component.py (see conftest.load_component_module), against a tiny real local SKILL.md
fixture built inline with tmp_path -- not resources/skill_resource/tests/fixtures/, per this
repo's convention that each component's tests stay self-contained.
"""
import importlib.util
import json
import sys
import types
from pathlib import Path

import dagster as dg
import pytest

from .conftest import load_component_module


def _load_skill_resource_module():
    here = Path(__file__).resolve().parent.parent.parent.parent.parent
    path = here / "resources" / "skill_resource" / "component.py"
    spec = importlib.util.spec_from_file_location("_skill_resource_for_openai_agent_test", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


SAMPLE_SKILL_MD = """---
name: pdf-report
description: Generate a formatted PDF report from a DataFrame.
---

## Instructions

1. Load the DataFrame.
2. Render it as a styled table.
3. Export to PDF.
"""


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def skill_resource(tmp_path):
    """A real SkillResource, built via the real SkillResourceComponent against a tiny real
    local skill folder (SKILL.md + one bundled file) -- not a hand-rolled fake."""
    skill_dir = tmp_path / "pdf_report_skill"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(SAMPLE_SKILL_MD)
    (skill_dir / "template.html").write_text("<html>report template</html>")

    skill_resource_mod = _load_skill_resource_module()
    component = skill_resource_mod.SkillResourceComponent(
        resource_key="pdf_skill", local_path=str(skill_dir)
    )
    defs = component.build_defs(None)
    return defs.resources["pdf_skill"]


def _run(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


def test_skill_keys_in_required_resource_keys(mod):
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    assert "pdf_skill" in asset_def.required_resource_keys


def test_skill_keys_accepts_csv_string(mod):
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys="pdf_skill, other_skill",
    )
    assert component.skill_keys == ["pdf_skill", "other_skill"]


def test_unresolvable_skill_key_raises_naming_the_key(mod):
    # skill_keys feeds required_resource_keys, so when NOTHING is registered under that
    # key at all, Dagster's own resource-requirement check fires first (at Definitions/
    # materialize construction, before the op body ever runs) -- still naming the key.
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["does_not_exist"],
    )
    with pytest.raises(dg.DagsterInvalidDefinitionError, match="does_not_exist"):
        _run(component)


def test_skill_resource_key_pointing_at_non_skill_resource_raises(mod):
    class NotASkill(dg.ConfigurableResource):
        pass

    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill"],
    )
    with pytest.raises(ValueError, match="not a SkillResource"):
        _run(component, resources={"pdf_skill": NotASkill()})


def test_tag_set_when_skill_keys_configured(mod):
    # Dagster's own strict tag-value validation (alpha-numeric/'_'/'-'/'.' only, <=63
    # chars) rejects a literal comma -- the component swaps ',' for '-' so a real
    # multi-skill config doesn't crash build_defs with DagsterInvalidDefinitionError.
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill", "other_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    key = dg.AssetKey.from_user_string("agent_out")
    tags = asset_def.specs_by_key[key].tags
    assert tags.get("dagster-community/skill") == "pdf_skill-other_skill"


def test_tag_absent_when_no_skill_keys(mod):
    component = mod.OpenAIAgentComponent(asset_name="agent_out", prompt="hi")
    asset_def = component.build_defs(None).assets[0]
    key = dg.AssetKey.from_user_string("agent_out")
    tags = asset_def.specs_by_key[key].tags
    assert "dagster-community/skill" not in tags


class _FnCallItem:
    def __init__(self, call_id, name, arguments):
        self.type = "function_call"
        self.call_id = call_id
        self.name = name
        self.arguments = arguments

    def model_dump(self, exclude_none=True):
        return {"type": "function_call", "call_id": self.call_id, "name": self.name, "arguments": self.arguments}


class _MessageItem:
    def __init__(self, text):
        self.type = "message"
        self._text = text

    def model_dump(self, exclude_none=True):
        return {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": self._text, "annotations": []}]}


def _fake_response(output_items, output_text):
    return types.SimpleNamespace(output=output_items, output_text=output_text)


def _fake_openai_module(monkeypatch, tool_name, tool_args):
    """Turn 1: model emits a tool call for `tool_name`. Turn 2: model returns final text.
    Mirrors test_mcp_real_server.py's own `drive()` fake-client shape exactly."""
    state = {"n": 0}
    seen = {}

    class _Responses:
        async def create(self, **kw):
            state["n"] += 1
            seen["tools"] = kw.get("tools")
            if state["n"] == 1:
                return _fake_response([_FnCallItem("c1", tool_name, json.dumps(tool_args))], None)
            return _fake_response([_MessageItem("Done, used the skill.")], "Done, used the skill.")

    class AsyncOpenAI:
        def __init__(self, **kw):
            self.responses = _Responses()

    fake = types.ModuleType("openai")
    fake.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("FAKE_OPENAI_KEY", "sk-fake")
    return seen


def test_load_skill_tool_call_loop(mod, skill_resource, monkeypatch):
    seen = _fake_openai_module(monkeypatch, "load_skill", {"name": "pdf-report"})

    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="Make me a pdf report.", skill_keys=["pdf_skill"],
        api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"pdf_skill": skill_resource})
    assert result.success

    mat = result.asset_value("agent_out")
    assert mat["stopped_reason"] == "final_answer"
    assert mat["loaded_skill_names"] == ["pdf-report"]
    assert mat["skill_load_count"] == 1

    # The first (turn 1) model call already saw load_skill/read_skill_file as tools.
    tool_names = {t["name"] for t in seen["tools"]}
    assert "load_skill" in tool_names
    assert "read_skill_file" in tool_names


def test_skills_metadata_on_materialization(mod, skill_resource, monkeypatch):
    _fake_openai_module(monkeypatch, "load_skill", {"name": "pdf-report"})

    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="Make me a pdf report.", skill_keys=["pdf_skill"],
        api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"pdf_skill": skill_resource})
    metadata = result.get_asset_materialization_events()[-1].materialization.metadata

    available = metadata["skills_available"].value
    assert available[0]["name"] == "pdf-report"
    assert available[0]["description"] == "Generate a formatted PDF report from a DataFrame."
    assert metadata["skills_loaded"].value == ["pdf-report"]
    assert metadata["skill_load_count"].value == 1


def _fake_openai_module_no_tools(monkeypatch):
    """No tool call at all -- the model answers directly on turn 1."""
    class _Responses:
        async def create(self, **kw):
            return _fake_response([_MessageItem("hi there")], "hi there")

    class AsyncOpenAI:
        def __init__(self, **kw):
            self.responses = _Responses()

    fake = types.ModuleType("openai")
    fake.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("FAKE_OPENAI_KEY", "sk-fake")


def test_skills_metadata_absent_without_skill_keys(mod, monkeypatch):
    _fake_openai_module_no_tools(monkeypatch)
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component)
    metadata = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "skills_available" not in metadata
    assert "skills_loaded" not in metadata
    assert "skill_load_count" not in metadata


def test_read_skill_file_does_not_increment_load_count(mod, skill_resource, monkeypatch):
    _fake_openai_module(monkeypatch, "read_skill_file", {"name": "pdf-report", "relative_path": "template.html"})

    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill"],
        api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"pdf_skill": skill_resource})
    assert result.success
    mat = result.asset_value("agent_out")
    assert mat["skill_load_count"] == 0
    assert mat["loaded_skill_names"] == []
    detail = mat["tool_call_details"][0]
    assert detail["tool"] == "read_skill_file"
    assert detail["is_error"] is False
    assert "report template" in detail["result_preview"]


# --- a skill needing code execution is omitted from the listing/tools, not silently presented ---

def _fake_openai_module_capture(monkeypatch):
    seen = {}

    class _Responses:
        async def create(self, **kw):
            seen["tools"] = kw.get("tools")
            seen["instructions"] = kw.get("instructions")
            return _fake_response([_MessageItem("done")], "done")

    class AsyncOpenAI:
        def __init__(self, **kw):
            self.responses = _Responses()

    fake = types.ModuleType("openai")
    fake.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("FAKE_OPENAI_KEY", "sk-fake")
    return seen


@pytest.fixture()
def code_skill_resource(tmp_path):
    skill_dir = tmp_path / "chart_skill"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(
        "---\nname: chart-skill\ndescription: Makes charts via a bundled script.\n---\n\n"
        "## Instructions\n\nRun the script.\n"
    )
    (skill_dir / "scripts").mkdir()
    (skill_dir / "scripts" / "make_chart.py").write_text("print('hi')")
    skill_resource_mod = _load_skill_resource_module()
    component = skill_resource_mod.SkillResourceComponent(resource_key="chart_skill", local_path=str(skill_dir))
    return component.build_defs(None).resources["chart_skill"]


def test_code_execution_skill_omitted_from_listing_and_tools(mod, code_skill_resource, monkeypatch):
    seen = _fake_openai_module_capture(monkeypatch)
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["chart_skill"], api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"chart_skill": code_skill_resource})
    assert result.success
    tool_names = {t["name"] for t in (seen.get("tools") or [])}
    assert "load_skill" not in tool_names


def test_mixed_skills_listing_only_shows_the_runnable_one(mod, skill_resource, code_skill_resource, monkeypatch):
    seen = _fake_openai_module_capture(monkeypatch)
    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill", "chart_skill"], api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"pdf_skill": skill_resource, "chart_skill": code_skill_resource})
    assert result.success
    tool_names = {t["name"] for t in (seen.get("tools") or [])}
    assert "load_skill" in tool_names
    instructions = seen.get("instructions") or ""
    assert "pdf-report" in instructions
    assert "chart-skill" not in instructions
