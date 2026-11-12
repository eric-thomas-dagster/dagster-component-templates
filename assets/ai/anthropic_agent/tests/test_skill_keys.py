"""skill_keys on AnthropicAgentComponent: a PARALLEL, independent mechanism to mcp_servers for
making Agent Skills (resources/skill_resource -- SkillResourceComponent/SkillResource) available
to this agent. Same getattr(context.resources, key) runtime-resolution convention as
server_resource_key (see test_server_resource_key.py), merged into the same tools list / tool-
dispatch loop as the MCP tools, using the exact same Anthropic {name, description, input_schema}
tool shape.

Builds a tiny REAL local skill fixture inline (via tmp_path) and registers it through the REAL
SkillResourceComponent (loaded via importlib, same convention as conftest.load_component_module)
-- not resources/skill_resource/tests/fixtures/, to keep this component's own tests self-
contained.
"""
import importlib.util
import sys
import types
from pathlib import Path

import dagster as dg
import pytest

from .conftest import load_component_module


def _load_skill_resource_module():
    path = (
        Path(__file__).resolve().parents[4]
        / "resources" / "skill_resource" / "component.py"
    )
    spec = importlib.util.spec_from_file_location(
        "skill_resource_component_under_test_for_anthropic_agent", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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
def skill_dir(tmp_path):
    d = tmp_path / "pdf-report"
    d.mkdir()
    (d / "SKILL.md").write_text(SAMPLE_SKILL_MD)
    (d / "reference.md").write_text("extra reference material")
    return d


@pytest.fixture()
def skill_resource(skill_dir):
    skill_resource_mod = _load_skill_resource_module()
    component = skill_resource_mod.SkillResourceComponent(
        resource_key="pdf_skill", local_path=str(skill_dir)
    )
    return component.build_defs(None).resources["pdf_skill"]


def _run(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


# ---- required_resource_keys / tag (no materialize needed) --------------------------------------

def test_skill_keys_resolves_into_required_resource_keys(mod):
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    assert "pdf_skill" in asset_def.required_resource_keys


def test_required_resource_keys_unions_skills_and_mcp_servers(mod):
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi",
        skill_keys=["pdf_skill"],
        mcp_servers=[{"server_resource_key": "shared_mcp"}],
    )
    asset_def = component.build_defs(None).assets[0]
    assert {"pdf_skill", "shared_mcp"} <= asset_def.required_resource_keys


def test_skill_tag_set_to_comma_joined_skill_keys_when_configured(mod):
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_skill", "other_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    # Dagster's strict tag-value validation forbids ',' -- it's swapped for '-' (same
    # sanitization as the sibling *_agent components done alongside this one).
    assert tags["dagster-community/skill"] == "pdf_skill-other_skill"


def test_skill_tag_absent_when_not_configured(mod):
    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="hi")
    asset_def = component.build_defs(None).assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert "dagster-community/skill" not in tags


def test_skill_keys_csv_string_is_coerced_to_a_list(mod):
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys="pdf_skill, other_skill",
    )
    assert component.skill_keys == ["pdf_skill", "other_skill"]


# ---- unresolvable skill_keys entry raises, naming the key ---------------------------------------

def test_missing_skill_key_resource_raises_naming_the_key(mod):
    """skill_keys references a resource_key that nothing registers -- no resource bound under
    that key at all. required_resource_keys includes it, so Dagster's own resource-binding
    validation (not this component's code) is what fires here -- but it still raises and still
    names the key, which is the end-to-end guarantee: a misconfigured skill_keys entry can never
    silently no-op."""
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["missing_skill"],
    )
    with pytest.raises(Exception, match="missing_skill"):
        _run(component)


def test_resource_without_describe_or_load_is_rejected_naming_the_key(mod):
    """A resource IS bound under the key (satisfying Dagster's own required_resource_keys
    validation) but isn't a SkillResource -- this is this component's own runtime check
    (getattr(context.resources, key) succeeds, then the describe()/load() duck-type check
    fails), raised at the very start of the asset body, before any model call."""
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["not_a_skill"],
    )
    with pytest.raises(Exception, match=r"not_a_skill.*not a SkillResource"):
        _run(component, resources={"not_a_skill": object()})


# ---- skills_available metadata + the load_skill/read_skill_file tool-use loop -------------------

def _fake_anthropic_module(turns):
    """turns: list of callables, each returning a types.SimpleNamespace(content=[...], stop_reason=...)
    for the Nth call. Mirrors test_mcp_real_server.py's `drive` fake-client convention."""
    state = {"n": 0}

    class _Messages:
        async def create(self, **kw):
            i = state["n"]
            state["n"] += 1
            return turns[i](kw)

    class AsyncAnthropic:
        def __init__(self, **kw):
            self.messages = _Messages()

    fake = types.ModuleType("anthropic")
    fake.AsyncAnthropic = AsyncAnthropic
    return fake


def test_skills_available_metadata_and_load_skill_tool_use_loop(mod, skill_resource, monkeypatch):
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="use the skill", skill_keys=["pdf_skill"],
    )

    seen_tools = {}

    def turn1(kw):
        seen_tools["tools"] = kw["tools"]
        seen_tools["system"] = kw.get("system")
        blk = types.SimpleNamespace(
            type="tool_use", id="tu1", name="load_skill", input={"name": "pdf-report"}
        )
        return types.SimpleNamespace(content=[blk], stop_reason="tool_use")

    def turn2(kw):
        blk = types.SimpleNamespace(type="text", text="done")
        return types.SimpleNamespace(content=[blk], stop_reason="end_turn")

    monkeypatch.setitem(sys.modules, "anthropic", _fake_anthropic_module([turn1, turn2]))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    result = _run(component, resources={"pdf_skill": skill_resource})
    assert result.success

    # The tool list sent to the model includes load_skill/read_skill_file, Anthropic-native shape.
    tool_names = {t["name"] for t in seen_tools["tools"]}
    assert {"load_skill", "read_skill_file"} <= tool_names
    load_skill_tool = next(t for t in seen_tools["tools"] if t["name"] == "load_skill")
    assert load_skill_tool["input_schema"]["required"] == ["name"]
    read_skill_file_tool = next(t for t in seen_tools["tools"] if t["name"] == "read_skill_file")
    assert set(read_skill_file_tool["input_schema"]["required"]) == {"name", "relative_path"}

    # The skill's name/description are in the system prompt up front (turn 1), before any load.
    assert "pdf-report" in seen_tools["system"]
    assert "Generate a formatted PDF report from a DataFrame." in seen_tools["system"]

    md = result.get_asset_materialization_events()[-1].materialization.metadata
    skills_available = md["skills_available"].data
    assert skills_available == [
        {"key": "pdf_skill", "name": "pdf-report", "description": "Generate a formatted PDF report from a DataFrame."}
    ]
    assert md["skills_loaded"].data == ["pdf-report"]
    assert md["skill_load_count"].value == 1


def test_read_skill_file_does_not_increment_skill_load_count(mod, skill_resource, monkeypatch):
    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="use the skill", skill_keys=["pdf_skill"],
    )

    def turn1(kw):
        blk = types.SimpleNamespace(
            type="tool_use", id="tu1", name="read_skill_file",
            input={"name": "pdf-report", "relative_path": "reference.md"},
        )
        return types.SimpleNamespace(content=[blk], stop_reason="tool_use")

    def turn2(kw):
        blk = types.SimpleNamespace(type="text", text="done")
        return types.SimpleNamespace(content=[blk], stop_reason="end_turn")

    monkeypatch.setitem(sys.modules, "anthropic", _fake_anthropic_module([turn1, turn2]))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    result = _run(component, resources={"pdf_skill": skill_resource})
    assert result.success

    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["skill_load_count"].value == 0
    assert md["skills_loaded"].data == []


def test_no_skill_metadata_when_skill_keys_not_configured(mod, monkeypatch):
    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="hi")

    def turn1(kw):
        blk = types.SimpleNamespace(type="text", text="done")
        return types.SimpleNamespace(content=[blk], stop_reason="end_turn")

    monkeypatch.setitem(sys.modules, "anthropic", _fake_anthropic_module([turn1]))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    result = _run(component)
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "skills_available" not in md
    assert "skills_loaded" not in md
    assert "skill_load_count" not in md
