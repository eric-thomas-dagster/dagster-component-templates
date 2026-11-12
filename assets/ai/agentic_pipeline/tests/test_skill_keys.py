"""`skill_keys` on `tool_use_loop` steps: lets a step reference a shared SkillResource
(registered once via resources/skill_resource's SkillResourceComponent) instead of a
single top-level field -- same per-step shape as `mcp_servers`/`server_resource_key`,
because `tool_use_loop` is the only op type with a real turn-by-turn loop where a model
can actually "choose" to load a skill (`mcp_call` is a single non-looping call).

This is the real end-to-end test: a REAL SkillResourceComponent-backed resource (built
from a real `SKILL.md` with real YAML front matter, written under `tmp_path` -- nothing
about the skill side is mocked), run through the actual `tool_use_loop` with a FAKE
LLM client (this test suite's existing `fake_litellm` pattern) and a mocked MCP
discover/call (this test suite's existing `tool_use_loop_capture`-style pattern from
test_mcp_server_resource_key.py -- `tool_use_loop` requires `mcp_servers: [...]` even
when the point of the test is skills, not MCP).
"""
import logging
import sys
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import tool_call

SKILL_MD = """---
name: pdf-report-skill
description: Generate a formatted PDF report from tabular data.
---

# PDF Report Skill

1. Load the data.
2. Format it into a report.
3. Render to PDF.
"""


@pytest.fixture
def skill_dir(tmp_path):
    d = tmp_path / "pdf_report_skill"
    d.mkdir()
    (d / "SKILL.md").write_text(SKILL_MD)
    (d / "template.txt").write_text("report template")
    return d


@pytest.fixture
def skill_resource(skill_dir):
    from resources.skill_resource.component import SkillResourceComponent

    component = SkillResourceComponent(resource_key="pdf_skill", local_path=str(skill_dir))
    defs = component.build_defs(None)
    return defs.resources["pdf_skill"]


@pytest.fixture
def tool_use_loop_capture(mod, monkeypatch):
    seen = []

    async def discover(log, servers):
        seen.append(list(servers))
        return [{"server": s.get("name") or "server", "name": "echo", "description": "",
                  "input_schema": {"type": "object", "properties": {}}} for s in servers]

    async def call(**kw):
        return {"raw": "ok", "kind": "text"}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return seen


def _ctx_with_resources(**resources):
    return SimpleNamespace(log=logging.getLogger("agentic_test"), resources=SimpleNamespace(**resources))


def test_do_tool_use_loop_resolves_real_skill_and_loads_real_skill_md(mod, fake_litellm, tool_use_loop_capture, skill_resource):
    from .conftest import new_state

    fake_litellm.reply("", tool_calls=[tool_call("load_skill", {"name": "pdf-report-skill"}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])

    ctx = _ctx_with_resources(pdf_skill=skill_resource)
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["pdf_skill"],
    }
    out = mod._do_tool_use_loop(step, new_state(), ctx)

    assert out["text"] == "done"
    assert out["skills_loaded"] == ["pdf-report-skill"]
    assert out["skill_load_count"] == 1
    assert out["skills_available"] == [
        {"key": "pdf_skill", "name": "pdf-report-skill", "description": "Generate a formatted PDF report from tabular data."}
    ]

    # The model-facing tool result for load_skill is the real SKILL.md content
    # (front matter + full body), not just the cheap name/description.
    second_call_messages = fake_litellm.calls[1]["messages"]
    tool_messages = [m for m in second_call_messages if m.get("role") == "tool"]
    assert any("# PDF Report Skill" in m["content"] and "Render to PDF" in m["content"] for m in tool_messages)

    # System prompt lists the skill's name + description up front.
    system_msg = fake_litellm.calls[0]["messages"][0]
    assert "pdf-report-skill" in str(system_msg["content"])
    assert "Generate a formatted PDF report" in str(system_msg["content"])


def test_skill_keys_unknown_resource_key_raises_naming_the_key(mod, fake_litellm):
    from .conftest import new_state

    ctx = _ctx_with_resources()
    step = {
        "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
        "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        "skill_keys": ["missing_skill"],
    }
    with pytest.raises(ValueError, match="missing_skill"):
        mod._do_tool_use_loop(step, new_state(), ctx)


def test_pipeline_asset_requires_skill_resource_key_materializes_and_tags(mod, fake_litellm, tool_use_loop_capture, skill_resource):
    fake_litellm.reply("", tool_calls=[tool_call("load_skill", {"name": "pdf-report-skill"}, id="t1")])
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])

    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p",
        source={"kind": "literal", "text": "doc"},
        steps=[{
            "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
            "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
            "skill_keys": ["pdf_skill"],
        }],
        outputs={"assets": ["agent"]},
    )
    asset_def = component.build_defs(None).assets[0]
    assert "pdf_skill" in asset_def.required_resource_keys

    key = dg.AssetKey(["p_agent"])
    tags = asset_def.specs_by_key[key].tags
    assert tags["dagster-community/skill"] == "pdf_skill"

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager(), "pdf_skill": skill_resource},
        raise_on_error=True,
    )
    assert result.success

    mat_events = [e for e in result.all_events if e.is_step_materialization]
    md = mat_events[0].materialization.metadata
    assert md["agent__skill_load_count"].value == 1
    assert md["agent__skills_loaded"].data == ["pdf-report-skill"]


def test_pipeline_asset_no_skill_tag_or_metadata_when_not_configured(mod, fake_litellm, tool_use_loop_capture):
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "done"}, id="fin")])

    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p",
        source={"kind": "literal", "text": "doc"},
        steps=[{
            "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
            "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
        }],
        outputs={"assets": ["agent"]},
    )
    asset_def = component.build_defs(None).assets[0]
    key = dg.AssetKey(["p_agent"])
    tags = asset_def.specs_by_key[key].tags
    assert "dagster-community/skill" not in tags

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)
    assert result.success
    mat_events = [e for e in result.all_events if e.is_step_materialization]
    md = mat_events[0].materialization.metadata
    assert "agent__skill_load_count" not in md
    assert "agent__skills_loaded" not in md
    assert "agent__skills_available" not in md


def test_two_skill_keys_do_not_crash_build_defs_on_tag_value_validation(mod, fake_litellm, skill_dir, tmp_path):
    """Dagster's tag-value validation only allows [A-Za-z0-9_.-], <=63 chars -- a raw
    comma-joined skill_keys list would otherwise crash build_defs as soon as 2+ keys are
    configured (',' isn't allowed)."""
    from resources.skill_resource.component import SkillResourceComponent

    other_dir = tmp_path / "other_skill"
    other_dir.mkdir()
    (other_dir / "SKILL.md").write_text(SKILL_MD.replace("pdf-report-skill", "other-skill"))
    SkillResourceComponent(resource_key="other_skill", local_path=str(other_dir)).build_defs(None)

    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p",
        source={"kind": "literal", "text": "doc"},
        steps=[{
            "id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini",
            "mcp_servers": [{"name": "inline", "type": "stdio", "command": ["x"]}],
            "skill_keys": ["pdf_skill", "other_skill"],
        }],
        outputs={"assets": ["agent"]},
    )
    asset_def = component.build_defs(None).assets[0]  # must not raise DagsterInvalidDefinitionError
    key = dg.AssetKey(["p_agent"])
    tag_value = asset_def.specs_by_key[key].tags["dagster-community/skill"]
    assert "," not in tag_value
    assert len(tag_value) <= 63
    assert "pdf_skill" in tag_value and "other_skill" in tag_value
