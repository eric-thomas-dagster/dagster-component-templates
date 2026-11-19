"""hosted_skills: Claude running CODE EXECUTION in Anthropic's hosted sandbox -- a separate
mechanism from skill_keys (plain text, load_skill/read_skill_file, no code execution at all).
Only the Anthropic SDK client is faked (same shape test_skill_keys.py's _fake_anthropic_module
already uses); verified request/response shapes come from reading the installed anthropic==1.11.0
SDK's own type definitions directly, not guessed.
"""
import importlib.util
import sys
import types
from pathlib import Path

import dagster as dg
import pytest

from .conftest import load_component_module


def _load_skill_resource_module():
    path = Path(__file__).resolve().parents[4] / "resources" / "skill_resource" / "component.py"
    spec = importlib.util.spec_from_file_location("_skill_resource_for_hosted_skills_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(**overrides):
    defaults = dict(asset_name="agent_out", prompt="make a chart")
    defaults.update(overrides)
    return load_component_module().AnthropicAgentComponent(**defaults)


def _run(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


def _text_block(text):
    return types.SimpleNamespace(type="text", text=text, model_dump=lambda: {"type": "text", "text": text})


def _thinking_block(text):
    return types.SimpleNamespace(type="thinking", thinking=text, model_dump=lambda: {"type": "thinking", "thinking": text})


def _bash_result_block(tool_use_id, file_ids):
    content_items = [types.SimpleNamespace(file_id=fid, type="bash_code_execution_output") for fid in file_ids]
    result_content = types.SimpleNamespace(type="bash_code_execution_result", stdout="", stderr="", return_code=0, content=content_items)
    dumped = {
        "type": "bash_code_execution_tool_result", "tool_use_id": tool_use_id,
        "content": {"type": "bash_code_execution_result", "content": [{"file_id": fid} for fid in file_ids]},
    }
    return types.SimpleNamespace(
        type="bash_code_execution_tool_result", tool_use_id=tool_use_id, content=result_content,
        model_dump=lambda: dumped,
    )


def _fake_anthropic_module(monkeypatch, turns, files_by_id=None):
    """turns: list of callables(kw) -> SimpleNamespace(content=[...], stop_reason=..., container=None)."""
    state = {"n": 0}
    files_by_id = files_by_id or {}
    seen_kwargs = []

    class _Messages:
        async def create(self, **kw):
            # `messages` is the SAME list object every call shares and keeps mutating
            # (append-in-place) -- snapshot a copy here, or a later inspection of an
            # earlier call's "messages" would see every later call's appends too.
            seen_kwargs.append({**kw, "messages": list(kw["messages"])})
            i = state["n"]
            state["n"] += 1
            return turns[i](kw)

    class _Resp:
        def __init__(self, data):
            self._data = data

        async def read(self):
            return self._data

    class _Files:
        async def retrieve_metadata(self, file_id):
            meta = files_by_id[file_id]["meta"]
            return types.SimpleNamespace(**meta)

        async def download(self, file_id):
            return _Resp(files_by_id[file_id]["data"])

    class AsyncAnthropic:
        def __init__(self, **kw):
            self.messages = _Messages()
            self.files = _Files()

    fake = types.ModuleType("anthropic")
    fake.AsyncAnthropic = AsyncAnthropic
    monkeypatch.setitem(sys.modules, "anthropic", fake)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")
    return seen_kwargs


# ---- construction / validation -----------------------------------------------------------

def test_hosted_skills_requires_type_and_skill_id_together(mod):
    with pytest.raises(ValueError, match="must both be set together"):
        mod.HostedSkillSpec(type="anthropic")


def test_hosted_skills_requires_exactly_one_source(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.HostedSkillSpec(type="anthropic", skill_id="pdf", skill_resource_key="chart_skill")
    with pytest.raises(ValueError, match="exactly one of"):
        mod.HostedSkillSpec()


def test_more_than_20_hosted_skills_rejected_at_build_defs(mod):
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf"} for _ in range(21)])
    with pytest.raises(ValueError, match="20"):
        component.build_defs(None)


def test_hosted_skill_resource_keys_in_required_resource_keys(mod):
    component = _component(hosted_skills=[{"skill_resource_key": "chart_skill"}])
    asset_def = component.build_defs(None).assets[0]
    assert "chart_skill" in asset_def.required_resource_keys


# ---- request shape: code_execution tool + container.skills --------------------------------

def test_hosted_skills_adds_code_execution_tool_and_container(monkeypatch):
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf", "version": "latest"}])

    def turn1(kw):
        return types.SimpleNamespace(content=[_text_block("done")], stop_reason="end_turn", container=None)

    seen = _fake_anthropic_module(monkeypatch, [turn1])
    result = _run(component)
    assert result.success

    tool_names = {t["name"] for t in seen[0]["tools"]}
    assert "code_execution" in tool_names
    code_tool = next(t for t in seen[0]["tools"] if t["name"] == "code_execution")
    assert code_tool["type"] == "code_execution_20250825"
    assert seen[0]["container"] == {"skills": [{"type": "anthropic", "skill_id": "pdf", "version": "latest"}]}


def test_code_execution_tool_version_is_overridable(monkeypatch):
    component = _component(
        hosted_skills=[{"type": "anthropic", "skill_id": "pdf"}], code_execution_tool_version="code_execution_20260521",
    )

    def turn1(kw):
        return types.SimpleNamespace(content=[_text_block("done")], stop_reason="end_turn", container=None)

    seen = _fake_anthropic_module(monkeypatch, [turn1])
    _run(component)
    code_tool = next(t for t in seen[0]["tools"] if t["name"] == "code_execution")
    assert code_tool["type"] == "code_execution_20260521"


def test_pause_turn_resends_without_a_tool_result_and_keeps_the_container(monkeypatch):
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf"}], max_iterations=3)

    def turn1(kw):
        return types.SimpleNamespace(
            content=[_text_block("partway")], stop_reason="pause_turn",
            container=types.SimpleNamespace(id="cont_abc"),
        )

    def turn2(kw):
        return types.SimpleNamespace(content=[_text_block("done")], stop_reason="end_turn", container=None)

    seen = _fake_anthropic_module(monkeypatch, [turn1, turn2])
    result = _run(component)
    assert result.success
    assert len(seen) == 2
    assert seen[1]["container"] == {"id": "cont_abc", "skills": [{"type": "anthropic", "skill_id": "pdf", "version": "latest"}]}
    # no tool_result message was inserted between the two assistant turns
    roles = [m["role"] for m in seen[1]["messages"]]
    assert roles == ["user", "assistant"]


def test_thinking_blocks_are_preserved_in_the_transcript(monkeypatch):
    """Pre-existing gap, not hosted_skills-specific: only text/tool_use blocks used to be kept
    in the running `messages` list -- a thinking block (or any other block type) was silently
    dropped. Exercised here via a plain hosted_skills run since it shares the same block loop."""
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf"}])

    def turn1(kw):
        return types.SimpleNamespace(
            content=[_thinking_block("reasoning..."), _text_block("done")], stop_reason="end_turn", container=None,
        )

    _fake_anthropic_module(monkeypatch, [turn1])
    result = _run(component)
    assert result.success
    transcript = result.output_for_node("agent_out")["transcript"]
    assistant_msg = transcript[-1]
    assert assistant_msg["role"] == "assistant"
    assert any(b.get("type") == "thinking" for b in assistant_msg["content"])


# ---- generated files: harvesting, downloading, hosted_skills_files_dir -----------------------

def test_generated_files_are_returned_without_hosted_skills_files_dir(monkeypatch):
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf"}])

    def turn1(kw):
        return types.SimpleNamespace(
            content=[_bash_result_block("srv1", ["file_abc"]), _text_block("made a chart")],
            stop_reason="end_turn", container=None,
        )

    _fake_anthropic_module(monkeypatch, [turn1], files_by_id={
        "file_abc": {"meta": {"filename": "chart.png", "mime_type": "image/png"}, "data": b"PNGDATA"},
    })
    result = _run(component)
    assert result.success
    output = result.output_for_node("agent_out")
    assert len(output["files"]) == 1
    f = output["files"][0]
    assert f["name"] == "chart.png" and f["media_type"] == "image/png" and f["bytes"] == b"PNGDATA"
    assert "path" not in f


def test_generated_files_are_written_to_hosted_skills_files_dir_with_a_manifest(monkeypatch, tmp_path):
    files_dir = str(tmp_path / "hosted_files")
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf"}], hosted_skills_files_dir=files_dir)

    def turn1(kw):
        return types.SimpleNamespace(
            content=[_bash_result_block("srv1", ["file_abc"]), _text_block("made a chart")],
            stop_reason="end_turn", container=None,
        )

    _fake_anthropic_module(monkeypatch, [turn1], files_by_id={
        "file_abc": {"meta": {"filename": "chart.png", "mime_type": "image/png"}, "data": b"PNGDATA"},
    })
    result = _run(component)
    assert result.success
    output = result.output_for_node("agent_out")
    f = output["files"][0]
    assert "path" in f and Path(f["path"]).read_bytes() == b"PNGDATA"

    skill_resource_mod = _load_skill_resource_module()
    run_id = result.run_id
    manifest_mod = load_component_module()
    manifest = manifest_mod.read_run_manifest(files_dir, run_id)
    assert len(manifest) == 1
    assert manifest[0]["name"] == "chart.png" and manifest[0]["tool"] == "hosted_skills"


def test_generated_file_download_failure_is_skipped_not_fatal(monkeypatch):
    component = _component(hosted_skills=[{"type": "anthropic", "skill_id": "pdf"}])

    def turn1(kw):
        return types.SimpleNamespace(
            content=[_bash_result_block("srv1", ["file_missing"]), _text_block("done")],
            stop_reason="end_turn", container=None,
        )

    _fake_anthropic_module(monkeypatch, [turn1], files_by_id={})  # file_missing -> KeyError inside the fake
    result = _run(component)
    assert result.success
    assert result.output_for_node("agent_out")["files"] == []


def test_no_hosted_skills_means_no_file_handling_at_all(monkeypatch):
    component = _component()

    def turn1(kw):
        return types.SimpleNamespace(content=[_text_block("done")], stop_reason="end_turn")

    _fake_anthropic_module(monkeypatch, [turn1])
    result = _run(component)
    assert result.success
    assert result.output_for_node("agent_out")["files"] == []


# ---- skill_resource_key: resolved to a custom skill_id at run time -------------------------

def _write_skill_dir(tmp_path, name="chart-skill"):
    d = tmp_path / name
    d.mkdir()
    (d / "SKILL.md").write_text(f"---\nname: {name}\ndescription: Makes charts.\n---\n\nbody\n")
    return str(d)


@pytest.fixture()
def custom_skill_resource(tmp_path):
    skill_resource_mod = _load_skill_resource_module()
    local_path = _write_skill_dir(tmp_path)
    return skill_resource_mod.SkillResource(
        key="chart_skill", name="chart-skill", description="Makes charts.",
        local_path=local_path, anthropic_skill_id="skill_abc123",
    )


def test_skill_resource_key_resolves_to_a_custom_skill_id(monkeypatch, custom_skill_resource):
    component = _component(hosted_skills=[{"skill_resource_key": "chart_skill", "version": "latest"}])

    def turn1(kw):
        return types.SimpleNamespace(content=[_text_block("done")], stop_reason="end_turn", container=None)

    seen = _fake_anthropic_module(monkeypatch, [turn1])
    result = _run(component, resources={"chart_skill": custom_skill_resource})
    assert result.success
    assert seen[0]["container"] == {"skills": [{"type": "custom", "skill_id": "skill_abc123", "version": "latest"}]}


def test_skill_resource_key_without_anthropic_skill_id_raises_clearly(monkeypatch, tmp_path):
    skill_resource_mod = _load_skill_resource_module()
    local_path = _write_skill_dir(tmp_path)
    plain_skill = skill_resource_mod.SkillResource(key="chart_skill", name="chart-skill", description="Makes charts.", local_path=local_path)
    component = _component(hosted_skills=[{"skill_resource_key": "chart_skill"}])

    def turn1(kw):
        return types.SimpleNamespace(content=[_text_block("done")], stop_reason="end_turn", container=None)

    _fake_anthropic_module(monkeypatch, [turn1])
    with pytest.raises(Exception, match="anthropic_skill_id"):
        _run(component, resources={"chart_skill": plain_skill})
