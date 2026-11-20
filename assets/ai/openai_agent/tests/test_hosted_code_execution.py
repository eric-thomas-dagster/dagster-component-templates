"""hosted_code_execution: OpenAI's hosted code_interpreter tool (Responses API only) running a
skill that needs code execution -- a separate mechanism from skill_keys' plain load_skill/
read_skill_file loop (text only, no code execution at all). Only the OpenAI SDK client is faked
(same shape test_skill_keys.py's fake client already uses); verified request/response shapes come
from OpenAI's own current API reference (container/file_ids, container_file_citation annotations,
the container-files content-retrieval endpoint), not guessed -- see the field's own docstring for
exactly what wasn't pinned down to one certain shape (the async file-download call).
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
    spec = importlib.util.spec_from_file_location("_skill_resource_for_hosted_code_execution_test", path)
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
    return load_component_module().OpenAIAgentComponent(**defaults)


def _run(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
    )


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


@pytest.fixture()
def explicit_code_skill_resource(tmp_path):
    """skill_kind='code_execution' override -- no scripts/ dir, no run-hint text, so the
    heuristic alone would say runnable_here=True; the explicit override must still route it
    through hosted_code_execution."""
    skill_dir = tmp_path / "override_skill"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("---\nname: override-skill\ndescription: Needs code execution, explicitly marked.\n---\n\nPlain instructions.\n")
    skill_resource_mod = _load_skill_resource_module()
    component = skill_resource_mod.SkillResourceComponent(
        resource_key="override_skill", local_path=str(skill_dir), skill_kind="code_execution",
    )
    return component.build_defs(None).resources["override_skill"]


def _message_item(text, annotations=None):
    content = [{"type": "output_text", "text": text, "annotations": annotations or []}]
    return types.SimpleNamespace(
        type="message",
        model_dump=lambda exclude_none=True: {"type": "message", "role": "assistant", "content": content},
    )


def _fake_response(output_items, output_text):
    return types.SimpleNamespace(output=output_items, output_text=output_text)


def _fake_openai_module(monkeypatch, turns, files_by_id=None):
    """turns: list of callables(kw) -> a fake response. files_by_id: {file_id: bytes} for
    client.containers.files.content.retrieve."""
    state = {"n": 0}
    files_by_id = files_by_id or {}
    seen_kwargs = []

    class _Responses:
        async def create(self, **kw):
            seen_kwargs.append(kw)
            i = state["n"]
            state["n"] += 1
            return turns[i](kw)

    class _Files:
        def __init__(self):
            self.created = []

        async def create(self, file, purpose):
            self.created.append({"file": file, "purpose": purpose})
            return types.SimpleNamespace(id=f"file-{len(self.created)}")

    class _ContentResp:
        def __init__(self, data):
            self._data = data

        async def read(self):
            return self._data

    class _Content:
        async def retrieve(self, file_id, container_id):
            return _ContentResp(files_by_id[file_id])

    class _ContainerFiles:
        def __init__(self):
            self.content = _Content()

    class _Containers:
        def __init__(self):
            self.files = _ContainerFiles()

    files_ns = _Files()

    class AsyncOpenAI:
        def __init__(self, **kw):
            self.responses = _Responses()
            self.files = files_ns
            self.containers = _Containers()

    fake = types.ModuleType("openai")
    fake.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("FAKE_OPENAI_KEY", "sk-fake")
    return seen_kwargs, files_ns


# ---- upload + tool wiring --------------------------------------------------------------------

def test_hosted_code_execution_uploads_skill_files_and_adds_tool(mod, code_skill_resource, monkeypatch):
    seen_kwargs, files_ns = _fake_openai_module(monkeypatch, [lambda kw: _fake_response([_message_item("done")], "done")])
    component = _component(
        skill_keys=["chart_skill"], hosted_code_execution=True, api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"chart_skill": code_skill_resource})
    assert result.success

    # SKILL.md + the one bundled script, both uploaded as purpose='user_data'.
    assert len(files_ns.created) == 2
    assert all(f["purpose"] == "user_data" for f in files_ns.created)

    ci_tools = [t for t in seen_kwargs[0]["tools"] if t.get("type") == "code_interpreter"]
    assert len(ci_tools) == 1
    assert ci_tools[0]["container"]["type"] == "auto"
    assert len(ci_tools[0]["container"]["file_ids"]) == 2

    # Listed in the system prompt (passed as `instructions`), not requiring a load_skill call.
    assert "chart-skill" in (seen_kwargs[0].get("instructions") or "")
    tool_names = {t.get("name") for t in seen_kwargs[0]["tools"] if t.get("type") == "function"}
    assert "load_skill" not in tool_names  # routed to code execution, not the plain skill loop


def test_hosted_code_execution_false_keeps_the_old_omit_and_warn_behavior(mod, code_skill_resource, monkeypatch):
    seen_kwargs, files_ns = _fake_openai_module(monkeypatch, [lambda kw: _fake_response([_message_item("done")], "done")])
    component = _component(skill_keys=["chart_skill"], api_key_env_var="FAKE_OPENAI_KEY")  # hosted_code_execution defaults False
    result = _run(component, resources={"chart_skill": code_skill_resource})
    assert result.success
    assert files_ns.created == []  # nothing uploaded -- default behavior unchanged
    assert not any(t.get("type") == "code_interpreter" for t in (seen_kwargs[0].get("tools") or []))


def test_skill_kind_override_routes_to_code_execution_even_without_scripts(mod, explicit_code_skill_resource, monkeypatch):
    seen_kwargs, files_ns = _fake_openai_module(monkeypatch, [lambda kw: _fake_response([_message_item("done")], "done")])
    component = _component(
        skill_keys=["override_skill"], hosted_code_execution=True, api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component, resources={"override_skill": explicit_code_skill_resource})
    assert result.success
    assert len(files_ns.created) == 1  # just SKILL.md -- no bundled files on this fixture
    assert any(t.get("type") == "code_interpreter" for t in seen_kwargs[0]["tools"])


# ---- generated files: discovery, download, (optional) manifest write --------------------------

def test_hosted_code_execution_generated_file_is_downloaded_and_returned(mod, monkeypatch):
    annotations = [{"type": "container_file_citation", "container_id": "cntr_1", "file_id": "cfile_1", "filename": "chart.png"}]
    turns = [lambda kw: _fake_response([_message_item("Here's your chart.", annotations)], "Here's your chart.")]
    seen_kwargs, files_ns = _fake_openai_module(monkeypatch, turns, files_by_id={"cfile_1": b"\x89PNGfakebytes"})

    component = _component(hosted_code_execution=True, api_key_env_var="FAKE_OPENAI_KEY")
    result = _run(component)
    assert result.success

    mat = result.asset_value("agent_out")
    assert len(mat["files"]) == 1
    f = mat["files"][0]
    assert f["file_id"] == "cfile_1"
    assert f["container_id"] == "cntr_1"
    assert f["name"] == "chart.png"
    assert f["bytes"] == b"\x89PNGfakebytes"


def test_hosted_code_execution_files_dir_writes_manifest(mod, tmp_path, monkeypatch):
    annotations = [{"type": "container_file_citation", "container_id": "cntr_1", "file_id": "cfile_1", "filename": "chart.png"}]
    turns = [lambda kw: _fake_response([_message_item("done", annotations)], "done")]
    _fake_openai_module(monkeypatch, turns, files_by_id={"cfile_1": b"pngdata"})

    files_dir = str(tmp_path / "hosted_files")
    component = _component(
        hosted_code_execution=True, hosted_code_execution_files_dir=files_dir, api_key_env_var="FAKE_OPENAI_KEY",
    )
    result = _run(component)
    assert result.success

    mat = result.asset_value("agent_out")
    assert mat["files"][0]["path"].endswith("chart.png") or "cfile_1" in mat["files"][0]["path"]
    with open(mat["files"][0]["path"], "rb") as fh:
        assert fh.read() == b"pngdata"

    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["hosted_code_execution_used"].value is True
    assert md["hosted_code_execution_files"].value[0]["name"] == "chart.png"


def test_no_generated_files_means_empty_files_list_and_used_false(mod, monkeypatch):
    _fake_openai_module(monkeypatch, [lambda kw: _fake_response([_message_item("no files here")], "no files here")])
    component = _component(hosted_code_execution=True, api_key_env_var="FAKE_OPENAI_KEY")
    result = _run(component)
    assert result.success
    mat = result.asset_value("agent_out")
    assert mat["files"] == []
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["hosted_code_execution_used"].value is False
