"""skill_keys: lets gemini_agent reference one or more SkillResourceComponent-registered
skills by resource_key. Each skill's name/description are injected into the agent's
system prompt up front (cheap); the full SKILL.md body is only fetched if the model
actually calls `load_skill` -- progressive disclosure, same shape Claude Code's own
Skill tool uses. `load_skill`/`read_skill_file` are added as Gemini FunctionDeclarations
in the SAME tool list the MCP tools go through, so the model can interleave calls to
either on any turn.

Builds a tiny REAL local skill fixture inline (a SKILL.md with real YAML front matter +
one bundled file) and registers it through the REAL SkillResourceComponent -- loaded via
importlib the same way this test dir's own conftest.py loads gemini_agent's component.py
-- so this test exercises the real describe()/load()/read_file() contract, not a mock of
it. Only the paid Gemini model call is faked (same fake-client shape test_mcp_real_server.py
already uses for this component).
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import dagster as dg
import pytest

from .conftest import load_component_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def load_skill_resource_module() -> ModuleType:
    path = REPO_ROOT / "resources" / "skill_resource" / "component.py"
    spec = importlib.util.spec_from_file_location("skill_resource_component_under_test", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def skill_resource(tmp_path):
    """A real skill folder + a real SkillResource built via SkillResourceComponent."""
    skill_dir = tmp_path / "pdf_report_skill"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(
        "---\n"
        "name: PDF Report Builder\n"
        "description: Builds a formatted PDF report from tabular data.\n"
        "---\n"
        "# PDF Report Builder\n\nFull instructions for building the report go here.\n"
    )
    (skill_dir / "template.html").write_text("<html><body>{{ rows }}</body></html>")

    skill_mod = load_skill_resource_module()
    component = skill_mod.SkillResourceComponent(resource_key="pdf_report_skill", local_path=str(skill_dir))
    defs = component.build_defs(None)
    return defs.resources["pdf_report_skill"]


def _fake_genai(monkeypatch, *, tool_call_name=None, tool_call_args=None):
    """Installs a fake google.genai module. If tool_call_name is set, turn 1 returns a
    function_call for it; turn 2 (and later) returns a final text response. If unset,
    turn 1 returns a final text response immediately (no tool call)."""
    state = {"n": 0}
    seen = {}

    class _Rec:
        def __init__(self, **kw):
            self.__dict__.update(kw)

        def model_dump(self, exclude_none=True):
            return dict(self.__dict__)

    class Part(_Rec):
        @staticmethod
        def from_text(text):
            return Part(text=text, function_call=None)

        @staticmethod
        def from_function_response(name, response):
            return Part(text=None, function_call=None, function_response={"name": name, "response": response})

    gtypes = types.ModuleType("google.genai.types")
    gtypes.Part = Part
    gtypes.Content = type("Content", (_Rec,), {})
    gtypes.FunctionDeclaration = type("FunctionDeclaration", (_Rec,), {})
    gtypes.Tool = type("Tool", (_Rec,), {})
    gtypes.GenerateContentConfig = type("GenerateContentConfig", (_Rec,), {})
    gtypes.ThinkingConfig = type("ThinkingConfig", (_Rec,), {})

    class _Models:
        async def generate_content(self, model, contents, config):
            state["n"] += 1
            seen.setdefault("configs", []).append(config)
            if state["n"] == 1 and tool_call_name:
                fc = types.SimpleNamespace(name=tool_call_name, args=tool_call_args or {})
                content = gtypes.Content(role="model", parts=[Part(text=None, function_call=fc)])
            else:
                content = gtypes.Content(role="model", parts=[Part.from_text("final answer")])
            return types.SimpleNamespace(candidates=[types.SimpleNamespace(content=content)])

    class Client:
        def __init__(self, **kw):
            self.aio = types.SimpleNamespace(models=_Models())

    genai = types.ModuleType("google.genai")
    genai.Client = Client
    genai.types = gtypes
    google = types.ModuleType("google")
    google.genai = genai
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.genai", genai)
    monkeypatch.setitem(sys.modules, "google.genai.types", gtypes)
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key")
    return seen


def _materialize(component, resources=None):
    return dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager(), **(resources or {})},
        raise_on_error=False,
    )


def test_required_resource_keys_includes_skill_keys(mod):
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    assert "pdf_report_skill" in asset_def.required_resource_keys


def test_no_skill_keys_means_no_skill_metadata(mod, monkeypatch):
    _fake_genai(monkeypatch)
    component = mod.GeminiAgentComponent(asset_name="agent_out", prompt="hi")
    result = _materialize(component)
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "skills_available" not in md
    assert "skills_loaded" not in md
    assert "skill_load_count" not in md


def test_skills_available_metadata_reflects_configured_skill(mod, monkeypatch, skill_resource):
    _fake_genai(monkeypatch)
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"],
    )
    result = _materialize(component, resources={"pdf_report_skill": skill_resource})
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    available = md["skills_available"].value
    assert available == [
        {"key": "pdf_report_skill", "name": "PDF Report Builder", "description": "Builds a formatted PDF report from tabular data."}
    ]
    assert md["skills_loaded"].value == []
    assert md["skill_load_count"].value == 0


def test_load_skill_tool_call_loop_tracks_loaded_names_and_count(mod, monkeypatch, skill_resource):
    _fake_genai(monkeypatch, tool_call_name="load_skill", tool_call_args={"name": "PDF Report Builder"})
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"], max_iterations=5,
    )
    result = _materialize(component, resources={"pdf_report_skill": skill_resource})
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["skills_loaded"].value == ["PDF Report Builder"]
    assert md["skill_load_count"].value == 1
    assert md["stopped_reason"].value == "final_answer"
    assert md["iterations"].value == 2
    # Two tool-call rounds: load_skill dispatched + reported.
    tool_calls = md["tool_calls"].value
    assert tool_calls[0]["tool"] == "load_skill"
    assert "Full instructions for building the report" in tool_calls[0]["result_preview"]


def test_read_skill_file_tool_reads_bundled_file(mod, monkeypatch, skill_resource):
    _fake_genai(
        monkeypatch, tool_call_name="read_skill_file",
        tool_call_args={"name": "PDF Report Builder", "relative_path": "template.html"},
    )
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"], max_iterations=5,
    )
    result = _materialize(component, resources={"pdf_report_skill": skill_resource})
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    # read_skill_file does NOT count toward skill_load_count (only load_skill does).
    assert md["skill_load_count"].value == 0
    assert md["skills_loaded"].value == []
    tool_calls = md["tool_calls"].value
    assert tool_calls[0]["tool"] == "read_skill_file"
    assert "{{ rows }}" in tool_calls[0]["result_preview"]


def test_read_skill_file_missing_path_reports_error_not_crash(mod, monkeypatch, skill_resource):
    _fake_genai(
        monkeypatch, tool_call_name="read_skill_file",
        tool_call_args={"name": "PDF Report Builder", "relative_path": "does_not_exist.txt"},
    )
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"], max_iterations=5,
    )
    result = _materialize(component, resources={"pdf_report_skill": skill_resource})
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    tool_calls = md["tool_calls"].value
    assert tool_calls[0]["is_error"] is True
    assert "No such file" in tool_calls[0]["result_preview"]


def test_unresolved_skill_key_raises_naming_the_key(mod, monkeypatch):
    _fake_genai(monkeypatch)
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["does_not_exist"],
    )
    # No resource registered under "does_not_exist" at all -- required_resource_keys
    # includes it, so Dagster's own resource-binding check rejects the run before our
    # asset body ever runs. The error still names the exact key.
    with pytest.raises(dg.DagsterInvalidDefinitionError, match="does_not_exist"):
        dg.materialize(
            component.build_defs(None).assets,
            resources={"io_manager": dg.InMemoryIOManager()},
        )


def test_skill_resource_registered_as_wrong_type_raises(mod, monkeypatch):
    _fake_genai(monkeypatch)
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["not_a_skill"],
    )

    class NotASkill(dg.ConfigurableResource):
        pass

    # A resource IS registered under the key, so binding succeeds and execution
    # reaches our own code -- which must raise, naming the key, because the
    # resource doesn't look like a SkillResource (missing describe()/load()).
    with pytest.raises(ValueError, match="not_a_skill"):
        dg.materialize(
            component.build_defs(None).assets,
            resources={"io_manager": dg.InMemoryIOManager(), "not_a_skill": NotASkill()},
        )


def test_dagster_community_skill_tag_set_when_configured(mod):
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    tags = asset_def.tags_by_key[next(iter(asset_def.keys))]
    assert tags["dagster-community/skill"] == "pdf_report_skill"


def test_dagster_community_skill_tag_sanitizes_multiple_keys(mod):
    # A raw comma-joined value would fail Dagster's strict tag-value validation
    # (alnum/_/-/. only) -- commas are replaced so the asset still builds.
    component = mod.GeminiAgentComponent(
        asset_name="agent_out", prompt="hi", skill_keys=["pdf_report_skill", "other_skill"],
    )
    asset_def = component.build_defs(None).assets[0]
    tags = asset_def.tags_by_key[next(iter(asset_def.keys))]
    assert tags["dagster-community/skill"] == "pdf_report_skill-other_skill"


def test_dagster_community_skill_tag_absent_when_not_configured(mod):
    component = mod.GeminiAgentComponent(asset_name="agent_out", prompt="hi")
    asset_def = component.build_defs(None).assets[0]
    tags = asset_def.tags_by_key[next(iter(asset_def.keys))]
    assert "dagster-community/skill" not in tags


def test_skill_keys_accepts_csv_string(mod):
    component = mod.GeminiAgentComponent(asset_name="agent_out", prompt="hi", skill_keys="a, b")
    assert component.skill_keys == ["a", "b"]
