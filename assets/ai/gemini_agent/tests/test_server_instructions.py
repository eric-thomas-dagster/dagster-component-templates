"""Per-server `instructions` (saved config + the real server's own initialize() instructions)
combine into one heading per server, spliced into the Gemini system_instruction, returned as
combined_server_instructions, and surfaced as run metadata (item 8)."""
import asyncio
import inspect
import sys
import types

import dagster as dg
import pytest

pytest.importorskip("mcp")

from .conftest import load_component_module  # noqa: E402
from .test_mcp_real_server import SERVER_SRC, _child_env  # noqa: E402


def _run(coro, timeout=90):
    return asyncio.run(asyncio.wait_for(coro, timeout))


class _Log:
    def __getattr__(self, _):
        return lambda *a, **k: None


def _call_kwargs(fn, cfg, **extra):
    cand = dict(log=_Log(), prompt="go", system_prompt=None, model="m", temperature=0.0, max_tokens=64,
                max_output_tokens=64, thinking_budget=None, max_iterations=3, mcp_servers=[cfg],
                request_timeout_seconds=30, api_key_env_var="FAKE_KEY", reasoning_effort=None)
    cand.update(extra)
    params = inspect.signature(fn).parameters
    return {k: v for k, v in cand.items() if k in params}


class _Rec:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def model_dump(self, exclude_none=True):
        return dict(self.__dict__)


def _fake_genai(monkeypatch, seen):
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
            seen["config"] = config
            content = gtypes.Content(role="model", parts=[Part.from_text("done")])
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


def _drive(mod, cfg, monkeypatch):
    seen = {}
    _fake_genai(monkeypatch, seen)
    monkeypatch.setenv("FAKE_KEY", "k")
    res = _run(mod._run_agent(**_call_kwargs(mod._run_agent, cfg)))
    return res, seen["config"]


def test_per_server_instructions_combine_into_system_instruction(tmp_path, monkeypatch):
    mod = load_component_module()
    server_src = SERVER_SRC.replace(
        'server = Server("mini")',
        'server = Server("mini", instructions="(from server) read-only, do not write.")',
    )
    p = tmp_path / "mini_mcp_server_instructions.py"
    p.write_text(server_src)
    cfg = {
        "name": "mini", "type": "stdio", "command": [sys.executable, str(p), "stdio"], "env": _child_env(),
        "instructions": "Saved note: always cite the ticket number.",
    }

    res, config = _drive(mod, cfg, monkeypatch)

    combined = res["combined_server_instructions"]
    assert combined is not None
    assert "## mini" in combined
    assert "Saved note: always cite the ticket number." in combined
    assert "(server-provided): (from server) read-only, do not write." in combined
    assert combined in config.system_instruction


def test_no_instructions_means_no_combined_block_or_system_instruction(tmp_path, monkeypatch):
    mod = load_component_module()
    p = tmp_path / "mini_mcp_server_plain.py"
    p.write_text(SERVER_SRC)
    cfg = {"name": "mini", "type": "stdio", "command": [sys.executable, str(p), "stdio"], "env": _child_env()}

    res, config = _drive(mod, cfg, monkeypatch)

    assert res["combined_server_instructions"] is None
    assert not hasattr(config, "system_instruction")


def test_saved_instructions_surface_in_run_metadata(tmp_path, monkeypatch):
    mod = load_component_module()
    p = tmp_path / "mini_mcp_server_plain2.py"
    p.write_text(SERVER_SRC)
    cfg = {
        "name": "mini", "type": "stdio", "command": [sys.executable, str(p), "stdio"], "env": _child_env(),
        "instructions": "Always show amounts in cents.",
    }
    _fake_genai(monkeypatch, {})
    monkeypatch.setenv("FAKE_KEY", "k")

    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(asset_name="agent_out", prompt="hi", model="m", api_key_env_var="FAKE_KEY", mcp_servers=[cfg])
    res = dg.materialize(comp.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    md = res.get_asset_materialization_events()[0].materialization.metadata
    assert "mcp_server_instructions" in md
    assert "Always show amounts in cents." in md["mcp_server_instructions"].value
