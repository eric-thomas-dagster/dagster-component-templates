"""Loads the real component.py by path and fakes ONLY the paid network call (litellm.completion)."""
import importlib.util
import json
import logging
import pathlib
import sys
import threading
import types
from types import SimpleNamespace

import pytest


def load_component_module():
    path = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("agentic_pipeline_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeLiteLLM(types.ModuleType):
    """Scripted litellm: each completion() pops the next scripted reply and records the kwargs it was called with."""

    def __init__(self):
        super().__init__("litellm")
        self.calls = []
        self.script = []
        self.drop_params = False
        self._lock = threading.Lock()

    def reply(self, content="", tool_calls=None, reasoning_content=None, tokens=10):
        self.script.append((content, tool_calls or [], reasoning_content, tokens))

    def completion(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
            content, tool_calls, reasoning, tokens = self.script.pop(0) if self.script else ("ok", [], None, 10)
        if isinstance(content, Exception):  # scripted failure (e.g. a model that is down)
            raise content
        msg = SimpleNamespace(content=content, tool_calls=tool_calls, reasoning_content=reasoning, provider_specific_fields=None)
        usage = SimpleNamespace(total_tokens=tokens, prompt_tokens=tokens // 2, completion_tokens=tokens - tokens // 2,
                                model_dump=lambda: {"total_tokens": tokens})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=usage)

    def completion_cost(self, completion_response=None):
        return 0.01


def tool_call(name, args, id="c1"):
    return SimpleNamespace(id=id, function=SimpleNamespace(name=name, arguments=json.dumps(args)))


@pytest.fixture
def fake_litellm(monkeypatch):
    fake = FakeLiteLLM()
    monkeypatch.setitem(sys.modules, "litellm", fake)
    return fake


@pytest.fixture
def mod(fake_litellm):
    return load_component_module()


@pytest.fixture
def ctx():
    return SimpleNamespace(log=logging.getLogger("agentic_test"))


def new_state(policy=None, text="hello"):
    state = {"source": {"text": text}, "__ctx__": {"partition_key": None, "partition_fields": None}, "__personas__": {}}
    if policy is not None:
        state["__policy__"] = policy
    return state


# ── installed-MCP-major awareness (the MCP client tests run under whichever majors are installed) ──────────────────
def _dist_major(name):
    try:
        from importlib.metadata import version
        return int(version(name).split(".")[0])
    except Exception:  # noqa: BLE001
        return None


MCP_MAJOR = _dist_major("mcp")
FASTMCP_MAJOR = _dist_major("fastmcp")


@pytest.fixture(scope="session")
def mcp_major():
    """Installed `mcp` major (1 or 2); the MCP client tests assert version-neutral behaviour and use this only for the
    few facts that really differ per major (skip with pytest.skip, never branch silently)."""
    if MCP_MAJOR is None:
        pytest.skip("mcp not installed")
    return MCP_MAJOR


@pytest.fixture(scope="session")
def fastmcp_major():
    if FASTMCP_MAJOR is None:
        pytest.skip("fastmcp not installed")
    return FASTMCP_MAJOR
