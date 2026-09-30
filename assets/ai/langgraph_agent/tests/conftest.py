"""Shared test helpers for LangGraphAgentComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed.
"""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "langgraph_agent_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _langgraph_available() -> bool:
    try:
        import langgraph.graph  # noqa: F401
        return True
    except ImportError:
        return False


requires_langgraph = pytest.mark.skipif(
    not _langgraph_available(),
    reason="requires langgraph (pip install langgraph)",
)


class FakeLLMResponse:
    def __init__(self, content: str):
        self.content = content


class FakeLLM:
    """Stands in for a real ChatOpenAI/ChatAnthropic/etc. client. Takes a
    `responder(msgs) -> str` callable so a single monkeypatch of
    `_build_llm` can drive every node in a graph deterministically --
    the real network/paid LLM call is never made in these tests."""

    def __init__(self, responder):
        self._responder = responder

    def invoke(self, msgs):
        return FakeLLMResponse(self._responder(msgs))
