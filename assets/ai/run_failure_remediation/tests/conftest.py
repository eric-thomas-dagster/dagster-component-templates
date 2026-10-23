"""Loads the real component.py by path; also loads agentic_pipeline + agent_review_queue so tests use the REAL `approve` helper
(approve_pending) and the REAL review queue against our pending records. Only the paid model call is faked."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


HERE = pathlib.Path(__file__).resolve().parent
AI = HERE.parents[1]


def load_component_module() -> ModuleType:
    return _load(HERE.parent / "component.py", "run_failure_remediation_under_test")


def load_agentic_module() -> ModuleType:
    return _load(AI / "agentic_pipeline" / "component.py", "agentic_pipeline_for_remediation_tests")


def load_review_queue_module() -> ModuleType:
    return _load(AI / "agent_review_queue" / "component.py", "agent_review_queue_for_remediation_tests")


class FakeLiteLLM(types.ModuleType):
    """Scripted litellm for the one pipeline test: only the paid network call is faked."""

    def __init__(self):
        super().__init__("litellm")
        self.calls = []
        self.script = []
        self.drop_params = False

    def completion(self, **kwargs):
        from types import SimpleNamespace
        self.calls.append(kwargs)
        content, tool_calls = self.script.pop(0)
        msg = SimpleNamespace(content=content, tool_calls=tool_calls, reasoning_content=None, provider_specific_fields=None)
        usage = SimpleNamespace(total_tokens=10, prompt_tokens=5, completion_tokens=5, model_dump=lambda: {"total_tokens": 10})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=usage)

    def completion_cost(self, completion_response=None):
        return 0.0
