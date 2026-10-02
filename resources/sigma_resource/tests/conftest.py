"""Shared test helpers for SigmaResource / SigmaResourceComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The real `requests` HTTP calls (token fetch +
.get()/.post()) are monkeypatched wholesale on the `requests` module that
component.py imports locally inside each method -- this mirrors the repo's
"mock only the paid/external call" convention.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("sigma_resource_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, json_data, status_code=200, content=b"1"):
        self._json_data = json_data
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data
