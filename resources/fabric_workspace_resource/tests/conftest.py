"""Shared test helpers for FabricWorkspaceResource.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. All HTTP calls go through `requests.get`/
`requests.post` (monkeypatched at the module boundary) and token fetching
goes through `_get_token()` (monkeypatched directly) -- `azure.identity`
isn't installed in this environment and is only imported lazily inside
`_credential()`, so no test needs it.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("fabric_workspace_resource_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}
        self.text = text or ""

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 300:
            raise RuntimeError(f"HTTP {self.status_code}")
