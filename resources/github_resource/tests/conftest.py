"""Shared test helpers for GitHubResource / GithubResourceComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. All of GitHubResource's convenience methods go
through `get_client()` -> `requests.Session()`, so the real `requests` HTTP
calls are monkeypatched on `requests.Session.get/post/put/patch/delete` --
never on the module's own `mod.requests` attribute (that wholesale-replace
pattern causes cross-test pollution at full-suite scale in this repo).
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("github_resource_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, json_data=None, status_code=200, headers=None):
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data
