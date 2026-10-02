"""Shared test helpers for DynamicsCrmResource / DynamicsCrmResourceComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The real `requests` HTTP calls (token fetch +
.get()/.post()/.patch()) are monkeypatched wholesale on the `requests` module
that component.py imports locally inside each method -- this mirrors the
repo's "mock only the paid/external call" convention (see marketo_resource's
tests for the same pattern).
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "dynamics_crm_resource_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, json_data=None, status_code=200, headers=None, content=b"x"):
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}
        # `content` truthiness gates whether callers attempt .json() -- a
        # 204 has an empty body in real life.
        self.content = b"" if status_code == 204 else content

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data
