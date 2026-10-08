"""Shared test helpers for DefaultResource / DefaultResourceComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The real `requests` HTTP calls are
monkeypatched wholesale on `requests.Session.request` (the one method
`DefaultResource._request` actually calls) -- this mirrors the repo's
"mock only the paid/external call" convention. No real network access is
ever made.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("default_resource_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, json_data=None, status_code=200, text=""):
        self._json_data = json_data
        self.status_code = status_code
        self.content = b"x" if json_data is not None else b""
        self.text = text if text else (str(json_data) if json_data is not None else "")

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data
