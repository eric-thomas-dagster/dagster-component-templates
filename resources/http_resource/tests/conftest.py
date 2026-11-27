"""Shared test helpers for HTTPResourceComponent (self-contained; loads component.py via
importlib so tests don't require the parent package to be pip-installed).

`requests` is a real dependency (not faked/injected) -- the external boundary mocked here
is `requests.Session.request` itself (monkeypatched per-test), matching this repo's "mock
only the paid/external call" convention. EnvVar-wrapped fields only resolve to real values
once bound into an actual Dagster run, so every test drives the resource through a tiny
probe asset under dg.materialize() rather than inspecting the resource object directly.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("http_resource_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text if text is not None else ("" if json_data is None else "")
        self.headers = headers or ({"content-type": "application/json"} if json_data is not None else {})

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data


def run_request(mod, component, call_kwargs=None, capture_headers=None):
    """Builds the resource from `component`, runs one HTTPResource.request() call inside a
    real materialized asset (so EnvVar fields resolve), and returns (result, success)."""
    call_kwargs = call_kwargs or {"method": "GET", "path": "/ping"}
    defs = component.build_defs(context=None)
    resource = defs.resources[component.resource_key]

    box = {}

    @dg.asset
    def _probe(http_resource: mod.HTTPResource):
        if capture_headers is not None:
            session = http_resource.get_session()
            capture_headers.append(dict(session.headers))
        box["result"] = http_resource.request(**call_kwargs)
        return dg.MaterializeResult()

    result = dg.materialize([_probe], resources={"http_resource": resource})
    return box.get("result"), result.success
