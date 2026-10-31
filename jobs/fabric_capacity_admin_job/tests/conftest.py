"""Shared test helpers for FabricCapacityAdminJobComponent.

Loads component.py directly via importlib (no pip install needed).
`azure.identity` isn't installed in this environment and is only imported
lazily inside the op body, so it's faked via sys.modules, restored after
each test so it doesn't leak into other test files.
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("fabric_capacity_admin_job_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json_data = json_data
        self.headers = headers or {}
        self.text = text or ""

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 300:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeCredential:
    def __init__(self, *a, **k):
        pass

    def get_token(self, scope):
        return types.SimpleNamespace(token="fake-token")


@pytest.fixture
def fake_azure_identity():
    saved = {k: sys.modules.get(k) for k in ("azure", "azure.identity")}
    azure_mod = types.ModuleType("azure")
    identity_mod = types.ModuleType("azure.identity")
    identity_mod.DefaultAzureCredential = _FakeCredential
    identity_mod.ClientSecretCredential = _FakeCredential
    azure_mod.identity = identity_mod
    sys.modules["azure"] = azure_mod
    sys.modules["azure.identity"] = identity_mod
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
