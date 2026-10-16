"""Loads component.py by file path so tests don't depend on package install."""
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest


# lancedb is not installed in CI; a tiny in-memory stand-in keeps the manager's real
# table-naming / range logic running and records every call.
STORE: dict = {}
CALLS: list = []


class _Table:
    def __init__(self, df):
        self._df = df

    def to_pandas(self):
        return self._df.copy()


class _DB:
    def create_table(self, name, data, mode="create"):
        CALLS.append(("create_table", name, mode))
        STORE[name] = data.to_pandas() if hasattr(data, "to_pandas") and not isinstance(data, pd.DataFrame) else data.copy()

    def open_table(self, name):
        CALLS.append(("open_table", name))
        return _Table(STORE[name])


_fake = types.ModuleType("lancedb")
_fake.connect = lambda path, storage_options=None: _DB()


def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_lance_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class Backend:
    def __init__(self, mod):
        self.manager = mod.LanceIOManager(base_path="memory://lance")

    @staticmethod
    def _name(asset, key):
        return asset if key is None else f"{asset}__{key}"

    def has(self, asset, key):
        return self._name(asset, key) in STORE

    def read(self, asset, key):
        return STORE[self._name(asset, key)]


@pytest.fixture
def be(mod, monkeypatch):
    monkeypatch.setitem(sys.modules, "lancedb", _fake)
    STORE.clear()
    CALLS.clear()
    return Backend(mod)
