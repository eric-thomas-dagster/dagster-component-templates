"""Loads component.py by file path so tests don't depend on package install."""
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest


# pymongo is not installed in CI; an in-memory stand-in for MongoClient keeps the manager's
# real collection-naming / replace / insert logic running.
DBS: dict = {}


class _Coll:
    def __init__(self, docs):
        self.docs = docs

    def drop(self):
        self.docs.clear()

    def estimated_document_count(self):
        return len(self.docs)

    def insert_many(self, records):
        self.docs.extend(dict(r) for r in records)

    def find(self, flt, proj=None):
        assert flt == {}
        return [{k: v for k, v in d.items() if k != "_id"} for d in self.docs]


class _DB:
    def __init__(self, colls):
        self.colls = colls

    def __getitem__(self, name):
        return _Coll(self.colls.setdefault(name, []))


class _Client:
    def __init__(self, uri):
        pass

    def __getitem__(self, name):
        return _DB(DBS.setdefault(name, {}))

    def close(self):
        pass


_fake = types.ModuleType("pymongo")
_fake.MongoClient = _Client


def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_mongodb_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class Backend:
    def __init__(self, mod):
        self.manager = mod.MongoDBIOManager(connection_uri="mongodb://fake", database="testdb")

    @staticmethod
    def _name(asset, key):
        return asset if key is None else f"{asset}_{key}"

    def has(self, asset, key):
        return bool(DBS.get("testdb", {}).get(self._name(asset, key)))

    def read(self, asset, key):
        return pd.DataFrame(DBS["testdb"][self._name(asset, key)])


@pytest.fixture
def be(mod, monkeypatch):
    monkeypatch.setitem(sys.modules, "pymongo", _fake)
    DBS.clear()
    return Backend(mod)
