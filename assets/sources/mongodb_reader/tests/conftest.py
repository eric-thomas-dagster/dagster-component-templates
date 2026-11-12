"""Test helpers for MongodbReaderComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (pymongo client) is faked; everything the
component itself owns -- query/filter dispatch, resource_key branching, metadata -- runs
for real under dg.materialize(). Mirrors mongodb_writer's tests/conftest.py conventions,
adapted for a read path (find/sort/limit) instead of a write path.
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("mongodb_reader_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, field, direction):
        self.docs.sort(key=lambda d: d.get(field), reverse=(direction == -1))
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []
        self.find_calls = []

    def find(self, query=None, projection=None):
        self.find_calls.append((query, projection))
        q = query or {}
        matched = [d for d in self.docs if all(d.get(k) == v for k, v in q.items())]
        return FakeCursor(matched)


class FakeMongoClient:
    """Records constructions; client[db][coll] hands out a persistent in-memory collection."""

    instances = []
    preseed = {}  # {(db, coll): [docs]} applied to every newly constructed client

    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        self.colls = {}
        for k, docs in FakeMongoClient.preseed.items():
            self.colls[k] = FakeCollection(docs=[dict(d) for d in docs])
        FakeMongoClient.instances.append(self)

    def __getitem__(self, database):
        client = self

        class _Db:
            def __getitem__(self, coll):
                return client.colls.setdefault((database, coll), FakeCollection())

        return _Db()


def install_fake_pymongo(monkeypatch):
    """Inject a fake `pymongo` exposing exactly the name the component imports
    (MongoClient); restored by monkeypatch at test end."""
    FakeMongoClient.instances = []
    FakeMongoClient.preseed = {}
    mod = types.ModuleType("pymongo")
    mod.MongoClient = FakeMongoClient
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    return FakeMongoClient


class FakeMongoResource:
    """Stands in for the registered MongoDBResource (exposes .get_client())."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client
