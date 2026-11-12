"""Test helpers for RedisReaderComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (redis client) is faked; everything the component
itself owns -- resource_key dispatch, metadata -- runs for real under dg.materialize().
"""
import fnmatch
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("redis_reader_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeRedis:
    """In-memory redis mimicking the raw-bytes (decode_responses=False) client the
    component's inline path constructs. Server state is class-level so the inline
    client and a resource-provided client see the same data; `constructed` records
    inline constructions."""

    store = {}  # key(bytes) -> {"type": str, "value": ...}
    constructed = []

    def __init__(self, host=None, port=None, password=None, db=None, ssl=None, **kw):
        self.cfg = {"host": host, "port": port, "password": password, "db": db, "ssl": ssl}
        FakeRedis.constructed.append(self)

    def keys(self, pattern):
        pat = pattern.decode() if isinstance(pattern, bytes) else pattern
        return [k for k in FakeRedis.store if fnmatch.fnmatch(k.decode(), pat)]

    def type(self, key):
        return FakeRedis.store[key]["type"].encode()

    def get(self, key):
        return FakeRedis.store[key]["value"]

    def hgetall(self, key):
        return FakeRedis.store[key]["value"]

    def lrange(self, key, start, end):
        return FakeRedis.store[key]["value"]

    def smembers(self, key):
        return FakeRedis.store[key]["value"]

    def zscan_iter(self, key):
        return iter(FakeRedis.store[key]["value"])


def install_fake_redis(monkeypatch):
    """Inject a fake `redis` module (restored by monkeypatch)."""
    FakeRedis.store, FakeRedis.constructed = {}, []
    FakeRedis.store[b"session:1"] = {
        "type": "hash",
        "value": {b"user_id": b"42", b"status": b"active"},
    }
    mod = types.ModuleType("redis")
    mod.Redis = FakeRedis
    monkeypatch.setitem(sys.modules, "redis", mod)
    return FakeRedis


class FakeRedisResource:
    """Stands in for the registered RedisResource (exposes .get_client())."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client
