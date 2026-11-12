"""Test helpers for RedisStreamsMonitorSensorComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (redis client) is faked; the sensor itself
runs for real via its underlying evaluation function.
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("redis_streams_monitor_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeRedis:
    """In-memory redis stream client. `constructed` records inline constructions."""

    constructed = []
    entries = []  # list of (entry_id, fields_dict), shared "server" state

    def __init__(self, host=None, port=None, db=None, password=None, ssl=None, decode_responses=None, **kw):
        self.cfg = {"host": host, "port": port, "db": db, "password": password, "ssl": ssl}
        FakeRedis.constructed.append(self)

    def ping(self):
        return True

    def xread(self, streams, count=None, block=None):
        (stream_name, last_id), = streams.items()
        if not FakeRedis.entries:
            return []
        return [(stream_name, list(FakeRedis.entries))]


def install_fake_redis(monkeypatch):
    """Inject a fake `redis` module (restored by monkeypatch)."""
    FakeRedis.constructed = []
    FakeRedis.entries = [("1-0", {"order_id": "123", "status": "placed"})]
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
