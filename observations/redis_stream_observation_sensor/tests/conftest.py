"""Test helpers for RedisStreamObservationSensorComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (redis client) is faked.
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("redis_stream_observation_sensor_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeRedis:
    """In-memory redis stream client. `constructed` records inline constructions."""

    constructed = []

    def __init__(self, host=None, port=None, db=None, password=None, decode_responses=None, **kw):
        self.cfg = {"host": host, "port": port, "db": db, "password": password}
        FakeRedis.constructed.append(self)

    def xlen(self, stream_name):
        return 42

    def xinfo_groups(self, stream_name):
        return [{"pending": 3}, {"pending": 2}]


def install_fake_redis(monkeypatch):
    """Inject a fake `redis` module (restored by monkeypatch)."""
    FakeRedis.constructed = []
    mod = types.ModuleType("redis")
    mod.Redis = FakeRedis
    monkeypatch.setitem(sys.modules, "redis", mod)
    return FakeRedis


class FakeObserveResource:
    """Stands in for a resource exposing `.observe(stream_name) -> dict`."""

    def __init__(self, observed):
        self._observed = observed
        self.calls = []

    def observe(self, stream_name):
        self.calls.append(stream_name)
        return dict(self._observed)
