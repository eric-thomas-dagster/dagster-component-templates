"""Test helpers for RedisWriterComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (redis client) is faked; everything the component
itself owns -- partition filtering, backfill wiring, resource_key dispatch, metadata -- runs for real
under dg.materialize().
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import dagster as dg
import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("redis_writer_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class PartitionedMemoryIOManager(dg.IOManager):
    """In-memory IO manager that survives across dg.materialize() calls (dg.InMemoryIOManager
    is per-run, so it cannot carry upstream partitions materialized in earlier runs) and
    behaves like a partition-range-aware warehouse IO manager: an asset consuming several
    partitions is handed ONE DataFrame spanning them (this component annotates its input
    ``pd.DataFrame``)."""

    def __init__(self):
        self.store = {}

    def handle_output(self, context, obj):
        pk = context.asset_partition_key if context.has_asset_partitions else None
        self.store[(context.asset_key, pk)] = obj

    def load_input(self, context):
        if context.has_asset_partitions:
            keys = list(context.asset_partition_keys)
            if len(keys) == 1:
                return self.store[(context.asset_key, keys[0])]
            return pd.concat([self.store[(context.asset_key, k)] for k in keys], ignore_index=True)
        return self.store[(context.asset_key, None)]


def daily_upstream(name="orders_src", start="2024-01-01"):
    """A daily-partitioned upstream asset returning that day's rows (id, order_date, total)."""
    @dg.asset(name=name, partitions_def=dg.DailyPartitionsDefinition(start_date=start))
    def _up(context: dg.AssetExecutionContext) -> pd.DataFrame:
        day = context.partition_key
        base = {"2024-01-02": 100, "2024-01-03": 200, "2024-01-04": 300}[day]
        return pd.DataFrame({
            "id": [base + 1, base + 2],
            "order_date": [f"{day} 08:00:00", f"{day} 17:30:00"],
            "total": [1.5, 2.5],
        })
    return _up


def materialize_upstream_days(upstream_def, instance, io_manager, days):
    for d in days:
        r = dg.materialize([upstream_def], partition_key=d, instance=instance,
                           resources={"io_manager": io_manager})
        assert r.success


class FakeRedis:
    """In-memory redis. The 'server' state is class-level so the inline client and a
    resource-provided client see the same data; `constructed` records inline constructions."""

    store = {}      # key -> hash dict | str | list
    expiries = {}
    constructed = []

    def __init__(self, host=None, port=None, password=None, db=None, ssl=None, **kw):
        self.cfg = {"host": host, "port": port, "password": password, "db": db, "ssl": ssl}
        FakeRedis.constructed.append(self)

    def hset(self, key, mapping=None):
        FakeRedis.store.setdefault(key, {}).update(mapping)

    def set(self, key, value):
        FakeRedis.store[key] = value

    def rpush(self, key, value):
        FakeRedis.store.setdefault(key, []).append(value)

    def expire(self, key, seconds):
        FakeRedis.expiries[key] = seconds


def install_fake_redis(monkeypatch):
    """Inject a fake `redis` module (restored by monkeypatch)."""
    FakeRedis.store, FakeRedis.expiries, FakeRedis.constructed = {}, {}, []
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
