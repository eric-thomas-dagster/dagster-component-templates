"""Test helpers for MongodbWriterComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (pymongo client) is faked; everything the component
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
    spec = importlib.util.spec_from_file_location("mongodb_writer_component", here / "component.py")
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


class FakeCollection:
    def __init__(self):
        self.docs = []

    def drop(self):
        self.docs = []

    def insert_many(self, records):
        self.docs.extend(dict(r) for r in records)

    def bulk_write(self, ops):
        for op in ops:
            ((key, val),) = op.filter.items()
            hit = [d for d in self.docs if d.get(key) == val]
            if hit:
                hit[0].update(op.update["$set"])
            elif op.upsert:
                self.docs.append(dict(op.update["$set"]))


class FakeMongoClient:
    """Records constructions; client[db][coll] hands out a persistent in-memory collection."""

    instances = []
    preseed = {}  # {(db, coll): [docs]} applied to every newly constructed client

    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        self.colls = {}
        for k, docs in FakeMongoClient.preseed.items():
            self.colls[k] = FakeCollection()
            self.colls[k].docs = [dict(d) for d in docs]
        FakeMongoClient.instances.append(self)

    def __getitem__(self, database):
        client = self

        class _Db:
            def __getitem__(self, coll):
                return client.colls.setdefault((database, coll), FakeCollection())

        return _Db()


class FakeUpdateOne:
    def __init__(self, filter, update, upsert=False):
        self.filter, self.update, self.upsert = filter, update, upsert


def install_fake_pymongo(monkeypatch):
    """Inject a fake `pymongo` exposing exactly the names the component imports
    (MongoClient, UpdateOne); restored by monkeypatch at test end."""
    FakeMongoClient.instances = []
    FakeMongoClient.preseed = {}
    mod = types.ModuleType("pymongo")
    mod.MongoClient = FakeMongoClient
    mod.UpdateOne = FakeUpdateOne
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    return FakeMongoClient


class FakeMongoResource:
    """Stands in for the registered MongoDBResource (exposes .get_client())."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client
