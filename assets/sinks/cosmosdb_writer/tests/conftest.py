"""Shared test helpers for CosmosdbWriterComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The only thing faked is the external service:
the `azure.cosmos` client (a recording in-memory container). Everything the
component itself owns -- partition filtering, backfill wiring, resource_key
dispatch, metadata -- runs for real under dg.materialize().
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
    spec = importlib.util.spec_from_file_location("cosmosdb_writer_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeContainer:
    def __init__(self):
        self.items = []

    def upsert_item(self, item):
        self.items.append(("upsert", dict(item)))

    def create_item(self, item):
        self.items.append(("create", dict(item)))


class FakeCosmosClient:
    """Records every container handed out so tests can read back what was written."""

    instances = []

    def __init__(self, endpoint=None, credential=None):
        self.endpoint = endpoint
        self.credential = credential
        self.containers = {}
        FakeCosmosClient.instances.append(self)

    def get_database_client(self, database):
        client = self

        class _Db:
            def get_container_client(self, container):
                return client.containers.setdefault((database, container), FakeContainer())

        return _Db()


def install_fake_azure_cosmos(monkeypatch):
    """Inject a fake `azure.cosmos` (restored by monkeypatch at test end)."""
    FakeCosmosClient.instances = []
    azure = types.ModuleType("azure")
    azure.__path__ = []
    cosmos = types.ModuleType("azure.cosmos")
    cosmos.CosmosClient = FakeCosmosClient
    azure.cosmos = cosmos
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.cosmos", cosmos)
    return FakeCosmosClient


class FakeCosmosResource:
    """Stands in for the registered CosmosDBResource (exposes .get_client())."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


class PartitionedMemoryIOManager(dg.IOManager):
    """In-memory IO manager that survives across dg.materialize() calls (dg.InMemoryIOManager
    is per-run, so it cannot carry upstream partitions materialized in earlier runs) and
    behaves like a partition-range-aware warehouse IO manager: an asset consuming several
    partitions is handed ONE DataFrame spanning them (``multi="concat"``, the default;
    this component annotates its input ``pd.DataFrame``) or, like dagster's filesystem
    IO manager, a ``{partition_key: DataFrame}`` dict (``multi="dict"``)."""

    def __init__(self, multi="concat"):
        self.store = {}
        self.multi = multi

    def handle_output(self, context, obj):
        pk = context.asset_partition_key if context.has_asset_partitions else None
        self.store[(context.asset_key, pk)] = obj

    def load_input(self, context):
        if context.has_asset_partitions:
            keys = list(context.asset_partition_keys)
            if len(keys) == 1:
                return self.store[(context.asset_key, keys[0])]
            parts = {k: self.store[(context.asset_key, k)] for k in keys}
            return parts if self.multi == "dict" else pd.concat(parts.values(), ignore_index=True)
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
