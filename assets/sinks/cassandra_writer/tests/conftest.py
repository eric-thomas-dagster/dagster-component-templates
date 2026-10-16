"""Test helpers for CassandraWriterComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (cassandra-driver (Cluster/session)) is faked; everything the component
itself owns -- partition filtering, backfill wiring, metadata -- runs for real
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
    spec = importlib.util.spec_from_file_location("cassandra_writer_component", here / "component.py")
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


class FakeBatch:
    def __init__(self):
        self.entries = []

    def add(self, prepared, values):
        self.entries.append((prepared, values))


class FakeSession:
    def __init__(self, cluster, keyspace):
        self.cluster, self.keyspace = cluster, keyspace
        self.prepared = []
        self.executed = []

    def prepare(self, cql):
        self.prepared.append(cql)
        return cql

    def execute(self, batch):
        self.executed.append(batch)


class FakeCluster:
    instances = []

    def __init__(self, hosts, port=9042, auth_provider=None):
        self.hosts, self.port, self.auth_provider = hosts, port, auth_provider
        self.sessions = []
        FakeCluster.instances.append(self)

    def connect(self, keyspace):
        s = FakeSession(self, keyspace)
        self.sessions.append(s)
        return s


class FakePlainTextAuthProvider:
    def __init__(self, username, password):
        self.username, self.password = username, password


def install_fake_cassandra(monkeypatch):
    """Inject fake cassandra.cluster / cassandra.auth / cassandra.query (restored by monkeypatch)."""
    FakeCluster.instances = []
    pkg = types.ModuleType("cassandra")
    pkg.__path__ = []
    cluster = types.ModuleType("cassandra.cluster")
    cluster.Cluster = FakeCluster
    auth = types.ModuleType("cassandra.auth")
    auth.PlainTextAuthProvider = FakePlainTextAuthProvider
    query = types.ModuleType("cassandra.query")
    query.BatchStatement = FakeBatch
    pkg.cluster, pkg.auth, pkg.query = cluster, auth, query
    for name, m in (("cassandra", pkg), ("cassandra.cluster", cluster),
                    ("cassandra.auth", auth), ("cassandra.query", query)):
        monkeypatch.setitem(sys.modules, name, m)
    return FakeCluster
