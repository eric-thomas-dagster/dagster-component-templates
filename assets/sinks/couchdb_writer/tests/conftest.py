"""Test helpers for CouchdbWriterComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (CouchDB HTTP API via `requests`) is faked; everything the component
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
    spec = importlib.util.spec_from_file_location("couchdb_writer_component", here / "component.py")
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


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeCouch:
    """In-memory CouchDB: records every HTTP call (method, url, json, auth) and keeps docs by URL."""

    def __init__(self):
        self.calls = []
        self.docs = {}
        self._auto = 0

    def get(self, url, **kw):
        self.calls.append({"method": "get", "url": url, "json": None, "auth": kw.get("auth")})
        doc = self.docs.get(url)
        return FakeResponse(200, {"_id": doc["_id"], "_rev": doc.get("_rev")}) if doc else FakeResponse(404)

    def put(self, url, json=None, **kw):
        self.calls.append({"method": "put", "url": url, "json": dict(json), "auth": kw.get("auth")})
        self.docs[url] = dict(json)
        return FakeResponse(201, {"ok": True})

    def post(self, url, json=None, **kw):
        self.calls.append({"method": "post", "url": url, "json": dict(json), "auth": kw.get("auth")})
        self._auto += 1
        doc = dict(json)
        doc["_id"] = f"auto-{self._auto}"
        self.docs[f"{url}/{doc['_id']}"] = doc
        return FakeResponse(201, {"ok": True})


def install_fake_requests(monkeypatch):
    """Patch requests.get/put/post (the external HTTP calls); restored by monkeypatch."""
    import requests

    fake = FakeCouch()
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(requests, "put", fake.put)
    monkeypatch.setattr(requests, "post", fake.post)
    return fake


class FakeCouchResource:
    """Stands in for the registered CouchDBResource: exposes exactly the attributes url/username/password."""

    def __init__(self, url, username="", password=""):
        self.url = url
        self.username = username
        self.password = password
