"""The dlt-based ingestion connectors filter their returned rows to the run's partition.

Every connector is built from its own example.yaml and materialized through dg.materialize(). Only the network is
faked: the dlt pipeline run and SQL query-back return one fixed 6-row frame, and `dlt.sources.*` modules that are not
installed (verified sources are vendored by `dlt init`) are fabricated on import."""
import importlib.abc
import importlib.machinery
import importlib.util
import json
import re
import sys
import types
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
BEGIN, END = "# BEGIN partition row-filter helper", "# END partition row-filter helper"


def _ported():
    return [p.parent for p in sorted(ROOT.glob("assets/ingestion/*/component.py")) if BEGIN in p.read_text()]


class _Permissive:
    def __getattr__(self, name):
        return self if not name.startswith("__") else object.__getattribute__(self, name)

    def __call__(self, *a, **k):
        return self

    def __iter__(self):
        return iter(())


class _FakeSourcesFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Fabricate `dlt.sources.<anything>` modules that are not installed."""

    def find_spec(self, name, path=None, target=None):
        if name.startswith("dlt.sources.") and name.count(".") >= 2:
            return importlib.machinery.ModuleSpec(name, self, is_package=True)
        return None

    def create_module(self, spec):
        mod = types.ModuleType(spec.name)
        mod.__path__ = []
        mod.__getattr__ = lambda attr: _Permissive() if not attr.startswith("__") else (_ for _ in ()).throw(AttributeError(attr))
        return mod

    def exec_module(self, module):
        pass


FRAME = pd.DataFrame({
    "id": range(6),
    "created_at": ["2024-01-02T10:00:00", "2024-01-02T18:00:00", "2024-01-03T10:00:00",
                   "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00"],
    "bucket": ["a", "a", "b", "b", "c", "c"],
})


class _Cursor:
    description = [(c,) for c in FRAME.columns]

    def fetchall(self):
        return list(FRAME.itertuples(index=False, name=None))

    def df(self, *a, **k):
        return FRAME.copy()


class _Sql:
    def __enter__(self): return self
    def __exit__(self, *e): return False

    def execute_query(self, sql, *a, **k):
        from contextlib import contextmanager

        @contextmanager
        def cm():
            yield _Cursor()
        return cm()


class _Pipeline:
    def __init__(self, **kw): pass
    def run(self, *a, **k): return "load-info"
    def sql_client(self): return _Sql()
    def __getattr__(self, n): return _Permissive()


class _AnyJson(dict):
    """A response body that answers any key (or index) with another such body, so login handshakes succeed."""

    def __missing__(self, key):
        return _AnyJson()

    def __getitem__(self, key):
        return dict.__getitem__(self, key) if key in self else _AnyJson()

    def __iter__(self):
        return iter(())


class _Response:
    status_code = 200
    ok = True
    headers = {}
    text = "{}"
    content = b"{}"

    def json(self, *a, **k):
        return _AnyJson(access_token="t", token="t", entries=[], data=[], items=[], results=[])

    def raise_for_status(self):
        pass


def _block_network(monkeypatch):
    """Nothing in these tests may reach a real service (example credentials must never be sent anywhere)."""
    import requests
    import socket

    monkeypatch.setattr(requests.sessions.Session, "request", lambda self, *a, **k: _Response())

    def _no_socket(*a, **k):
        raise AssertionError("test attempted a real network connection")

    monkeypatch.setattr(socket.socket, "connect", _no_socket)


REQUIRED = {"mailchimp_ingestion": {"api_key": "abc123-us19"}, "webflow_ingestion": {"site_id": "site1"}}


def _build(folder, monkeypatch, extra):
    _block_network(monkeypatch)
    extra = {**REQUIRED.get(folder.name, {}), **extra}
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    monkeypatch.setattr(sys, "meta_path", [*sys.meta_path, _FakeSourcesFinder()])
    spec = importlib.util.spec_from_file_location(f"ing_{folder.name}", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)  # dlt resolves a resource function's module through sys.modules
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod.dlt, "pipeline", lambda **kw: _Pipeline(**kw))
    import dlt.sources.rest_api as _rest_api

    monkeypatch.setattr(_rest_api, "rest_api_source", lambda *a, **k: _Permissive())
    cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
    return cls(**{**doc["attributes"], **extra}).build_defs(None).assets[0]


def test_helper_copies_identical_and_everything_is_wired():
    blocks = {}
    for f in _ported():
        s = (f / "component.py").read_text()
        blocks.setdefault(re.search(rf"{BEGIN}.*?{END}", s, re.S).group(0), []).append(f.name)
        assert re.search(r"combined_df = pd\.concat\(all_data, ignore_index=True\)\n\s+combined_df = _filter_df_to_partition\(", s), f
        props = json.loads((f / "schema.json").read_text())
        props = props.get("attributes") or props.get("properties")
        assert {"partition_date_column", "partition_static_column", "backfill_policy", "backfill_policy_max_partitions_per_run"} <= set(props), f
        assert "does not currently use the partition key" not in props["partition_type"]["description"], f
    assert len(blocks) == 1 and len(_ported()) == 110


@pytest.mark.parametrize("folder", _ported(), ids=lambda p: p.name)
def test_daily_partition_returns_only_that_days_rows(folder, monkeypatch):
    asset = _build(folder, monkeypatch, dict(partition_type="daily", partition_start="2024-01-01", partition_date_column="created_at"))
    result = dg.materialize([asset], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert result.success, result
    df = result.output_for_node(asset.op.name)
    assert set(df["id"]) == {0, 1}  # each resource returns the same frame, so ids repeat
    assert df["created_at"].str.startswith("2024-01-02").all()


def test_static_partition_and_unpartitioned_runs(monkeypatch):
    folder = ROOT / "assets/ingestion/amplitude_ingestion"
    asset = _build(folder, monkeypatch, dict(partition_type="static", partition_values="a,b,c", partition_static_column="bucket"))
    r = dg.materialize([asset], partition_key="b", resources={"io_manager": dg.InMemoryIOManager()})
    assert set(r.output_for_node(asset.op.name)["id"]) == {2, 3}
    plain = _build(folder, monkeypatch, {})
    r = dg.materialize([plain], resources={"io_manager": dg.InMemoryIOManager()})
    assert len(r.output_for_node(plain.op.name)) == 6
