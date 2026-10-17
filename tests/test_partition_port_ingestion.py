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
    """The dlt connectors that filter `combined_df` (other ingestion components share the helper but have their own tests)."""
    return [p.parent for p in sorted(ROOT.glob("assets/ingestion/*/component.py"))
            if BEGIN in p.read_text() and "combined_df = _filter_df_to_partition(" in p.read_text()]


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


CAPTURED = []  # the config each connector hands to dlt's rest_api_source
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

    CAPTURED.clear()
    monkeypatch.setattr(_rest_api, "rest_api_source", lambda *a, **k: (CAPTURED.append(a[0] if a else k.get("config")), _Permissive())[1])
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


WINDOW = {"start_param": "updated_since", "end_param": "updated_until"}


def _window_ported():
    return [p for p in _ported() if "BEGIN partition window helper" in (p / "component.py").read_text()]


@pytest.mark.parametrize("folder", _window_ported(), ids=lambda p: p.name)
def test_partition_window_reaches_every_resources_request(folder, monkeypatch):
    asset = _build(folder, monkeypatch, dict(partition_type="daily", partition_start="2024-01-01", partition_date_column="created_at",
                                             partition_window_params=WINDOW))
    assert dg.materialize([asset], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False).success
    resources = CAPTURED[-1]["resources"]
    assert resources
    for res in resources:
        params = res["endpoint"]["params"]
        assert params["updated_since"] == "2024-01-02T00:00:00Z" and params["updated_until"] == "2024-01-03T00:00:00Z", res


def test_window_is_only_applied_to_a_partitioned_time_run_and_in_the_chosen_format(monkeypatch):
    folder = ROOT / "assets/ingestion/amplitude_ingestion"
    plain = _build(folder, monkeypatch, dict(partition_window_params=WINDOW))
    dg.materialize([plain], resources={"io_manager": dg.InMemoryIOManager()})
    assert all("updated_since" not in (r["endpoint"].get("params") or {}) for r in CAPTURED[-1]["resources"])
    epoch = _build(folder, monkeypatch, dict(partition_type="daily", partition_start="2024-01-01",
                                             partition_window_params={**WINDOW, "format": "epoch"}))
    dg.materialize([epoch], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()})
    assert CAPTURED[-1]["resources"][0]["endpoint"]["params"]["updated_since"] == 1704153600
    static = _build(folder, monkeypatch, dict(partition_type="static", partition_values="a,b", partition_window_params=WINDOW))
    dg.materialize([static], partition_key="a", resources={"io_manager": dg.InMemoryIOManager()})
    assert all("updated_since" not in (r["endpoint"].get("params") or {}) for r in CAPTURED[-1]["resources"])


def test_resources_can_be_limited_and_the_helper_is_identical_everywhere():
    blocks = {re.search(r"# BEGIN partition window helper.*?# END partition window helper", (p / "component.py").read_text(), re.S).group(0)
              for p in _window_ported()}
    assert len(blocks) == 1 and len(_window_ported()) == 108
    ns = {}
    exec(next(iter(blocks)), ns)

    class Ctx:
        has_partition_key = True
        has_partition_key_range = False

        class partition_time_window:
            import datetime as _d
            start = _d.datetime(2024, 1, 2, tzinfo=_d.timezone.utc)
            end = _d.datetime(2024, 1, 3, tzinfo=_d.timezone.utc)

    config = {"resources": ["events", {"name": "users", "endpoint": {"path": "users"}}, {"name": "static", "endpoint": "plans"}]}
    assert ns["_apply_partition_window"](config, Ctx, {"start_param": "s"}, ["users"])
    assert config["resources"][0] == "events" and config["resources"][1]["endpoint"]["params"] == {"s": "2024-01-02T00:00:00Z"}
    assert ns["_apply_partition_window"](config, Ctx, {"start_param": "s", "end_param": "e", "format": "date"}, None)
    assert config["resources"][0]["endpoint"]["params"] == {"s": "2024-01-02", "e": "2024-01-03"}
    assert config["resources"][2]["endpoint"] == {"path": "plans", "params": {"s": "2024-01-02", "e": "2024-01-03"}}
    with pytest.raises(ValueError):
        ns["_apply_partition_window"](config, Ctx, {"start_param": "s", "format": "nope"})


class _AnyPartitionIO(dg.IOManager):
    """Stores one value per asset and ignores partitions, so a run spanning several partitions can be inspected."""

    def __init__(self):
        self.values = {}

    def handle_output(self, context, obj):
        self.values[context.asset_key] = obj

    def load_input(self, context):
        return self.values[context.asset_key]


def _range_tags(first, last):
    return {"dagster/asset_partition_range_start": first, "dagster/asset_partition_range_end": last}


@pytest.mark.parametrize("folder", _ported(), ids=lambda p: p.name)
def test_single_run_backfill_covers_the_whole_range_in_one_run(folder, monkeypatch):
    """An opt-in single_run backfill is ONE run for several partitions: it must return every partition's rows."""
    asset = _build(folder, monkeypatch, dict(partition_type="daily", partition_start="2024-01-01", partition_date_column="created_at",
                                             backfill_policy="single_run"))
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()
    io = _AnyPartitionIO()
    r = dg.materialize([asset], resources={"io_manager": io}, tags=_range_tags("2024-01-02", "2024-01-03"), raise_on_error=False)
    assert r.success, r
    df = io.values[asset.key]
    assert set(df["id"]) == {0, 1, 2}                       # 2024-01-02 (ids 0, 1) and 2024-01-03 (id 2); not 2024-01-04
    assert not df["created_at"].str.startswith("2024-01-04").any()
