"""Tests for QuickBooksIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout pattern as servicenow_ingestion/zendesk_ingestion. QuickBooks builds its own
dlt rest_api_source config directly (not a verified source), so table_name_map is wired in
via the `table_name` key on each resource dict (confirmed real: `EndpointResource` has both
`name` and `table_name` keys), and `asset_shape='multi_asset'` subsets by only building
resource dicts for the selected entities before calling rest_api_source -- unselected
entities are never requested from QuickBooks at all.

`asset_shape='single'` (default) preserves the original single-asset, union-all-entities
behavior unchanged.
"""
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

import importlib.util
import pathlib

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("quickbooks_ingestion_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

QuickBooksIngestionComponent = _mod.QuickBooksIngestionComponent


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        client_id="cid",
        client_secret="csecret",
        refresh_token="rtoken",
        realm_id="12345",
        resources="Customer,Invoice",
    )
    defaults.update(overrides)
    return QuickBooksIngestionComponent(**defaults)


class _Cursor:
    def __init__(self, df):
        self._df = df

    def df(self):
        return self._df


class _Sql:
    def __init__(self, df_or_per_table):
        self._df_or_per_table = df_or_per_table

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_query(self, sql, *a, **k):
        df = self._df_or_per_table
        if isinstance(df, dict):
            match = next((d for name, d in df.items() if name in sql), None)
            df = match if match is not None else pd.DataFrame({"Id": []})

        @contextmanager
        def cm():
            yield _Cursor(df)
        return cm()


class _Pipeline:
    def __init__(self, df, **kw):
        self._df = df

    def run(self, *a, **k):
        return "load-info"

    def sql_client(self):
        return _Sql(self._df)


def _patch_dlt(monkeypatch, df, captured_configs):
    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _Pipeline(df, **kw))
    import dlt.sources.rest_api as _rest_api

    def _fake_rest_api_source(config):
        captured_configs.append(config)
        return object()

    monkeypatch.setattr(_rest_api, "rest_api_source", _fake_rest_api_source)


def _fake_token_response(monkeypatch):
    """`_fetch_access_token` does `import requests as _requests; _requests.post(...)` locally --
    that import resolves to the same module object already in sys.modules, so patching the real
    `requests.post` here reaches it."""
    import requests

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "fake-token"}

    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp())


FRAME = pd.DataFrame({"Id": ["1", "2", "3"], "DisplayName": ["a", "b", "c"]})


def test_asset_shape_defaults_to_single(monkeypatch):
    _fake_token_response(monkeypatch)
    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(monkeypatch):
    _fake_token_response(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, {"Customer": FRAME, "Invoice": pd.DataFrame({"Id": ["9"]})}, captured)

    component = _build_component(asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_Customer"), dg.AssetKey("test_asset_Invoice")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["Customer", "Invoice"]

    customers = result.output_for_node("test_asset", output_name="test_asset_Customer")
    invoices = result.output_for_node("test_asset", output_name="test_asset_Invoice")
    assert len(customers) == 3
    assert len(invoices) == 1


def test_multi_asset_shape_selecting_one_resource_never_fetches_the_other(monkeypatch):
    _fake_token_response(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, {"Customer": FRAME, "Invoice": pd.DataFrame({"Id": ["9"]})}, captured)

    component = _build_component(asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_Customer")],
    )
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["Customer"]  # Invoice was never requested from QuickBooks at all


def test_table_name_map_overrides_destination_table_in_single_shape(monkeypatch):
    _fake_token_response(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, {"raw_customers": FRAME}, captured)

    component = _build_component(resources="Customer", table_name_map={"Customer": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert captured[-1]["resources"][0]["table_name"] == "raw_customers"
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(monkeypatch):
    _fake_token_response(monkeypatch)
    captured = []
    _patch_dlt(monkeypatch, {"raw_customers": FRAME}, captured)

    component = _build_component(
        resources="Customer", asset_shape="multi_asset", table_name_map={"Customer": "raw_customers"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    assert captured[-1]["resources"][0]["table_name"] == "raw_customers"
    df = result.output_for_node("test_asset", output_name="test_asset_Customer")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error():
    component = _build_component(resources="Customer", table_name_map={"Invoice": "raw_invoices"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
