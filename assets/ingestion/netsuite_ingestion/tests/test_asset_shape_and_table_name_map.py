"""Tests for NetSuiteIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as servicenow_ingestion (this component is structurally the closest to it in
the repo: a hand-built dlt.sources.rest_api.rest_api_source config with an existing
information_schema introspection loop for query-back, no verified source involved).

`table_name_map` is wired via the EndpointResource config's own `table_name` key
(distinct from `name`, confirmed real via `dlt.sources.rest_api.typing.EndpointResource
.__annotations__` -- the same mechanism servicenow_ingestion uses), not dlt's
`apply_hints`/`with_resources` (those are verified-source-only APIs; this component
builds its own rest_api_source config directly, like workday_ingestion).
"""
import importlib.util
import pathlib
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("netsuite_ingestion_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

NetSuiteIngestionComponent = _mod.NetSuiteIngestionComponent


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        account_id="1234567",
        consumer_key="ck",
        consumer_secret="cs",
        token_id="tid",
        token_secret="tsec",
        resources="customer,invoice",
    )
    defaults.update(overrides)
    return NetSuiteIngestionComponent(**defaults)


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
            df = match if match is not None else pd.DataFrame({"id": []})

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


def test_asset_shape_defaults_to_single():
    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_table_name_map_overrides_destination_table_in_single_shape(monkeypatch):
    captured = []
    _patch_dlt(monkeypatch, {"raw_customers": pd.DataFrame({"id": [1, 2, 3]})}, captured)

    component = _build_component(resources="customer", table_name_map={"customer": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resources_cfg = captured[-1]["resources"]
    assert resources_cfg[0]["name"] == "customer"
    assert resources_cfg[0]["table_name"] == "raw_customers"

    df = result.output_for_node("test_asset")
    assert len(df) == 3
    assert set(df["_resource_type"]) == {"customer"}


def test_multi_asset_shape_produces_one_asset_per_resource(monkeypatch):
    captured = []
    _patch_dlt(monkeypatch, {
        "customer": pd.DataFrame({"id": [1, 2]}),
        "invoice": pd.DataFrame({"id": [9]}),
    }, captured)

    component = _build_component(asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_customer"), dg.AssetKey("test_asset_invoice")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["customer", "invoice"]

    customer_df = result.output_for_node("test_asset", output_name="test_asset_customer")
    invoice_df = result.output_for_node("test_asset", output_name="test_asset_invoice")
    assert list(customer_df["id"]) == [1, 2]
    assert list(invoice_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_fetches_the_other(monkeypatch):
    captured = []
    _patch_dlt(monkeypatch, {
        "customer": pd.DataFrame({"id": [1, 2]}),
        "invoice": pd.DataFrame({"id": [9]}),
    }, captured)

    component = _build_component(asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_customer")],
    )
    assert result.success

    resource_names = [r["name"] for r in captured[-1]["resources"]]
    assert resource_names == ["customer"]  # invoice was never requested from NetSuite's API at all

    df = result.output_for_node("test_asset", output_name="test_asset_customer")
    assert list(df["id"]) == [1, 2]


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(monkeypatch):
    captured = []
    _patch_dlt(monkeypatch, {"raw_customers": pd.DataFrame({"id": [1, 2, 3]})}, captured)

    component = _build_component(
        resources="customer", asset_shape="multi_asset", table_name_map={"customer": "raw_customers"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    resources_cfg = captured[-1]["resources"]
    assert resources_cfg[0]["name"] == "customer"
    assert resources_cfg[0]["table_name"] == "raw_customers"

    df = result.output_for_node("test_asset", output_name="test_asset_customer")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error():
    component = _build_component(resources="customer", table_name_map={"invoice": "raw_invoices"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)


def test_partition_date_column_still_filters_rows_in_single_shape(monkeypatch):
    frame = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "lastmodifieddate": ["2024-01-01T10:00:00", "2024-01-01T18:00:00", "2024-01-02T10:00:00", "2024-01-02T18:00:00"],
    })
    captured = []
    _patch_dlt(monkeypatch, frame, captured)

    component = _build_component(
        resources="customer",
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="lastmodifieddate",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def],
        partition_key="2024-01-01",
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    df = result.output_for_node("test_asset")
    assert set(df["id"]) == {1, 2}
