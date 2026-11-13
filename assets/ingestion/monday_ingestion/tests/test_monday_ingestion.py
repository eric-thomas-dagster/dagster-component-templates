"""Committed tests for MondayIngestionComponent, focused on the write_disposition fix for
the rerun-row-duplication bug (same bug/fix as sitemap_crawler_ingestion/firecrawl_ingestion/
linear_ingestion/etc. this session): dlt's own default write_disposition is "append", and
this component previously set none at all, so every rerun against a persistent destination
piled another full copy of every board/user/item on top of the last. This component had no
tests at all before this fix -- these are new.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeResponse, install_fake_dlt, install_fake_requests_post, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _build_component(mod, **overrides):
    defaults = dict(asset_name="test_asset", api_token="test-token", resources="boards", include_preview_metadata=False)
    defaults.update(overrides)
    return mod.MondayIngestionComponent(**defaults)


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def _boards_response(boards):
    return FakeResponse(json_body={"data": {"boards": boards}})


BOARD_A = {"id": "b-1", "name": "Board A", "state": "active", "board_kind": "public"}
BOARD_B = {"id": "b-2", "name": "Board B", "state": "active", "board_kind": "public"}


def test_single_asset_fetch_returns_one_row_per_board(mod, monkeypatch):
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A, BOARD_B]))
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert {r["id"] for r in rows} == {"b-1", "b-2"}


def test_default_write_disposition_is_replace(mod, monkeypatch):
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A]))
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "replace"
    md = result.asset_materializations_for_node("test_asset")[0].metadata
    assert md["write_disposition"].value == "replace"


def test_merge_write_disposition_sets_primary_key(mod, monkeypatch):
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A]))
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, write_disposition="merge")
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "merge"
    assert resource.compute_table_schema()["columns"]["id"]["primary_key"] is True


def test_append_write_disposition_is_explicit_opt_in(mod, monkeypatch):
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A]))
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, write_disposition="append")
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "append"


def test_readback_dedupes_duplicate_ids_keeping_latest_updated_at(mod, monkeypatch):
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A]))
    table_rows = {"boards": pd.DataFrame([
        {"id": "b-1", "name": "stale", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": "b-1", "name": "fresh", "updated_at": "2024-06-01T00:00:00Z"},
        {"id": "b-2", "name": "only", "updated_at": "2024-01-01T00:00:00Z"},
    ])}
    install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("test_asset")[0].metadata
    assert md["row_count"].value == 2
    assert md["duplicate_rows_removed"].value == 1
    df = result.output_for_node("test_asset")
    assert df.set_index("id").loc["b-1", "name"] == "fresh"


def test_dlt_internal_tables_are_excluded_from_readback(mod, monkeypatch):
    """Same real bug class dropbox_ingestion's fix caught: information_schema table
    discovery must not pick up dlt's own `_dlt_loads`/`_dlt_version` bookkeeping tables."""
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A]))
    table_rows = {
        "boards": pd.DataFrame([{"id": "b-1", "name": "Board A"}]),
        "_dlt_loads": pd.DataFrame([{"load_id": "123"}]),
        "_dlt_version": pd.DataFrame([{"version": 1}]),
    }
    install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 1
    assert "load_id" not in df.columns
    assert "version" not in df.columns


def test_dedupe_by_key_keeps_latest_order_col(mod):
    df = pd.DataFrame([
        {"id": "a", "updated_at": "2024-01-01", "v": "old"},
        {"id": "a", "updated_at": "2024-02-01", "v": "new"},
        {"id": "b", "created_at": "2024-01-01", "v": "b"},
    ])
    deduped, removed = mod._dedupe_by_key(df, "id", ("updated_at", "created_at"))
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("id").loc["a", "v"] == "new"


def test_dedupe_by_key_no_key_column_is_a_no_op(mod):
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = mod._dedupe_by_key(df, "id", ("updated_at",))
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_key_empty_dataframe_is_a_no_op(mod):
    df = pd.DataFrame()
    deduped, removed = mod._dedupe_by_key(df, "id", ())
    assert removed == 0
    assert len(deduped) == 0


# --- multi_asset shape: same write_disposition/dedupe fix applies there too ------------

def test_multi_asset_shape_default_write_disposition_and_dedupe(mod, monkeypatch):
    install_fake_requests_post(monkeypatch, mod, lambda url, json=None, headers=None, timeout=None: _boards_response([BOARD_A]))
    table_rows = {"boards": pd.DataFrame([
        {"id": "b-1", "name": "stale", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": "b-1", "name": "fresh", "updated_at": "2024-06-01T00:00:00Z"},
    ])}
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    component = _build_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, selection=["test_asset_boards"])
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "replace"
    md = result.asset_materializations_for_node("test_asset")[0].metadata
    assert md["row_count"].value == 1
    assert md["duplicate_rows_removed"].value == 1
    assert md["write_disposition"].value == "replace"
