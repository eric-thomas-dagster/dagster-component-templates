"""Real, committed tests for two gaps found in review feedback: dbt-mart /
warehouse-table exports had no DataFrame-returning upstream asset to read via
an IO manager, and one component instance covered exactly one export, so N
exports meant N instances.

1. `source_table` + `connection_url_env_var` query the warehouse directly
   (real SQLite through the component's own SQLAlchemy path, same convention
   as sql_transform's tests) instead of requiring an upstream asset.
   `partition_filter_column` pushes a single-partition-key filter into SQL.
2. `exports:` is a list of per-export overrides, one component instance
   producing N assets -- same mutually-exclusive single-vs-collection
   convention as sql_transform's transforms: / external_sftp_path's paths:.
"""
import pathlib

import dagster as dg
import pandas as pd
import pytest
import sqlalchemy

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_url(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'warehouse.db'}"
    eng = sqlalchemy.create_engine(url)
    pd.DataFrame({
        "id": [1, 2, 3, 4],
        "brand": ["acme", "acme", "globex", "globex"],
        "export_date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
    }).to_sql("orders", eng, index=False)
    eng.dispose()
    monkeypatch.setenv("TEST_DF_TO_CSV_URL", url)
    return url


def read(path):
    return pd.read_csv(path)


def _comp(mod, **kw):
    args = dict(connection_url_env_var="TEST_DF_TO_CSV_URL")
    args.update(kw)
    return mod.DataframeToCsvComponent(**args)


# ── source_table (warehouse-query mode) ──────────────────────────────────

def test_source_table_and_upstream_asset_key_both_set_is_rejected(mod, db_url, tmp_path):
    comp = _comp(mod, asset_name="out", upstream_asset_key="orders_src", source_table="orders", file_path=str(tmp_path / "o.csv"))
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_source_table_nor_upstream_asset_key_is_rejected(mod, db_url, tmp_path):
    comp = _comp(mod, asset_name="out", file_path=str(tmp_path / "o.csv"))
    with pytest.raises(ValueError, match="upstream_asset_key or source_table"):
        comp.build_defs(None)


def test_source_table_requires_connection_url_env_var(mod, tmp_path):
    comp = mod.DataframeToCsvComponent(asset_name="out", source_table="orders", file_path=str(tmp_path / "o.csv"))
    with pytest.raises(ValueError, match="connection_url_env_var"):
        comp.build_defs(None)


def test_source_table_queries_the_real_warehouse_table(mod, db_url, tmp_path):
    p = tmp_path / "out.csv"
    comp = _comp(mod, asset_name="orders_export", source_table="orders", file_path=str(p))
    asset_def = comp.build_defs(None).assets[0]
    dg.materialize([asset_def], raise_on_error=True)
    df = read(p)
    assert len(df) == 4


def test_partition_filter_column_pushes_filter_into_sql(mod, db_url, tmp_path):
    p = tmp_path / "{partition_key}.csv"
    comp = _comp(
        mod, asset_name="orders_export", source_table="orders", file_path=str(p),
        partition_filter_column="export_date", partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = comp.build_defs(None).assets[0]
    dg.materialize([asset_def], partition_key="2024-01-01", raise_on_error=True)
    out_path = tmp_path / "2024-01-01.csv"
    df = read(out_path)
    assert len(df) == 2
    assert set(df["export_date"]) == {"2024-01-01"}


# ── exports: (collection mode) ───────────────────────────────────────────

def test_asset_name_and_exports_both_set_is_rejected(mod, db_url, tmp_path):
    comp = _comp(mod, asset_name="out", upstream_asset_key="x", file_path=str(tmp_path / "o.csv"), exports=[
        {"asset_name": "a", "source_table": "orders", "file_path": str(tmp_path / "a.csv")},
    ])
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_asset_name_nor_exports_is_rejected(mod, db_url):
    comp = _comp(mod)
    with pytest.raises(ValueError, match="asset_name"):
        comp.build_defs(None)


def test_exports_produce_one_asset_each_sharing_the_connection(mod, db_url, tmp_path):
    comp = _comp(mod, exports=[
        {"asset_name": "acme_orders", "source_table": "orders", "file_path": str(tmp_path / "acme.csv")},
        {"asset_name": "globex_orders", "source_table": "orders", "file_path": str(tmp_path / "globex.csv")},
    ])
    defs = comp.build_defs(None)
    assets = list(defs.assets)
    assert len(assets) == 2
    dg.materialize(assets, raise_on_error=True)
    assert (tmp_path / "acme.csv").exists()
    assert (tmp_path / "globex.csv").exists()


def test_export_entry_missing_asset_name_is_rejected(mod, db_url, tmp_path):
    comp = _comp(mod, exports=[{"source_table": "orders", "file_path": str(tmp_path / "a.csv")}])
    with pytest.raises(ValueError, match="asset_name"):
        comp.build_defs(None)
