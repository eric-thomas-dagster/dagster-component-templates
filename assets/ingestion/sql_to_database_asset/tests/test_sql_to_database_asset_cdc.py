"""Tests for SQLToDatabaseAssetComponent's delete_column (true CDC apply: insert + update +
delete-by-key, not just upsert). Runs end to end against real temp-file SQLite databases,
same convention as the rest of this component's test suite -- nothing mocked.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from .test_sql_to_database_asset import _make_component, _materialize
from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _write_src(db_url, df, table="events"):
    engine = create_engine(db_url)
    df.to_sql(table, engine, index=False, if_exists="replace")
    engine.dispose()


def _read_dst(db_url, table):
    engine = create_engine(db_url)
    try:
        return pd.read_sql_table(table, engine).set_index("id").sort_index()
    finally:
        engine.dispose()


def test_delete_column_into_missing_table_skips_delete_flagged_rows(mod, tmp_path, monkeypatch):
    src_url = f"sqlite:///{tmp_path / 'src.db'}"
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    _write_src(src_url, pd.DataFrame({
        "id": [1, 2, 3], "total": [10.0, 20.0, 30.0], "is_deleted": [False, False, True],
    }))
    comp = _make_component(
        mod, src_url, dst_url, monkeypatch,
        if_exists="upsert", upsert_key_columns=["id"], delete_column="is_deleted",
    )
    assert _materialize(comp).success
    out = _read_dst(dst_url, "events_sink")
    assert sorted(out.index.tolist()) == [1, 2]


def test_delete_column_removes_matched_rows_and_skips_unmatched_delete(mod, tmp_path, monkeypatch):
    src_url = f"sqlite:///{tmp_path / 'src.db'}"
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    _write_src(src_url, pd.DataFrame({
        "id": [1, 2, 3], "total": [10.0, 20.0, 30.0], "is_deleted": [False, False, False],
    }))
    comp = _make_component(
        mod, src_url, dst_url, monkeypatch,
        if_exists="upsert", upsert_key_columns=["id"], delete_column="is_deleted",
    )
    assert _materialize(comp).success

    # Second sync: delete id=1 (existing), update id=2, insert id=4, and a
    # delete for id=99 which the destination never had (must stay absent).
    _write_src(src_url, pd.DataFrame({
        "id": [1, 2, 4, 99],
        "total": [10.0, 999.0, 40.0, 0.0],
        "is_deleted": [True, False, False, True],
    }))
    assert _materialize(comp).success
    out = _read_dst(dst_url, "events_sink")
    assert sorted(out.index.tolist()) == [2, 3, 4]
    assert out.loc[2, "total"] == 999.0
    assert out.loc[3, "total"] == 30.0  # untouched (not present in second sync)


def test_upsert_without_delete_column_is_unaffected(mod, tmp_path, monkeypatch):
    """Regression: existing insert+update behavior must be unchanged when delete_column unset."""
    src_url = f"sqlite:///{tmp_path / 'src.db'}"
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    _write_src(src_url, pd.DataFrame({"id": [1, 2], "total": [10.0, 20.0]}))
    comp = _make_component(mod, src_url, dst_url, monkeypatch, if_exists="upsert", upsert_key_columns=["id"])
    assert _materialize(comp).success

    _write_src(src_url, pd.DataFrame({"id": [2, 3], "total": [999.0, 30.0]}))
    assert _materialize(comp).success
    out = _read_dst(dst_url, "events_sink")
    assert sorted(out.index.tolist()) == [1, 2, 3]
    assert out.loc[2, "total"] == 999.0
