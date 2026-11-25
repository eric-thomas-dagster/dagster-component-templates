"""Committed tests for DataframeToDeltaTableComponent's CDC delete-by-key (delete_column)
and merge-mode auto-create. Nothing is mocked: every test writes to a REAL Delta table in
tmp_path (deltalake) and reads it back.
"""
import dagster as dg
import pandas as pd
import pytest
from deltalake import write_deltalake

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def table(uri):
    from deltalake import DeltaTable
    return DeltaTable(str(uri)).to_pandas().sort_values("id").reset_index(drop=True)


def merge_sink(mod, uri, **kw):
    kw.setdefault("mode", "merge")
    kw.setdefault("merge_predicate", "target.id = source.id")
    return mod.DataframeToDeltaTableComponent(
        asset_name="to_delta", upstream_asset_key="events_src", table_uri=str(uri), **kw
    ).build_defs(None).assets[0]


def test_merge_into_missing_table_auto_creates_and_skips_delete_flagged_rows(mod, tmp_path):
    uri = tmp_path / "fresh"
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"], "is_deleted": [False, False, True]})

    @dg.asset(name="events_src")
    def up() -> pd.DataFrame:
        return df

    sink = merge_sink(mod, uri, delete_column="is_deleted")
    r = dg.materialize([up, sink])
    assert r.success
    out = table(uri)
    assert out["id"].tolist() == [1, 2]


def test_merge_delete_by_key_removes_matched_rows_and_skips_unmatched_delete(mod, tmp_path):
    uri = tmp_path / "t"
    write_deltalake(
        str(uri),
        pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"], "is_deleted": [False, False, False]}),
    )
    incoming = pd.DataFrame({
        "id": [1, 2, 4, 99],
        "val": ["a", "b-upd", "d", "ghost"],
        "is_deleted": [True, False, False, True],
    })

    @dg.asset(name="events_src")
    def up() -> pd.DataFrame:
        return incoming

    sink = merge_sink(mod, uri, delete_column="is_deleted")
    r = dg.materialize([up, sink])
    assert r.success
    out = table(uri)
    # id=1 deleted (matched + flagged), id=2 updated, id=3 untouched, id=4 inserted,
    # id=99 never existed and was flagged for delete -- must never be inserted.
    assert out["id"].tolist() == [2, 3, 4]
    assert out.set_index("id").loc[2, "val"] == "b-upd"


def test_merge_without_delete_column_is_unaffected_plain_upsert(mod, tmp_path):
    """Regression: existing insert+update behavior must be unchanged when delete_column unset."""
    uri = tmp_path / "t2"
    write_deltalake(str(uri), pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}))

    @dg.asset(name="events_src")
    def up() -> pd.DataFrame:
        return pd.DataFrame({"id": [2, 3], "val": ["b-upd", "c"]})

    sink = merge_sink(mod, uri)
    r = dg.materialize([up, sink])
    assert r.success
    out = table(uri)
    assert out["id"].tolist() == [1, 2, 3]
    assert out.set_index("id").loc[2, "val"] == "b-upd"
