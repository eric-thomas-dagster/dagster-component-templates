"""Tests for DataframeToDatabricksComponent's delete_column (true CDC apply: insert +
update + delete-by-key, not just upsert). Only the databricks-sql-connector wire boundary
is faked, same convention as test_dataframe_to_databricks_upsert.py.
"""
import dagster as dg
import pandas as pd
import pytest

from .test_dataframe_to_databricks import _comp
from .test_dataframe_to_databricks_upsert import dbx  # noqa: F401 -- reuse the fixture


@pytest.fixture()
def mod():
    from .conftest import load_component_module
    return load_component_module()


def _cdc_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({
            "id": [1, 2, 3],
            "v": ["a", "b", "c"],
            "is_deleted": [False, False, True],
        })
    return orders_flat


def test_delete_column_against_missing_table_excludes_flagged_rows_from_staging(mod, dbx):
    dbx["target_exists"] = False
    comp = _comp(mod, mode="upsert", upsert_key_columns=["id"], delete_column="is_deleted")
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_cdc_upstream(), *defs.assets])
    assert result.success
    conn = dbx["conns"][0]
    assert any("RENAME TO" in s for s in conn.statements)
    assert not any(s.startswith("MERGE INTO") for s in conn.statements)
    assert not any(s.startswith("DELETE FROM") for s in conn.statements)
    # id=3 (is_deleted=True) must never reach the staging table's INSERT rows.
    inserted_ids = sorted(r[0] for r in conn.rows)
    assert inserted_ids == [1, 2]


def test_delete_column_against_existing_table_deletes_then_merges(mod, dbx):
    dbx["target_exists"] = True
    comp = _comp(mod, mode="upsert", upsert_key_columns=["id"], delete_column="is_deleted")
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_cdc_upstream(), *defs.assets])
    assert result.success
    conn = dbx["conns"][0]
    deletes = [s for s in conn.statements if s.startswith("DELETE FROM main.sc.out ")]
    assert len(deletes) == 1
    assert "is_deleted" in deletes[0]
    stage_deletes = [s for s in conn.statements if s.startswith("DELETE FROM main.sc.out_stage_")]
    assert len(stage_deletes) == 1
    merges = [s for s in conn.statements if s.startswith("MERGE INTO")]
    assert len(merges) == 1
    # All 3 source rows (including the delete-flagged one) reach staging --
    # the delete-flag filtering happens via SQL against staging, not pandas,
    # once the target already exists.
    inserted_ids = sorted(r[0] for r in conn.rows)
    assert inserted_ids == [1, 2, 3]


def test_upsert_without_delete_column_is_unaffected(mod, dbx):
    """Regression: existing upsert behavior (no DELETE statements at all) must be
    unchanged when delete_column is unset."""
    dbx["target_exists"] = True
    from .test_dataframe_to_databricks import _flat_upstream
    comp = _comp(mod, mode="upsert", upsert_key_columns=["id"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    conn = dbx["conns"][0]
    assert not any(s.startswith("DELETE FROM main.sc.out ") for s in conn.statements)
    assert any(s.startswith("MERGE INTO") for s in conn.statements)
