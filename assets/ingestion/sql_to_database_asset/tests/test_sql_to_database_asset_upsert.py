"""Tests for SQLToDatabaseAssetComponent's if_exists='upsert' (ADF SQL sink parity, ask
11). Runs end to end against real temp-file SQLite databases (same convention as the rest
of this component's test suite) -- SQLite genuinely supports the INSERT...ON CONFLICT
syntax this dialect branch generates, so this exercises real upsert behavior, not a fake.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from .test_sql_to_database_asset import _make_component, _materialize, src_db  # noqa: F401
from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _read_table(db_url, table):
    engine = create_engine(db_url)
    try:
        return pd.read_sql_table(table, engine)
    finally:
        engine.dispose()


def test_upsert_requires_key_columns(mod, src_db, tmp_path, monkeypatch):
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    comp = _make_component(mod, src_db, dst_url, monkeypatch, if_exists="upsert")
    with pytest.raises(ValueError, match="upsert_key_columns"):
        _materialize(comp)


def test_upsert_creates_table_on_first_write(mod, src_db, tmp_path, monkeypatch):
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    comp = _make_component(mod, src_db, dst_url, monkeypatch, if_exists="upsert", upsert_key_columns=["id"])
    result = _materialize(comp)
    assert result.success
    out = _read_table(dst_url, "events_sink")
    assert sorted(out["id"].tolist()) == [1, 2, 3, 4, 5, 6]


def test_upsert_second_run_updates_matched_rows_and_inserts_new(mod, src_db, tmp_path, monkeypatch):
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    comp = _make_component(mod, src_db, dst_url, monkeypatch, if_exists="upsert", upsert_key_columns=["id"])
    assert _materialize(comp).success

    # Change an existing row's value and add a new one in the source, then re-sync.
    src_engine = create_engine(src_db)
    with src_engine.begin() as conn:
        conn.execute(text("UPDATE events SET total = 999.0 WHERE id = 1"))
        conn.execute(text("INSERT INTO events (id, total, event_date) VALUES (7, 70.0, '2024-01-05T00:00:00')"))
    src_engine.dispose()

    assert _materialize(comp).success
    out = _read_table(dst_url, "events_sink").set_index("id")
    assert out.loc[1, "total"] == 999.0  # updated in place, not duplicated
    assert out.loc[7, "total"] == 70.0  # newly inserted
    assert sorted(out.index.tolist()) == [1, 2, 3, 4, 5, 6, 7]


# ---------------------------------------------------------------------------
# build_dialect_upsert_sql -- pure function, per dialect
# ---------------------------------------------------------------------------


def test_postgres_dialect_uses_on_conflict(mod):
    sql = mod.build_dialect_upsert_sql("postgresql", None, "t", "t_stage", ["id", "v"], ["id"])
    assert "ON CONFLICT (\"id\") DO UPDATE SET \"v\" = EXCLUDED.\"v\"" in sql


def test_mysql_dialect_uses_on_duplicate_key_update(mod):
    sql = mod.build_dialect_upsert_sql("mysql", None, "t", "t_stage", ["id", "v"], ["id"])
    assert "ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)" in sql


def test_snowflake_dialect_uses_merge(mod):
    sql = mod.build_dialect_upsert_sql("snowflake", None, "t", "t_stage", ["id", "v"], ["id"])
    assert sql.startswith("MERGE INTO")


def test_unknown_dialect_raises(mod):
    with pytest.raises(ValueError, match="no known syntax"):
        mod.build_dialect_upsert_sql("cockroachdb", None, "t", "t_stage", ["id", "v"], ["id"])
