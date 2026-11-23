"""Tests for DataframeToRedshiftComponent's if_exists='upsert' (ADF SQL sink parity, ask
11). The "target table doesn't exist yet" path runs for real against the `pg` fixture's
real SQLite engine (same as the rest of this suite). The "target exists -> runs a real
MERGE INTO" path uses a fake engine, since SQLite itself has no MERGE statement to run
the generated SQL against -- real-platform behavior is pinned by `build_merge_sql`'s own
pure-function tests in dataframe_to_snowflake's test suite (same inlined, drift-tested
block -- see tests/test_upsert_merge_sql_drift.py).
"""
import contextlib

import dagster as dg
import pandas as pd
import pytest
import sqlalchemy

from .test_dataframe_to_redshift import _comp, _flat_upstream, pg  # noqa: F401 -- reuse fixture + helpers


@pytest.fixture()
def mod():
    from .conftest import load_component_module
    return load_component_module()


def test_upsert_requires_key_columns(mod, pg):
    comp = _comp(mod, if_exists="upsert")
    defs = comp.build_defs(load_context=None)
    with pytest.raises(ValueError, match="upsert_key_columns"):
        dg.materialize([_flat_upstream(), *defs.assets])


def test_upsert_against_missing_table_renames_staging(mod, pg):
    comp = _comp(mod, if_exists="upsert", upsert_key_columns=["id"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    eng = pg.real(pg.sqlite_url)
    try:
        out = pd.read_sql_table("out", eng)
    finally:
        eng.dispose()
    assert sorted(out["id"].tolist()) == [1, 2, 3]


class _FakeConnection:
    def __init__(self, recorder):
        self.recorder = recorder

    def execute(self, stmt, *a, **k):
        self.recorder.append(str(stmt))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeEngine:
    def __init__(self, recorder):
        self.recorder = recorder

    def begin(self):
        return _FakeConnection(self.recorder)


def test_upsert_against_existing_table_runs_merge(mod, monkeypatch):
    executed = []
    real_create_engine = sqlalchemy.create_engine
    real_inspect = sqlalchemy.inspect

    def fake_create_engine(url, *a, **k):
        return _FakeEngine(executed) if str(url).startswith("postgresql") else real_create_engine(url, *a, **k)

    class _FakeInspector:
        def has_table(self, table, schema=None):
            return True

    def fake_inspect(engine):
        return _FakeInspector() if isinstance(engine, _FakeEngine) else real_inspect(engine)

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
    monkeypatch.setattr(sqlalchemy, "inspect", fake_inspect)
    for k, v in {"REDSHIFT_HOST": "h", "REDSHIFT_DATABASE": "d", "REDSHIFT_USER": "u", "REDSHIFT_PASSWORD": "p"}.items():
        monkeypatch.setenv(k, v)

    def _fake_to_sql(self, name, con, **kwargs):
        executed.append(f"TO_SQL {name}")

    monkeypatch.setattr(pd.DataFrame, "to_sql", _fake_to_sql)

    comp = mod.DataframeToRedshiftComponent(
        asset_name="to_rs", upstream_asset_key="orders_flat", table="out", schema="public",
        if_exists="upsert", upsert_key_columns=["id"],
    )
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    merge_statements = [s for s in executed if s.startswith("MERGE INTO")]
    assert len(merge_statements) == 1
    assert 'ON t."id" = s."id"' in merge_statements[0]
    assert any(s.startswith("DROP TABLE IF EXISTS") for s in executed)
