"""Real, committed tests for SqlServerChangeTrackingAssetComponent.

Two layers:
- SQL-building helpers (_build_current_version_sql / _build_min_valid_
  version_sql / _build_full_load_sql / _build_incremental_sql) are tested
  directly as pure string builders -- the real T-SQL syntax (CHANGE_
  TRACKING_CURRENT_VERSION, CHANGE_TRACKING_MIN_VALID_VERSION,
  CHANGETABLE(CHANGES ...)) is verified against the real, stable SQL
  Server feature (unchanged since SQL Server 2008), not a web API that
  could drift -- there's no local SQL Server to run these against, so
  this is the honest bar: confirm the generated SQL text is correct.
- Orchestration (_read_changes, _get_last_ct_version, and the full asset
  via dg.materialize) is tested against a fake engine/connection whose
  .execute() returns scripted rows per query pattern, with a REAL
  dg.instance_for_test() so the cursor-persistence logic (reading back
  this asset's own prior materialization's ct_version metadata) is
  exercised for real, not faked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()


# --- SQL-building helpers ----------------------------------------------------

def test_current_version_sql():
    assert mod._build_current_version_sql() == "SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version"


def test_min_valid_version_sql():
    sql = mod._build_min_valid_version_sql("dbo", "orders")
    assert "CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.orders'))" in sql


def test_full_load_sql_selects_pk_then_other_columns():
    sql = mod._build_full_load_sql("dbo", "orders", ["order_id"], ["order_id", "customer_id", "total"])
    assert sql == "SELECT [order_id], [customer_id], [total] FROM [dbo].[orders]"


def test_incremental_sql_shape():
    sql = mod._build_incremental_sql("dbo", "orders", ["order_id"], ["order_id", "customer_id", "total"])
    assert "CHANGETABLE(CHANGES [dbo].[orders], :since_version) AS CT" in sql
    assert "CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION, CT.[order_id], T.[customer_id], T.[total]" in sql
    assert "LEFT JOIN [dbo].[orders] AS T ON CT.[order_id] = T.[order_id]" in sql
    assert "ORDER BY CT.SYS_CHANGE_VERSION" in sql


def test_incremental_sql_joins_on_every_pk_column():
    sql = mod._build_incremental_sql("dbo", "order_items", ["order_id", "line_no"], ["order_id", "line_no", "qty"])
    assert "CT.[order_id] = T.[order_id] AND CT.[line_no] = T.[line_no]" in sql


# --- fake engine for orchestration tests -------------------------------------

class _FakeResult:
    def __init__(self, rows, cols):
        self._rows, self._cols = rows, cols

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def keys(self):
        return self._cols


class _FakeConnection:
    def __init__(self, current_version, min_valid_version, incremental_rows, full_rows, columns):
        self.current_version = current_version
        self.min_valid_version = min_valid_version
        self.incremental_rows = incremental_rows
        self.full_rows = full_rows
        self.columns = columns
        self.executed = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params))
        if "CHANGE_TRACKING_CURRENT_VERSION" in sql:
            return _FakeResult([(self.current_version,)], ["current_version"])
        if "CHANGE_TRACKING_MIN_VALID_VERSION" in sql:
            return _FakeResult([(self.min_valid_version,)], ["min_valid_version"])
        if "CHANGETABLE" in sql:
            return _FakeResult(self.incremental_rows, ["SYS_CHANGE_OPERATION", "SYS_CHANGE_VERSION"] + self.columns)
        return _FakeResult(self.full_rows, self.columns)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn

    def dispose(self):
        pass


def _patch_engine(monkeypatch, conn):
    monkeypatch.setattr(mod, "_make_engine", lambda url: _FakeEngine(conn))


def _comp(**kw):
    args = dict(
        asset_name="orders_changes", connection_url_env_var="MSSQL_URL", schema_name="dbo", table_name="orders",
        primary_key_columns=["order_id"], columns=["order_id", "customer_id", "total"],
    )
    args.update(kw)
    return mod.SqlServerChangeTrackingAssetComponent(**args)


def test_first_run_does_a_full_load_and_tags_rows_as_insert(monkeypatch):
    conn = _FakeConnection(
        current_version=100, min_valid_version=0, incremental_rows=[],
        full_rows=[(1, 10, 99.5), (2, 11, 50.0)], columns=["order_id", "customer_id", "total"],
    )
    _patch_engine(monkeypatch, conn)
    monkeypatch.setenv("MSSQL_URL", "mssql+pyodbc://fake")

    comp = _comp()
    with dg.instance_for_test() as instance:
        result = dg.materialize(list(comp.build_defs(None).assets), instance=instance)
    assert result.success

    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["ct_version"].value == 100
    assert meta["previous_ct_version"].value == "none (first run)"
    assert meta["row_count"].value == 2
    assert any("CHANGETABLE" not in sql for sql, _ in conn.executed if "FROM [dbo].[orders]" in sql)


def test_second_run_uses_the_stored_version_for_an_incremental_read(monkeypatch):
    conn1 = _FakeConnection(current_version=100, min_valid_version=0, incremental_rows=[], full_rows=[(1, 10, 99.5)], columns=["order_id", "customer_id", "total"])
    _patch_engine(monkeypatch, conn1)
    monkeypatch.setenv("MSSQL_URL", "mssql+pyodbc://fake")

    comp = _comp()
    with dg.instance_for_test() as instance:
        dg.materialize(list(comp.build_defs(None).assets), instance=instance)

        conn2 = _FakeConnection(
            current_version=150, min_valid_version=50,
            incremental_rows=[("U", 101, 1, 10, 150.0)], full_rows=[], columns=["order_id", "customer_id", "total"],
        )
        monkeypatch.setattr(mod, "_make_engine", lambda url: _FakeEngine(conn2))
        result = dg.materialize(list(comp.build_defs(None).assets), instance=instance)
        assert result.success

        incremental_call = next(p for sql, p in conn2.executed if "CHANGETABLE" in sql)
        assert incremental_call == {"since_version": 100}

        meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
        assert meta["ct_version"].value == 150
        assert meta["previous_ct_version"].value == 100


def test_stale_cursor_falls_back_to_a_full_reload(monkeypatch):
    conn1 = _FakeConnection(current_version=100, min_valid_version=0, incremental_rows=[], full_rows=[(1, 10, 99.5)], columns=["order_id", "customer_id", "total"])
    _patch_engine(monkeypatch, conn1)
    monkeypatch.setenv("MSSQL_URL", "mssql+pyodbc://fake")

    comp = _comp()
    with dg.instance_for_test() as instance:
        dg.materialize(list(comp.build_defs(None).assets), instance=instance)

        # min_valid_version (200) is now PAST the stored cursor (100) -- history was cleaned up.
        conn2 = _FakeConnection(
            current_version=300, min_valid_version=200,
            incremental_rows=[("U", 250, 1, 999.0)], full_rows=[(1, 10, 99.5), (2, 11, 50.0)],
            columns=["order_id", "customer_id", "total"],
        )
        monkeypatch.setattr(mod, "_make_engine", lambda url: _FakeEngine(conn2))
        result = dg.materialize(list(comp.build_defs(None).assets), instance=instance)
        assert result.success

        assert not any("CHANGETABLE" in sql for sql, _ in conn2.executed)
        meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
        assert meta["row_count"].value == 2
        assert meta["ct_version"].value == 300


def test_operation_counts_recorded_in_metadata(monkeypatch):
    conn = _FakeConnection(
        current_version=1, min_valid_version=0, incremental_rows=[],
        full_rows=[(1, 10, 99.5), (2, 11, 50.0), (3, 12, 1.0)], columns=["order_id", "customer_id", "total"],
    )
    _patch_engine(monkeypatch, conn)
    monkeypatch.setenv("MSSQL_URL", "mssql+pyodbc://fake")
    comp = _comp()
    with dg.instance_for_test() as instance:
        result = dg.materialize(list(comp.build_defs(None).assets), instance=instance)
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["operation_counts"].data == {"I": 3}


def test_primary_key_not_listed_in_columns_is_rejected():
    comp = _comp(columns=["customer_id", "total"])  # missing order_id
    with pytest.raises(ValueError, match="must also be listed in columns"):
        comp.build_defs(None)


def test_missing_connection_env_var_raises_clearly(monkeypatch):
    monkeypatch.delenv("MSSQL_URL", raising=False)
    comp = _comp()
    with dg.instance_for_test() as instance:
        result = dg.materialize(list(comp.build_defs(None).assets), instance=instance, raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "MSSQL_URL" in str(failure.event_specific_data.error)
