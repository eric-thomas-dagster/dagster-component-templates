"""Committed tests for SnowflakeCopyIntoAssetComponent.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection whose cursor mimics COPY INTO's real per-file result set (file/status/
rows_parsed/rows_loaded/errors_seen columns via cursor.description + fetchall()). The component
runs for real under dg.materialize().
"""
import dagster as dg
import pytest
import snowflake.connector
from dagster_snowflake import SnowflakeResource

from .conftest import load_component_module


COPY_RESULT_COLUMNS = ["file", "status", "rows_parsed", "rows_loaded", "errors_seen"]


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = [(c,) for c in COPY_RESULT_COLUMNS]
        self._rows = [
            ("orders_1.csv", "LOADED", 100, 100, 0),
            ("orders_2.csv", "LOAD_FAILED", 50, 40, 10),
        ]

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs=None):
        self.kwargs = kwargs or {}
        self.executed = []
        self.commit_calls = 0
        self.close_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_calls += 1

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    return rec


def _comp(mod, **kw):
    kw.setdefault("asset_key", "snowflake/loads/orders")
    kw.setdefault("database", "DB")
    kw.setdefault("schema", "RAW")
    kw.setdefault("table", "ORDERS")
    kw.setdefault("stage", "orders_stage")
    kw.setdefault("account_env_var", "SNOWFLAKE_ACCOUNT")
    kw.setdefault("user_env_var", "SNOWFLAKE_USER")
    kw.setdefault("password_env_var", "SNOWFLAKE_PASSWORD")
    return mod.SnowflakeCopyIntoAssetComponent(**kw)


def test_requires_connection_or_resource_key(mod):
    with pytest.raises(Exception, match="resource_key"):
        mod.SnowflakeCopyIntoAssetComponent(asset_key="x/y", database="D", schema="S", table="T", stage="s")


def test_rejects_both_file_format_name_and_options(mod):
    with pytest.raises(Exception, match="file_format_name"):
        _comp(mod, file_format_name="FMT", file_format_options={"TYPE": "CSV"})


def test_stage_reference_adds_leading_at(mod):
    assert _comp(mod, stage="orders_stage")._build_copy_sql().split("\n")[1] == "FROM @orders_stage"
    assert _comp(mod, stage="@orders_stage")._build_copy_sql().split("\n")[1] == "FROM @orders_stage"


def test_stage_path_is_appended(mod):
    sql = _comp(mod, stage="orders_stage", stage_path="2026/01/")._build_copy_sql()
    assert "FROM @orders_stage/2026/01/" in sql


def test_file_format_options_rendered_with_quoting_rules(mod):
    comp = _comp(mod, file_format_options={"TYPE": "CSV", "FIELD_DELIMITER": "|", "SKIP_HEADER": 1})
    rendered = comp._build_copy_sql()
    assert "FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)" in rendered


def test_copy_options_rendered(mod):
    comp = _comp(mod, on_error="CONTINUE", purge=True, force=False, size_limit=1000)
    sql = comp._build_copy_sql()
    assert "ON_ERROR = 'CONTINUE'" in sql
    assert "PURGE = TRUE" in sql
    assert "FORCE = FALSE" in sql
    assert "SIZE_LIMIT = 1000" in sql


def test_files_and_pattern_rendered(mod):
    comp = _comp(mod, files=["a.csv", "b.csv"], pattern=r".*\.csv")
    sql = comp._build_copy_sql()
    assert "FILES = ('a.csv', 'b.csv')" in sql
    assert r"PATTERN = '.*\.csv'" in sql


def test_materialize_runs_copy_and_aggregates_result_metadata(mod, sf):
    asset_def = _comp(mod, file_format_options={"TYPE": "CSV"}).build_defs(None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    (conn,) = sf.conns
    assert conn.executed[0].startswith("COPY INTO DB.RAW.ORDERS")
    assert conn.commit_calls == 1
    assert conn.close_calls == 1

    mats = result.asset_materializations_for_node("snowflake__loads__orders")
    metadata = mats[0].metadata
    assert metadata["rows_loaded"].value == 140
    assert metadata["rows_parsed"].value == 150
    assert metadata["errors_seen"].value == 10
    assert metadata["file_count"].value == 2


def test_resource_path_uses_real_snowflake_resource(mod, sf, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT")  # inline path would raise if it were used
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    asset_def = mod.SnowflakeCopyIntoAssetComponent(
        asset_key="snowflake/loads/orders", database="DB", schema="RAW", table="ORDERS",
        stage="orders_stage", resource_key="snow",
    ).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"snow"}
    assert dg.materialize([asset_def], resources={"snow": res}).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct"
