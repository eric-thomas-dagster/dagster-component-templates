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
        self.rowcount = 0

    def execute(self, sql, *a, **k):
        if self.conn.delay_seconds:
            import time
            time.sleep(self.conn.delay_seconds)
        self.conn.executed.append(sql)
        self.rowcount = 7 if sql.startswith("DELETE") else len(self._rows)

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs=None, delay_seconds=0):
        self.kwargs = kwargs or {}
        self.executed = []
        self.commit_calls = 0
        self.close_calls = 0
        self.delay_seconds = delay_seconds

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_calls += 1

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []
        self.delay_seconds = 0


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs, delay_seconds=rec.delay_seconds)
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
    # Unquoted -- matches Snowflake's own COPY INTO reference docs for ON_ERROR's keyword-enum
    # values (ON_ERROR = CONTINUE), not quoted like a generic string option.
    assert "ON_ERROR = CONTINUE" in sql
    assert "PURGE = TRUE" in sql
    assert "FORCE = FALSE" in sql
    assert "SIZE_LIMIT = 1000" in sql


def test_match_by_column_name_rendered_unquoted(mod):
    comp = _comp(mod, match_by_column_name="CASE_INSENSITIVE")
    sql = comp._build_copy_sql()
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in sql


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


def test_no_pool_by_default(mod, sf):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.op.pool is None


def test_pool_wires_through(mod, sf):
    asset_def = _comp(mod, pool="warehouse_slots").build_defs(None).assets[0]
    assert asset_def.op.pool == "warehouse_slots"


def test_retry_policy_wires_through(mod, sf):
    asset_def = _comp(
        mod, retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    ).build_defs(None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_seconds_fails_a_slow_copy(mod, sf):
    import time
    sf.delay_seconds = 1
    asset_def = _comp(mod, timeout_seconds=0.05).build_defs(None).assets[0]
    start = time.monotonic()
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 1


def test_loads_collection_builds_one_asset_per_entry(mod, sf):
    comp = mod.SnowflakeCopyIntoAssetComponent(
        database="DB", schema="RAW",
        account_env_var="SNOWFLAKE_ACCOUNT", user_env_var="SNOWFLAKE_USER", password_env_var="SNOWFLAKE_PASSWORD",
        loads=[
            {"asset_key": "snowflake/loads/orders", "table": "ORDERS", "stage": "orders_stage", "stage_path": "orders/"},
            {"asset_key": "snowflake/loads/customers", "table": "CUSTOMERS", "stage": "customers_stage", "stage_path": "customers/"},
        ],
    )
    defs = comp.build_defs(None)
    keys = {a.key for a in defs.assets}
    assert keys == {dg.AssetKey(["snowflake", "loads", "orders"]), dg.AssetKey(["snowflake", "loads", "customers"])}
    result = dg.materialize(list(defs.assets))
    assert result.success
    assert len(sf.conns) == 2
    executed_first_statements = {c.executed[0] for c in sf.conns}
    assert any(s.startswith("COPY INTO DB.RAW.ORDERS") for s in executed_first_statements)
    assert any(s.startswith("COPY INTO DB.RAW.CUSTOMERS") for s in executed_first_statements)


def test_loads_mutually_exclusive_with_single_instance_fields(mod):
    with pytest.raises(ValueError, match="not both"):
        mod.SnowflakeCopyIntoAssetComponent(
            asset_key="snowflake/loads/orders", database="DB", schema="RAW", table="ORDERS", stage="s",
            account_env_var="A", user_env_var="U",
            loads=[{"asset_key": "x", "table": "T", "stage": "s"}],
        ).build_defs(None)


def test_loads_entry_missing_required_field_raises(mod):
    comp = mod.SnowflakeCopyIntoAssetComponent(
        database="DB", schema="RAW",
        account_env_var="SNOWFLAKE_ACCOUNT", user_env_var="SNOWFLAKE_USER",
        loads=[{"asset_key": "snowflake/loads/orders", "table": "ORDERS"}],  # missing stage
    )
    with pytest.raises(ValueError, match="stage"):
        comp.build_defs(None)


def test_partition_key_substitution_in_stage_path(mod, sf):
    comp = _comp(mod, stage="orders_stage", stage_path="{partition_key}/", partition_type="daily", partition_start="2024-01-01")
    asset_def = comp.build_defs(None).assets[0]
    result = dg.materialize([asset_def], partition_key="2024-06-01")
    assert result.success
    (conn,) = sf.conns
    assert "FROM @orders_stage/2024-06-01/" in conn.executed[0]


def test_replace_partition_column_deletes_before_copy(mod, sf):
    comp = _comp(
        mod, stage="orders_stage", stage_path="{partition_key}/",
        partition_type="daily", partition_start="2024-01-01",
        replace_partition_column="load_date",
    )
    asset_def = comp.build_defs(None).assets[0]
    result = dg.materialize([asset_def], partition_key="2024-06-01")
    assert result.success
    (conn,) = sf.conns
    assert conn.executed[0] == "DELETE FROM DB.RAW.ORDERS WHERE load_date = '2024-06-01'"
    assert conn.executed[1].startswith("COPY INTO DB.RAW.ORDERS")
    mats = result.asset_materializations_for_node("snowflake__loads__orders")
    md = mats[0].metadata
    assert md["replace_partition_rows_deleted"].value == 7


def test_replace_partition_column_skipped_without_partition_key(mod, sf):
    """No partition on this run -- replace_partition_column must not DELETE the whole table."""
    comp = _comp(mod, stage="orders_stage", replace_partition_column="load_date")
    asset_def = comp.build_defs(None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    (conn,) = sf.conns
    assert len(conn.executed) == 1
    assert conn.executed[0].startswith("COPY INTO")


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
