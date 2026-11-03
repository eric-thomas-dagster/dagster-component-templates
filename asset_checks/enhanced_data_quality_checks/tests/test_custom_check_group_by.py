"""Real, committed test for a missing-functionality gap: `custom_sql_check`
and `dataframe_query_check` silently ignored `group_by`/`allowed_failures`
even though both fields are already declared on `BaseCheckConfig` (which
`CustomSqlCheckConfig`/`DataframeQueryCheckConfig` inherit) and
`SharedCheckExecutor.execute_database_check`/`execute_dataframe_check`
already fully implement grouped execution.

`_execute_custom_dataframe_check` (the `custom_dataframe_check` / python_code
executor) already read `check_cfg.group_by`/`allowed_failures` and passed
them through correctly -- `_execute_custom_sql_check` and
`_execute_dataframe_query_check` never did, so configuring `group_by` on
either of those two check types was a silent no-op: the check always ran
ungrouped, with no error or warning.

Fixed by extracting `group_by`/`allowed_failures` from `check_cfg` in both
executors (matching the already-correct pattern in
`_execute_custom_dataframe_check`) and passing them through to the shared
executor.

Also fixed a real bug this surfaced in `_execute_grouped_database_check`,
which had never been exercised by anything (nothing passed `group_by` into
`execute_database_check` before this fix): it assumed the first row of a SQL
result was a header row (`pd.DataFrame(result[1:], columns=result[0])`), but
`_execute_database_query` returns plain DB-API row tuples with no column
names and no header row -- this silently dropped the first group and
mislabeled columns with the first data row's own values. Rewritten to the
only shape that can be interpreted without guessing at column identity: the
query must already GROUP BY in SQL and return exactly `(group, metric)` per
row.

Uses a real, temp-file-backed DuckDB database via `dagster_duckdb.DuckDBResource`
for the database-mode check, and a real pandas DataFrame for the
dataframe-mode check -- not mocked.
"""
import importlib.util
import pathlib
import types

import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_custom_check_group_by", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks
CustomSqlCheckConfig = _mod.CustomSqlCheckConfig
DataframeQueryCheckConfig = _mod.DataframeQueryCheckConfig


@pytest.fixture
def duckdb_with_orders(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    resource = DuckDBResource(database=db_path)
    with resource.get_connection() as conn:
        conn.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, amount DOUBLE)")
        conn.execute(
            "INSERT INTO orders VALUES "
            "(1, 'east', 10.0), (2, 'east', 20.0), "
            "(3, 'west', 5.0), (4, 'west', 1000.0), "
            "(5, 'north', 3.0)"
        )
    return resource


@pytest.fixture
def orders_df():
    return pd.DataFrame({
        "region": ["east", "east", "west", "west", "north"],
        "amount": [10.0, 20.0, 5.0, 1000.0, 3.0],
    })


def test_custom_sql_check_group_by_fails_when_a_group_exceeds_threshold(duckdb_with_orders):
    """west's $1005 total exceeds the <100 threshold; allowed_failures=0 so the
    overall check must fail even though 2/3 groups (east, north) pass."""
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT region, SUM(amount) FROM orders GROUP BY region",
        expected_result=100,
        comparison="less_than",
        group_by="region",
        allowed_failures=0,
    )
    result = c._execute_custom_sql_check(fake_context, check_cfg, "duckdb")
    assert result.passed is False
    assert "2/3 groups passed" in result.description


def test_custom_sql_check_group_by_passes_within_allowed_failures(duckdb_with_orders):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT region, SUM(amount) FROM orders GROUP BY region",
        expected_result=100,
        comparison="less_than",
        group_by="region",
        allowed_failures=1,
    )
    result = c._execute_custom_sql_check(fake_context, check_cfg, "duckdb")
    assert result.passed is True


def test_custom_sql_check_group_by_rejects_rows_that_are_not_group_metric_pairs(duckdb_with_orders):
    """A grouped query must already GROUP BY in SQL and return exactly
    (group, metric) per row -- there's no column-name metadata on raw DB-API
    rows to locate an arbitrary group_by-named column within a wider result."""
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT id, region, amount FROM orders",  # 3 columns, not (group, metric)
        expected_result=100,
        comparison="less_than",
        group_by="region",
        allowed_failures=0,
    )
    result = c._execute_custom_sql_check(fake_context, check_cfg, "duckdb")
    assert result.passed is False
    assert "must return exactly (group, metric)" in result.description


def test_dataframe_query_check_group_by_fails_when_a_group_exceeds_threshold(duckdb_with_orders, orders_df):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    check_cfg = DataframeQueryCheckConfig(
        query="df['amount'].sum()",
        expected_result=100,
        comparison="less_than",
        group_by="region",
        allowed_failures=0,
    )
    result = c._execute_dataframe_query_check(fake_context, orders_df, check_cfg)
    assert result.passed is False
    assert "2/3 groups passed" in result.description


def test_dataframe_query_check_group_by_passes_within_allowed_failures(duckdb_with_orders, orders_df):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    check_cfg = DataframeQueryCheckConfig(
        query="df['amount'].sum()",
        expected_result=100,
        comparison="less_than",
        group_by="region",
        allowed_failures=1,
    )
    result = c._execute_dataframe_query_check(fake_context, orders_df, check_cfg)
    assert result.passed is True
