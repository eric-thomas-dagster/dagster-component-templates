"""Real, committed test for a bug an external demo-builder hit: a DuckDB
scalar query result (e.g. `SELECT COUNT(*)`) raised a TypeError when compared
against a plain int.

Root cause, confirmed by reading `_execute_database_query`'s DuckDB/Postgres
branch end to end: `conn.execute(query)` on a real `duckdb.DuckDBPyConnection`
has BOTH `fetchall` and `fetchone`. The code checked `hasattr(result,
'fetchall')` FIRST, so DuckDB always took that branch and got back
`result.fetchall()` unmodified -- `[(n,)]`, a list containing one 1-tuple --
never reaching the `fetchone` branch a few lines down, which already did the
correct single-value unwrap (`row[0]`) for drivers that lack `fetchall`.

Every caller throughout this component only unwraps ONE level
(`result[0] if result else 0`), e.g. `_execute_database_row_count` /
`_execute_database_null_check`: they expected either a bare scalar or a
single list element that IS the scalar, so they got the tuple `(n,)` itself
back and crashed the first time they compared or summed it against a plain
int (`min_rows <= (n,)` -> TypeError: '<=' not supported between instances
of 'int' and 'tuple').

Fixed at the root in `_execute_database_query`: the `fetchall` branch now
applies the same single-row/single-column unwrap the `fetchone` branch
already did, so both branches return the same shape for the same kind of
query -- a bare scalar for a single-value result, the full list of tuples
otherwise.

Uses a real, persistent (temp-file-backed) DuckDB database via
`dagster_duckdb.DuckDBResource` -- not mocked -- since the bug is specific to
DuckDB's actual `DuckDBPyConnection.execute(...)` return shape.
"""
import importlib.util
import pathlib
import types

import pytest
from dagster_duckdb import DuckDBResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_duckdb_scalar", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


@pytest.fixture
def duckdb_with_orders(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    resource = DuckDBResource(database=db_path)
    with resource.get_connection() as conn:
        conn.execute("CREATE TABLE orders (id INTEGER, customer_email VARCHAR)")
        conn.execute(
            "INSERT INTO orders VALUES (1, 'a@x.com'), (2, NULL), (3, 'c@x.com')"
        )
    return resource


def test_execute_database_query_unwraps_a_real_duckdb_count_to_a_bare_int(duckdb_with_orders):
    """This is the exact shape that used to crash downstream: [(n,)] instead of n."""
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    result = c._execute_database_query(duckdb_with_orders, "SELECT COUNT(*) FROM orders")
    assert result == 3
    assert isinstance(result, int)


def test_execute_database_query_still_returns_full_rows_for_multi_row_results(duckdb_with_orders):
    """Multi-row results must NOT be scalar-unwrapped -- only the genuine
    single-row/single-column case changes shape."""
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    result = c._execute_database_query(duckdb_with_orders, "SELECT id FROM orders ORDER BY id")
    assert result == [(1,), (2,), (3,)]


def test_row_count_check_against_real_duckdb_does_not_raise_and_compares_correctly(duckdb_with_orders):
    """End-to-end through the actual check path that crashed: row_count's
    min_rows <= row_count <= max_rows comparison against a real COUNT(*)."""
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    c._flat_config = {"min_rows": 1}  # read via _get_check_config_value

    # Resource stored as a plain INSTANCE attribute (SimpleNamespace, not a
    # custom class) -- DuckDBResource implements Dagster's resource
    # descriptor protocol (__get__), which recurses if the value is looked
    # up off a class dict instead of a bare instance dict.
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    result = c._execute_database_row_count(fake_context)
    assert result.passed is True


def test_null_check_against_real_duckdb_does_not_raise_and_sums_correctly(duckdb_with_orders):
    """The other confirmed instance of the same bug: null_check sums
    per-column counts, which crashed on `sum([(1,)])`-shaped values."""
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    c._flat_config = {"columns": ["customer_email"]}  # read via _get_check_config_value

    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders))

    result = c._execute_database_null_check(fake_context)
    assert result.passed is False  # one real NULL in customer_email
    assert "1" in result.description
