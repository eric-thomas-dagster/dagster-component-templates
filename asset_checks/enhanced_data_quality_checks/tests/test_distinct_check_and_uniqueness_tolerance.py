"""Real tests for two gaps closed in `enhanced_data_quality_checks`:

1. A new `distinct_check` type (asserts a column's distinct value count/ratio
   falls within an expected range) -- there was no check type covering this;
   `uniqueness_check` only asserts strict zero-duplicates, not a count/ratio
   expectation.
2. Tolerance on `uniqueness_check`: each column config can now optionally set
   `max_duplicates` (absolute row count, default 0 -- preserves the original
   strict behavior) and/or `max_duplicate_pct` (percentage of total rows)
   instead of requiring exactly zero duplicates.

Nothing is mocked: dataframe-mode tests use real pandas DataFrames; the
database-mode test uses a real, temp-file-backed DuckDB via
`dagster_duckdb.DuckDBResource`, matching this component's existing test
conventions (see test_sweep_shape_checks.py).
"""
import importlib.util
import pathlib

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_distinct_tolerance", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


def _asset_component(**kwargs):
    c = EnhancedDataQualityChecks(**kwargs)
    c._asset_name = "orders"
    c._asset_key = _mod.AssetKey(["orders"])
    c._partitions_def_for_checks = None
    return c


def _only_check(checks):
    """Each of these tests configures exactly one check, so grab it
    positionally rather than by name (uniqueness_check's/distinct_check's
    auto-generated name may not contain the check-type substring, matching
    the same caution test_sweep_shape_checks.py already documents for
    uniqueness_check)."""
    assert len(checks) == 1, checks
    return checks[0]


def _fake_df_context():
    return dg.build_asset_check_context()


def _fake_db_context(duckdb_resource):
    return dg.build_asset_check_context(resources={"duckdb": duckdb_resource})


@pytest.fixture
def duckdb_resource(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    resource = DuckDBResource(database=db_path)
    with resource.get_connection() as conn:
        conn.execute("CREATE TABLE orders (id INTEGER, region VARCHAR)")
        conn.execute(
            "INSERT INTO orders VALUES "
            "(1, 'east'), (2, 'east'), (3, 'west'), (4, 'west'), (5, 'north')"
        )
    return resource


# ---------------------------------------------------------------------------
# distinct_check
# ---------------------------------------------------------------------------

def test_distinct_check_dataframe_passes_within_count_range():
    df = pd.DataFrame({"region": ["e", "e", "w", "n"]})
    c = _asset_component(
        data_source_type="dataframe",
        distinct_check=[{"column": "region", "min_distinct_count": 2, "max_distinct_count": 5}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is True
    assert result.metadata["distinct_count"].value == 3


def test_distinct_check_dataframe_fails_below_min_count():
    df = pd.DataFrame({"region": ["e", "e", "e", "e"]})
    c = _asset_component(
        data_source_type="dataframe",
        distinct_check=[{"column": "region", "min_distinct_count": 2}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False
    assert result.metadata["distinct_count"].value == 1


def test_distinct_check_dataframe_fails_above_max_count():
    df = pd.DataFrame({"region": ["e", "w", "n", "s"]})
    c = _asset_component(
        data_source_type="dataframe",
        distinct_check=[{"column": "region", "max_distinct_count": 2}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False
    assert result.metadata["distinct_count"].value == 4


def test_distinct_check_dataframe_pct_bounds():
    # 2 distinct values out of 4 rows = 50%
    df = pd.DataFrame({"region": ["e", "e", "w", "w"]})
    c = _asset_component(
        data_source_type="dataframe",
        distinct_check=[{"column": "region", "min_distinct_pct": 40.0, "max_distinct_pct": 60.0}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is True
    assert result.metadata["distinct_pct"].value == pytest.approx(50.0)


def test_distinct_check_dataframe_fails_above_max_pct():
    # 4 distinct values out of 4 rows = 100%, above a 60% ceiling
    df = pd.DataFrame({"region": ["e", "w", "n", "s"]})
    c = _asset_component(
        data_source_type="dataframe",
        distinct_check=[{"column": "region", "max_distinct_pct": 60.0}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False


def test_distinct_check_database_to_dataframe_mode_runs(duckdb_resource):
    c = _asset_component(
        database_resource_key="duckdb", table_name="orders",
        distinct_check=[{"column": "region", "min_distinct_count": 2}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_db_context(duckdb_resource))
    # orders has 3 distinct regions (east, west, north)
    assert result.passed is True
    assert result.metadata["distinct_count"].value == 3


def test_distinct_check_missing_column_fails_clearly():
    df = pd.DataFrame({"region": ["e", "w"]})
    c = _asset_component(
        data_source_type="dataframe",
        distinct_check=[{"column": "does_not_exist"}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False


# ---------------------------------------------------------------------------
# uniqueness_check tolerance (max_duplicates / max_duplicate_pct)
# ---------------------------------------------------------------------------

def test_uniqueness_check_default_tolerance_is_still_zero():
    """No max_duplicates set -> original strict (zero-duplicates) behavior preserved."""
    df = pd.DataFrame({"id": [1, 2, 2, 3]})
    c = _asset_component(data_source_type="dataframe", uniqueness_check=[{"columns": [{"column": "id"}]}])
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False


def test_uniqueness_check_max_duplicates_tolerates_some_duplication():
    # duplicate_count counts every row that belongs to a duplicate group (both
    # "2"s here, not just the extra occurrence beyond the first) -- so this is
    # 2 duplicate rows out of 4, tolerated by max_duplicates=2
    df = pd.DataFrame({"id": [1, 2, 2, 3]})
    c = _asset_component(
        data_source_type="dataframe",
        uniqueness_check=[{"columns": [{"column": "id", "max_duplicates": 2}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is True


def test_uniqueness_check_max_duplicates_still_fails_above_tolerance():
    # 3 duplicate rows (every "2"), max_duplicates=2 is not enough
    df = pd.DataFrame({"id": [1, 2, 2, 2]})
    c = _asset_component(
        data_source_type="dataframe",
        uniqueness_check=[{"columns": [{"column": "id", "max_duplicates": 2}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False


def test_uniqueness_check_max_duplicate_pct_tolerates_some_duplication():
    # 2 duplicate rows out of 4 = 50%, tolerated by a 60% ceiling
    df = pd.DataFrame({"id": [1, 2, 2, 3]})
    c = _asset_component(
        data_source_type="dataframe",
        uniqueness_check=[{"columns": [{"column": "id", "max_duplicates": 10, "max_duplicate_pct": 60.0}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is True


def test_uniqueness_check_max_duplicate_pct_fails_above_ceiling():
    # 2 duplicate rows out of 4 = 50%, above a 30% ceiling even though max_duplicates is generous
    df = pd.DataFrame({"id": [1, 1, 2, 2]})
    c = _asset_component(
        data_source_type="dataframe",
        uniqueness_check=[{"columns": [{"column": "id", "max_duplicates": 10, "max_duplicate_pct": 30.0}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False
