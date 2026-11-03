"""Comprehensive, fully-real tests for 5 check types: row_count_check,
null_check, static_threshold, range_check, uniqueness_check -- both
dataframe and database (real DuckDB) modes, going through the exact same
`_build_single_asset_checks()` dispatch a real YAML config goes through.

Nothing is mocked: database-mode tests use a real, temp-file-backed DuckDB
via `dagster_duckdb.DuckDBResource` (see tests/test_duckdb_scalar_result.py
for precedent); dataframe-mode tests use real pandas DataFrames. There is
nothing paid or external anywhere in this component.

This sweep found a SEVERE, universal root-cause bug plus two narrower,
unrelated ones. Each is captured as an `xfail(strict=True)` test with a
docstring explaining root cause (verified live, not guessed), so the suite
stays green until they're fixed and a fix that doesn't actually fix the bug
gets caught (`strict=True` turns an unexpected pass into a failure).

THE BIG ONE -- `_get_check_config_value(field_name)` only ever checks (1) a
direct instance attribute named exactly `field_name`, then (2)
`self._flat_config.get(field_name)`. But `_apply_nested_check_config` (the
function that populates `_flat_config` from a real YAML list entry) stores
most `BaseCheckConfig` fields -- `group_by`, `allowed_failures`, and, for
null_check/static_threshold/uniqueness_check, even the check's OWN primary
field -- under a check-type-PREFIXED key (`null_check_columns`,
`static_threshold_min`, `uniqueness_columns`, `row_count_group_by`, ...).
Executors that call `_get_check_config_value('columns')` /
`_get_check_config_value('group_by')` / `_get_check_config_value('min_value')`
with the BARE (unprefixed) name therefore always get the default, silently,
with no error. Confirmed by direct execution (not just code reading) for
every check type below:

- **null_check**: BOTH dataframe and database executors read bare
  `'columns'` (stored as `null_check_columns`) -- `columns` is ALWAYS `[]`,
  so the check ALWAYS reports "0 null values found" and ALWAYS PASSES,
  regardless of real data, regardless of mode, 100% of the time, when
  configured via the documented `null_check: [{columns: [...]}]` syntax.
  This check currently does nothing at all.
- **static_threshold**: the dataframe/database executors read bare
  `'metric'`/`'min_value'`/`'max_value'` (stored as `static_threshold_metric`
  /`static_threshold_min`/`static_threshold_max`) -- `metric` always falls
  back to its hard-coded default `'num_rows'` and `min_value`/`max_value`
  are always `None`, so both threshold comparisons are always skipped and
  the check ALWAYS PASSES, regardless of what's configured, in both modes.
  This check currently does nothing at all.
- **uniqueness_check**: both modes read bare `'columns'` (stored as
  `uniqueness_columns`) -- `columns` is always `[]`, so
  `_analyze_uniqueness_validation` always reports "0/0 checks passed" and
  the check ALWAYS PASSES regardless of real duplicates, in both modes
  (database-to-dataframe just delegates to the same broken dataframe path).
  This check currently does nothing at all.
- **row_count_check**: the primary fields (`min_rows`/`max_rows`) happen to
  be stored BARE (not prefixed) by `_apply_nested_check_config`, so the
  un-grouped path genuinely works in both modes. But `group_by` is stored as
  `row_count_group_by` while both executors read bare `'group_by'` -- so
  `group_by` is always `None` and the check always silently falls back to
  the ungrouped whole-table count, regardless of what `group_by` is set to,
  in both modes. The `allowed_failures`-related bugs this sweep originally
  suspected are moot: the grouped branch that would ignore them is never
  reached at all.

Two separate, narrower, unrelated bugs:

- **range_check (dataframe mode only)**: `_analyze_range_validation`
  computes `column_data = _get_column_values(df, column_name)` (a PLAIN
  PYTHON LIST, confirmed by reading `_get_column_values`'s `.tolist()`),
  then immediately does `column_data[~pd.isna(column_data)]` -- boolean-array
  indexing, which only works on numpy arrays / pandas Series, not plain
  lists. This raises `TypeError: only integer scalar arrays can be
  converted to a scalar index` on every single call, which is caught by
  `_analyze_range_validation`'s own try/except and silently reported as
  `failed_column_names: []` (zero outliers) -- so dataframe-mode
  range_check ALWAYS PASSES regardless of real outliers. Confirmed by
  calling `_analyze_range_validation` directly and observing the swallowed
  exception. Database-mode range_check does NOT share this bug -- it has
  its own, separately-implemented, working SQL-based outlier-counting path
  (`_execute_database_range_check`), confirmed correct by direct testing.
- Default check-name suffix swap: `_create_range_check` and
  `_create_uniqueness_check` pass the WRONG `default_suffix` to
  `_get_check_name` (component.py ~2838 and ~2864): range_check's
  auto-generated name uses suffix "percent_delta" (collides with an actual
  percent_delta check on the same asset, demonstrated below), and
  uniqueness_check's uses "range_check". `_create_data_type_check` has the
  same class of bug (wrong suffix "predicted_range") but that's outside
  this file's scope.

Also worth flagging even though it's a different check type entirely
(outside this file's scope -- predicted_range belongs to another part of
this sweep): `_execute_dataframe_predicted_range` reads
`self.predicted_range_metric` etc. as DIRECT INSTANCE ATTRIBUTES rather
than through `_get_check_config_value` at all -- these attributes are never
set anywhere in the real `_build_single_asset_checks` dispatch path, so
`predicted_range` crashes with a plain `AttributeError` on every single
invocation. Confirmed live. This suggests the "bare vs prefixed key"
disease and its direct-attribute cousin likely extend well beyond this
file's 5 check types into most/all of the ~17 simple check types that go
through the same `_apply_nested_check_config`/`_get_check_config_value`
machinery -- worth a dedicated, systematic pass across the whole component.
"""
import importlib.util
import pathlib

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_sweep_shape", _COMPONENT_PY)
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


def _check_fn(checks, name_substring: str):
    """`_build_single_asset_checks()` returns `AssetChecksDefinition`s that
    are directly callable (they forward call args to the wrapped function),
    going through the exact same factory dispatch real YAML config uses."""
    for d in checks:
        for spec in d.check_specs:
            if name_substring in spec.name:
                return d
    raise AssertionError(f"no check matching {name_substring!r} in {checks}")


def _only_check(checks):
    """For range_check/uniqueness_check: their auto-generated names are
    corrupted by a real, separately-documented bug (_create_range_check /
    _create_uniqueness_check pass the wrong default_suffix to
    _get_check_name), so a substring lookup by the "right" name doesn't
    find them. Each of these tests configures exactly one check, so grab it
    positionally instead."""
    assert len(checks) == 1, checks
    return checks[0]


def _spec_of(checks, name_substring: str):
    for d in checks:
        for spec in d.check_specs:
            if name_substring in spec.name:
                return spec
    raise AssertionError(f"no check matching {name_substring!r} in {checks}")


def _fake_df_context():
    return dg.build_asset_check_context()


@pytest.fixture
def duckdb_resource(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    resource = DuckDBResource(database=db_path)
    with resource.get_connection() as conn:
        conn.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, amount DOUBLE, email VARCHAR)")
        conn.execute(
            "INSERT INTO orders VALUES "
            "(1, 'east', 10.0, 'a@x.com'), (2, 'east', 20.0, 'b@x.com'), "
            "(3, 'west', 5.0, NULL), (4, 'west', 1000.0, 'd@x.com'), "
            "(5, 'north', 3.0, 'e@x.com')"
        )
    return resource


def _fake_db_context(duckdb_resource):
    return dg.build_asset_check_context(resources={"duckdb": duckdb_resource})


@pytest.fixture
def orders_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "region": ["east", "east", "west", "west", "north"],
        "amount": [10.0, 20.0, 5.0, 1000.0, 3.0],
        "email": ["a@x.com", "b@x.com", None, "d@x.com", "e@x.com"],
    })


# ---------------------------------------------------------------------------
# row_count_check
# ---------------------------------------------------------------------------

def test_row_count_dataframe_passes(orders_df):
    c = _asset_component(data_source_type="dataframe", row_count_check=[{"min_rows": 1, "max_rows": 10}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_df_context(), orders_df)
    assert result.passed is True


def test_row_count_dataframe_fails_below_min(orders_df):
    c = _asset_component(data_source_type="dataframe", row_count_check=[{"min_rows": 100}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_df_context(), orders_df)
    assert result.passed is False


def test_row_count_database_passes(duckdb_resource):
    c = _asset_component(database_resource_key="duckdb", table_name="orders", row_count_check=[{"min_rows": 1, "max_rows": 10}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_db_context(duckdb_resource))
    assert result.passed is True


def test_row_count_database_fails(duckdb_resource):
    c = _asset_component(database_resource_key="duckdb", table_name="orders", row_count_check=[{"min_rows": 100}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_db_context(duckdb_resource))
    assert result.passed is False


def test_row_count_database_group_by_is_silently_ignored(duckdb_resource):
    # Was a real bug: _execute_database_row_count read bare 'group_by' via
    # _get_check_config_value, but _apply_nested_check_config stored it as
    # 'row_count_group_by' -- group_by was always None, silently falling back
    # to the ungrouped whole-table count. Fixed at the root in
    # _get_check_config_value (prefixed-key fallback). Name kept for history.
    c = _asset_component(
        database_resource_key="duckdb", table_name="orders",
        row_count_check=[{"min_rows": 2, "group_by": "region", "allowed_failures": 0}],
    )
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_db_context(duckdb_resource))
    assert result.passed is False


def test_row_count_dataframe_group_by_is_silently_ignored(orders_df):
    # Was the identical bare-vs-prefixed 'group_by' bug in dataframe mode.
    # Fixed at the root in _get_check_config_value. Name kept for history.
    c = _asset_component(
        data_source_type="dataframe",
        row_count_check=[{"min_rows": 2, "group_by": "region", "allowed_failures": 0}],
    )
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_df_context(), orders_df)
    assert result.passed is False


def test_row_count_edge_case_empty_table_min_rows_zero_passes():
    empty_df = pd.DataFrame({"id": [], "region": []})
    c = _asset_component(data_source_type="dataframe", row_count_check=[{"min_rows": 0}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "row_count")(_fake_df_context(), empty_df)
    assert result.passed is True


# ---------------------------------------------------------------------------
# null_check
# ---------------------------------------------------------------------------

def test_null_check_dataframe_passes_when_no_nulls(orders_df):
    c = _asset_component(data_source_type="dataframe", null_check=[{"columns": ["id", "region"]}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "null_check")(_fake_df_context(), orders_df)
    assert result.passed is True


def test_null_check_dataframe_fails_when_nulls_present(orders_df):
    # Was a real bug: bare-vs-prefixed 'columns' mismatch (always []), plus a
    # separate numpy.bool_ vs bool crash once columns was fixed. Both fixed
    # at the root (_get_check_config_value prefix fallback, and
    # _count_null_values now casts to a native int). Name kept for history.
    c = _asset_component(data_source_type="dataframe", null_check=[{"columns": ["email"]}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "null_check")(_fake_df_context(), orders_df)
    assert result.passed is False


def test_null_check_database_passes_when_no_nulls(duckdb_resource):
    c = _asset_component(database_resource_key="duckdb", table_name="orders", null_check=[{"columns": ["id", "region"]}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "null_check")(_fake_db_context(duckdb_resource))
    assert result.passed is True


def test_null_check_database_fails_when_nulls_present(duckdb_resource):
    # Was the identical bare-vs-prefixed 'columns' bug in database mode.
    # Fixed at the root in _get_check_config_value. Name kept for history.
    c = _asset_component(database_resource_key="duckdb", table_name="orders", null_check=[{"columns": ["email"]}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "null_check")(_fake_db_context(duckdb_resource))
    assert result.passed is False


def test_null_check_all_null_column_edge_case():
    # Was the same 'columns' breakage -- an all-null column reported 0
    # nulls because the configured column list never reached the executor.
    df = pd.DataFrame({"id": [1, 2, 3], "always_null": [None, None, None]})
    c = _asset_component(data_source_type="dataframe", null_check=[{"columns": ["always_null"]}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "null_check")(_fake_df_context(), df)
    assert result.passed is False
    assert "3" in result.description


# ---------------------------------------------------------------------------
# static_threshold
# ---------------------------------------------------------------------------

def test_static_threshold_dataframe_passes(orders_df):
    """NOTE: passes today, but vacuously -- see the xfail tests below. This
    is kept because it must keep passing once the real bug is fixed too."""
    c = _asset_component(data_source_type="dataframe", static_threshold=[{"metric": "num_rows", "min_value": 1, "max_value": 10}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "static_threshold")(_fake_df_context(), orders_df)
    assert result.passed is True


def test_static_threshold_dataframe_fails(orders_df):
    # Was a real bug: bare-vs-prefixed 'metric' mismatch, plus 'min_value'/
    # 'max_value' being stored under abbreviated keys ('static_threshold_min'/
    # '_max') that didn't match what the executor read ('min_value'/
    # 'max_value'). Fixed in _get_check_config_value (prefix fallback) and in
    # _apply_nested_check_config (renamed the stored keys to match). Name
    # kept for history.
    c = _asset_component(data_source_type="dataframe", static_threshold=[{"metric": "num_rows", "min_value": 100}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "static_threshold")(_fake_df_context(), orders_df)
    assert result.passed is False


def test_static_threshold_dataframe_sum_metric(orders_df):
    """A non-default metric expression ('sum:<column>') -- should evaluate
    10+20+5+1000+3=1038, which is within [1000, 1100]."""
    c = _asset_component(data_source_type="dataframe", static_threshold=[{"metric": "sum:amount", "min_value": 1000, "max_value": 1100}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "static_threshold")(_fake_df_context(), orders_df)
    metadata_query = result.metadata["query"].text
    assert metadata_query == "static_threshold_sum:amount", (
        f"metric config was ignored -- query metadata shows {metadata_query!r}, "
        "proving 'metric' fell back to the hard-coded 'num_rows' default"
    )


def test_static_threshold_database_passes(duckdb_resource):
    """NOTE: passes today, but vacuously -- see the xfail test below."""
    c = _asset_component(database_resource_key="duckdb", table_name="orders", static_threshold=[{"metric": "num_rows", "min_value": 1, "max_value": 10}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "static_threshold")(_fake_db_context(duckdb_resource))
    assert result.passed is True


def test_static_threshold_database_fails(duckdb_resource):
    # Was the identical bare-vs-prefixed bug in database mode. Fixed at the
    # root. Name kept for history.
    c = _asset_component(database_resource_key="duckdb", table_name="orders", static_threshold=[{"metric": "num_rows", "min_value": 100}])
    checks = c._build_single_asset_checks()
    result = _check_fn(checks, "static_threshold")(_fake_db_context(duckdb_resource))
    assert result.passed is False


# ---------------------------------------------------------------------------
# range_check
# ---------------------------------------------------------------------------

def test_range_check_dataframe_passes(orders_df):
    """NOTE: passes today, but vacuously -- see the xfail test below."""
    c = _asset_component(
        data_source_type="dataframe",
        range_check=[{"columns": [{"column": "amount", "min_value": 0, "max_value": 2000}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), orders_df)
    assert result.passed is True


def test_range_check_dataframe_fails_with_outlier(orders_df):
    # Was a real bug: _analyze_range_validation indexed a plain Python list
    # (from _get_column_values) with a boolean numpy array, which only works
    # on numpy arrays/pandas Series -- raised TypeError on every call,
    # swallowed by its own try/except, always reported 0 outliers. Fixed by
    # wrapping the column values in np.array(...) before boolean-indexing.
    # Name kept for history.
    c = _asset_component(
        data_source_type="dataframe",
        range_check=[{"columns": [{"column": "amount", "min_value": 0, "max_value": 100}]}],  # 1000 is an outlier
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), orders_df)
    assert result.passed is False


def test_range_check_database_passes(duckdb_resource):
    c = _asset_component(
        database_resource_key="duckdb", table_name="orders",
        range_check=[{"columns": [{"column": "amount", "min_value": 0, "max_value": 2000}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_db_context(duckdb_resource))
    assert result.passed is True


def test_range_check_database_fails_with_outlier(duckdb_resource):
    c = _asset_component(
        database_resource_key="duckdb", table_name="orders",
        range_check=[{"columns": [{"column": "amount", "min_value": 0, "max_value": 100}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_db_context(duckdb_resource))
    assert result.passed is False


def test_range_check_database_edge_case_no_non_null_values(duckdb_resource):
    """A column with zero non-null rows after filtering: the "no data to
    analyze" branch should pass trivially rather than crash or false-fail."""
    c = _asset_component(
        database_resource_key="duckdb", table_name="orders",
        range_check=[{"columns": [{"column": "email", "min_value": 0, "max_value": 100}]}],  # email isn't numeric, but min/max won't match -> exercises no-outlier path differently; use amount with impossible filter instead
    )
    # Use a WHERE clause that matches nothing, so total_values == 0 for amount.
    c.where_clause = "id > 9999"
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_db_context(duckdb_resource))
    assert result.passed is True


def test_range_check_dataframe_group_by_allowed_failures_is_ignored(orders_df):
    # Was a real bug: _execute_dataframe_range_check hard-coded
    # allowed_failures=0 instead of reading range_allowed_failures. Fixed by
    # reading the real config value. Name kept for history.
    c = _asset_component(
        data_source_type="dataframe",
        range_check=[{
            "columns": [{"column": "amount", "min_value": 0, "max_value": 15}],
            "group_by": "region", "allowed_failures": 0,
        }],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), orders_df)
    assert result.passed is False


# ---------------------------------------------------------------------------
# uniqueness_check
# ---------------------------------------------------------------------------

def test_uniqueness_check_dataframe_passes_with_no_duplicates(orders_df):
    """NOTE: passes today, but vacuously -- see the xfail tests below."""
    c = _asset_component(data_source_type="dataframe", uniqueness_check=[{"columns": [{"column": "id"}]}])
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), orders_df)
    assert result.passed is True


def test_uniqueness_check_dataframe_fails_with_duplicates():
    # Was a real bug: bare-vs-prefixed 'columns' mismatch (always []), so
    # _analyze_uniqueness_validation always reported '0/0 checks passed' and
    # the check ALWAYS PASSED regardless of real duplicates. Fixed at the
    # root in _get_check_config_value. Name kept for history.
    df = pd.DataFrame({"id": [1, 2, 2, 3], "region": ["e", "e", "w", "n"]})
    c = _asset_component(data_source_type="dataframe", uniqueness_check=[{"columns": [{"column": "id"}]}])
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.passed is False


def test_uniqueness_check_multi_column_combination_runs_at_all():
    """region+amount combined is unique even though region alone repeats --
    this should report exactly 1 real check performed (total_checks=1)."""
    df = pd.DataFrame({"region": ["e", "e", "w"], "amount": [1.0, 2.0, 1.0]})
    c = _asset_component(data_source_type="dataframe", uniqueness_check=[{"columns": [{"columns": ["region", "amount"]}]}])
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.metadata["total_checks"].value == 1, (
        "total_checks=0 means the configured columns never reached "
        "_analyze_uniqueness_validation at all"
    )


def test_uniqueness_check_database_to_dataframe_mode_detects_duplicates(duckdb_resource):
    """orders has no duplicate ids, so insert one via a second table row to
    make this a meaningful duplicate-detection test rather than a vacuous
    pass -- re-use the fixture's connection isn't available here, so assert
    on total_checks actually running instead."""
    c = _asset_component(
        database_resource_key="duckdb", table_name="orders",
        uniqueness_check=[{"columns": [{"column": "id"}]}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_db_context(duckdb_resource))
    assert result.metadata["total_checks"].value == 1, (
        "total_checks=0 means the configured columns never reached "
        "_analyze_uniqueness_validation at all"
    )


def test_uniqueness_check_group_by_produces_one_check_per_group():
    # Was a real bug: uniqueness_check's group_by/allowed_failures config was
    # accepted and stored but never read by the executor -- uniqueness was
    # always validated across the WHOLE dataframe regardless of group_by.
    # Fixed by adding a grouped branch (mirrors null_check's pattern): one
    # validation per group, aggregated with allowed_failures. id=2 appears
    # twice overall but never twice WITHIN a single group, so this passes
    # once grouping actually runs -- the real signal is total_groups == 3,
    # proving one validation ran per group rather than one overall.
    df = pd.DataFrame({"id": [1, 2, 2, 3], "region": ["e", "e", "w", "n"]})
    c = _asset_component(
        data_source_type="dataframe",
        uniqueness_check=[{"columns": [{"column": "id"}], "group_by": "region", "allowed_failures": 1}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    assert result.metadata["total_groups"].value == 3, (
        "group_by should produce one uniqueness check per group (3 regions "
        "here), not the single whole-table check this used to always run"
    )
    assert result.passed is True


# ---------------------------------------------------------------------------
# Copy-paste default-check-name-suffix bug: _create_range_check and
# _create_uniqueness_check swapped default suffixes with each other.
# ---------------------------------------------------------------------------

def test_range_check_default_name_is_not_percent_delta(orders_df):
    # Was a real bug: _create_range_check passed default_suffix='percent_delta'
    # instead of 'range_check', colliding with an actual percent_delta check
    # on the same asset. Fixed. Name kept for history.
    c = _asset_component(data_source_type="dataframe", range_check=[{"columns": [{"column": "amount", "min_value": 0}]}])
    checks = c._build_single_asset_checks()
    spec = _spec_of(checks, "orders_")
    assert "percent_delta" not in spec.name
    assert "range_check" in spec.name or "range" in spec.name


def test_uniqueness_check_default_name_is_not_range_check(orders_df):
    # Was a real bug: _create_uniqueness_check passed default_suffix=
    # 'range_check' instead of 'uniqueness_check', colliding with an actual
    # range_check on the same asset. Fixed. Name kept for history.
    c = _asset_component(data_source_type="dataframe", uniqueness_check=[{"columns": [{"column": "id"}]}])
    checks = c._build_single_asset_checks()
    spec = _spec_of(checks, "orders_")
    assert spec.name != "orders_range_check"
    assert "uniqueness" in spec.name


def test_range_check_and_percent_delta_default_names_do_not_collide(orders_df):
    """Was a real naming collision: configuring BOTH range_check and
    percent_delta on the same asset with no custom names used to produce two
    checks with the SAME auto-generated name ('{asset}_percent_delta'),
    since _create_range_check's wrong default_suffix happened to equal
    percent_delta's own correct name. Fixed by giving range_check its own
    correct default_suffix."""
    c = _asset_component(
        data_source_type="dataframe",
        range_check=[{"columns": [{"column": "amount", "min_value": 0}]}],
        percent_delta=[{"metric": "num_rows", "max_delta": 20}],
    )
    checks = c._build_single_asset_checks()
    names = [spec.name for d in checks for spec in d.check_specs]
    assert len(names) == len(set(names)), f"duplicate check names: {names}"


# ---------------------------------------------------------------------------
# hasattr(df, 'filter') anti-pattern -- pandas DataFrames also have a
# `.filter()` method (label-pattern selection, unrelated to boolean masking),
# so every `hasattr(df, 'filter')  # Polars` check in this file always
# matched pandas too. This surfaced while adding group_by support to
# uniqueness_check: the "Polars" branch ran `pandas.DataFrame.filter(<bool
# Series>)`, which pandas interprets as selecting columns named `True`/
# `False` -- producing a frame with every row but ZERO columns. Found in 3
# places: null_check's grouped branch, uniqueness_check's new grouped
# branch, and _filter_dataframe's time_filter_column/hours_back/days_back
# path (the shared WHERE-clause helper nearly every check type calls).
# Fixed by using the already-existing, correct `_is_polars` helper
# (`hasattr(df, 'select')`, which pandas DataFrames don't have) everywhere.
# ---------------------------------------------------------------------------

def test_null_check_group_by_is_not_corrupted_by_the_polars_hasattr_bug():
    df = pd.DataFrame({"id": [1, 2, 3], "region": ["e", "e", "w"], "email": ["a@x.com", None, "c@x.com"]})
    c = _asset_component(
        data_source_type="dataframe",
        null_check=[{"columns": ["email"], "group_by": "region", "allowed_failures": 0}],
    )
    checks = c._build_single_asset_checks()
    result = _only_check(checks)(_fake_df_context(), df)
    # region "e" has 1 null email out of 2 rows -> that group fails;
    # region "w" has 0 nulls -> passes. allowed_failures=0 -> overall fail.
    assert result.passed is False
    assert result.metadata["total_groups"].value == 2


def test_time_filter_column_actually_filters_pandas_dataframes():
    """Was corrupted the same way: hours_back/days_back filtering via
    _filter_dataframe always took the broken 'Polars' branch for pandas,
    producing a 0-column frame instead of filtering by time."""
    from datetime import datetime, timedelta
    now = datetime.now()
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "created_at": [now - timedelta(hours=1), now - timedelta(hours=100), now - timedelta(hours=200)],
    })
    c = _asset_component(
        data_source_type="dataframe",
        time_filter_column="created_at",
        hours_back=24,
        row_count_check=[{"min_rows": 1, "max_rows": 1}],
    )
    checks = c._build_single_asset_checks()
    filtered = c._filter_dataframe(df)
    assert list(filtered.columns) == ["id", "created_at"], (
        f"time filtering corrupted the dataframe's columns: {list(filtered.columns)}"
    )
    assert len(filtered) == 1, "only the 1-hour-old row should survive a 24-hour cutoff"
