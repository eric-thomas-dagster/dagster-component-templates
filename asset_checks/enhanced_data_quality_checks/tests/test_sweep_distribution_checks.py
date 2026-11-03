"""Comprehensive, fully-real tests for 5 check types: benford_law,
entropy_analysis, pattern_matching, value_set_validation, data_type_check.

Written as part of a systematic sweep of enhanced_data_quality_checks after
several real, silent bugs were found elsewhere in this component (dead/
shadowed method definitions, group_by/allowed_failures wiring gaps). Nothing
in this component is paid or external (no API calls), so nothing here is
mocked: dataframe-mode tests use real pandas DataFrames, database-mode
("database_to_dataframe") tests use a real, temp-file-backed DuckDB database
via dagster_duckdb.DuckDBResource.

Three real bugs were found and confirmed while writing these tests (not
fixed here -- see the xfail tests below for root-cause docs):

1. `_execute_dataframe_pattern_matching` builds Python source as an f-string
   that interpolates the user's regex pattern directly into a single-quoted
   string literal (`result = self._analyze_pattern_matching(df, '{column}',
   '{regex_pattern}', {min_pct})`), then eval/execs it. Any regex containing
   a single quote -- an entirely ordinary case, e.g. matching names with
   apostrophes like "O'Brien" -- produces invalid Python source and the
   check fails with a syntax error instead of evaluating the regex.

2. `_execute_dataframe_pattern_matching` and `_execute_dataframe_data_type`
   both hardcode `allowed_failures=0` in their `SharedCheckExecutor
   .execute_dataframe_check(...)` call, silently ignoring the real,
   documented `pattern_allowed_failures` / `data_type_allowed_failures`
   config fields (confirmed populated by `_apply_nested_check_config`) --
   grouped pattern_matching/data_type checks can never tolerate any group
   failing, no matter what `allowed_failures` is configured to.

3. `_execute_dataframe_value_set_validation`'s grouped branch hardcodes
   `passed = len(failed_groups) == 0`, silently ignoring the real,
   documented `value_set_allowed_failures` config field the same way.

entropy_analysis's own group_by/allowed_failures wiring, by contrast, is
correct (`passed = len(failed_groups) <= allowed_failures`) and is tested
here as the working counter-example.
"""
import importlib.util
import pathlib

import dagster as dg
import numpy as np
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_sweep_distribution", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


def _component(**kwargs):
    defaults = dict(database_resource_key=None, table_name="data", data_source_type="dataframe")
    defaults.update(kwargs)
    c = EnhancedDataQualityChecks(**defaults)
    c._asset_name = "data"
    c._partitions_def_for_checks = None
    return c


def _single_check(c):
    checks = c._build_single_asset_checks()
    assert len(checks) == 1
    return checks[0]


def _blocking_of(check_defs, name_substring: str) -> bool:
    for d in check_defs:
        for spec in d.check_specs:
            if name_substring in spec.name:
                return spec.blocking
    raise AssertionError(f"no check spec matching {name_substring!r} in {check_defs}")


@pytest.fixture
def duckdb_resource(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    resource = DuckDBResource(database=db_path)
    return resource


# ---------------------------------------------------------------------------
# Benford's Law
# ---------------------------------------------------------------------------

def _benford_compliant_values(n=2000, seed=7):
    rng = np.random.default_rng(seed)
    # Lognormal data is well-known to follow Benford's Law closely.
    return rng.lognormal(mean=5.0, sigma=3.0, size=n)


def _benford_violating_values(n=500):
    # All values in [50000, 50999]: leading digit is always 5.
    return list(range(50000, 50000 + n))


def test_benford_law_passes_on_real_benford_compliant_data():
    df = pd.DataFrame({"amount": _benford_compliant_values()})
    c = _component(benford_law=[{"column": "amount"}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True


def test_benford_law_fails_on_data_that_violates_benfords_law():
    df = pd.DataFrame({"amount": _benford_violating_values()})
    c = _component(benford_law=[{"column": "amount"}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False


def test_benford_law_blocking_true_reaches_the_asset_check_spec():
    c = _component(benford_law=[{"column": "amount", "blocking": True}])
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "benford") is True


def test_benford_law_blocking_defaults_to_false():
    c = _component(benford_law=[{"column": "amount"}])
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "benford") is False


def test_benford_law_database_mode_against_real_duckdb(duckdb_resource):
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE TABLE amounts (amount DOUBLE)")
        values = _benford_compliant_values()
        conn.executemany("INSERT INTO amounts VALUES (?)", [(float(v),) for v in values])

    c = _component(database_resource_key="duckdb", table_name="amounts", data_source_type="database",
                   benford_law=[{"column": "amount"}])
    check_fn = _single_check(c)
    context = dg.build_asset_check_context(resources={"duckdb": duckdb_resource})
    result = check_fn(context)
    assert result.passed is True


# ---------------------------------------------------------------------------
# Entropy analysis
# ---------------------------------------------------------------------------

def test_entropy_analysis_passes_within_bounds():
    # High-entropy column: evenly spread categories.
    df = pd.DataFrame({"category": ["a", "b", "c", "d"] * 25})
    c = _component(entropy_analysis=[{"column": "category", "min_entropy": 1.5}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True


def test_entropy_analysis_fails_when_below_min_entropy():
    # Low-entropy column: one dominant value.
    df = pd.DataFrame({"category": ["a"] * 99 + ["b"]})
    c = _component(entropy_analysis=[{"column": "category", "min_entropy": 0.5}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False


def test_entropy_analysis_zero_entropy_on_single_distinct_value():
    df = pd.DataFrame({"category": ["only_one"] * 50})
    c = _component(entropy_analysis=[{"column": "category"}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert "0.000" in result.description


def test_entropy_analysis_blocking_true_reaches_the_asset_check_spec():
    c = _component(entropy_analysis=[{"column": "category", "blocking": True}])
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "entropy") is True


def test_entropy_analysis_group_by_fails_when_exceeding_allowed_failures():
    """Two low-entropy regions (east, west) should fail; north is high-entropy."""
    df = pd.DataFrame({
        "region": ["east"] * 20 + ["west"] * 20 + ["north"] * 20,
        "category": (["a"] * 19 + ["b"]) + (["a"] * 19 + ["b"]) + (["a", "b", "c", "d"] * 5),
    })
    c = _component(entropy_analysis=[{"column": "category", "group_by": "region", "min_entropy": 1.0, "allowed_failures": 0}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False
    assert "1/3 groups passed" in result.description


def test_entropy_analysis_group_by_passes_within_allowed_failures():
    df = pd.DataFrame({
        "region": ["east"] * 20 + ["west"] * 20 + ["north"] * 20,
        "category": (["a"] * 19 + ["b"]) + (["a"] * 19 + ["b"]) + (["a", "b", "c", "d"] * 5),
    })
    c = _component(entropy_analysis=[{"column": "category", "group_by": "region", "min_entropy": 1.0, "allowed_failures": 2}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True


def test_entropy_analysis_database_mode_against_real_duckdb(duckdb_resource):
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE TABLE cats (category VARCHAR)")
        conn.execute("INSERT INTO cats VALUES " + ", ".join(f"('{v}')" for v in (["a", "b", "c", "d"] * 25)))

    c = _component(database_resource_key="duckdb", table_name="cats", data_source_type="database",
                   entropy_analysis=[{"column": "category", "min_entropy": 1.5}])
    check_fn = _single_check(c)
    context = dg.build_asset_check_context(resources={"duckdb": duckdb_resource})
    result = check_fn(context)
    assert result.passed is True


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------

def test_pattern_matching_passes_when_values_match():
    df = pd.DataFrame({"email": ["a@x.com", "b@x.com", "c@x.com"]})
    c = _component(pattern_matching=[{"column": "email", "regex_pattern": r"^[^@]+@[^@]+\.[a-z]+$", "match_percentage": 100.0}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True


def test_pattern_matching_fails_when_values_dont_match():
    df = pd.DataFrame({"email": ["a@x.com", "not-an-email", "also bad"]})
    c = _component(pattern_matching=[{"column": "email", "regex_pattern": r"^[^@]+@[^@]+\.[a-z]+$", "match_percentage": 100.0}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False


def test_pattern_matching_blocking_true_reaches_the_asset_check_spec():
    c = _component(pattern_matching=[{"column": "email", "regex_pattern": r".*", "blocking": True}])
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "pattern") is True


def test_pattern_matching_missing_column_fails_cleanly():
    # Was a real bug: the 'column not found' branch passed _get_pattern_info()'s
    # already-MetadataValue-wrapped dict into build_metadata's pattern_results
    # kwarg, which wraps it again in MetadataValue.json(...) -- crashed with
    # "Iteration is not allowed on `@record` TextMetadataValue" instead of a
    # clean error. Fixed by dropping the redundant pattern_results kwarg (the
    # error message already names the missing column). Name kept for history.
    df = pd.DataFrame({"other_col": [1, 2, 3]})
    c = _component(pattern_matching=[{"column": "email", "regex_pattern": r".*"}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False
    assert "not found" in result.description


def test_pattern_matching_regex_containing_a_single_quote_is_broken():
    # Was a real bug: the regex was interpolated into a single-quoted Python
    # string literal via a naive f-string before being eval/exec'd. A regex
    # containing a single quote -- e.g. matching names with apostrophes like
    # "O'Brien", an entirely ordinary pattern -- broke the generated Python
    # source (unterminated string literal). Fixed by using `!r` (Python's
    # own repr()) to produce a correctly escaped literal. Name kept for history.
    df = pd.DataFrame({"name": ["O'Brien", "Smith", "O'Connor"]})
    c = _component(pattern_matching=[{"column": "name", "regex_pattern": r"^[A-Za-z]+(?:'[A-Za-z]+)*$", "match_percentage": 100.0}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True
    assert "syntax" not in result.description.lower()


def test_pattern_matching_group_by_respects_allowed_failures():
    # Was a real bug: hardcoded allowed_failures=0 in the
    # SharedCheckExecutor.execute_dataframe_check(...) call, silently
    # ignoring the real, documented pattern_allowed_failures config field.
    # Fixed by reading it. Name kept for history.
    df = pd.DataFrame({
        "region": ["east"] * 3 + ["west"] * 3,
        "email": ["a@x.com", "b@x.com", "c@x.com", "bad", "bad2", "bad3"],
    })
    c = _component(pattern_matching=[{
        "column": "email", "regex_pattern": r"^[^@]+@[^@]+\.[a-z]+$", "match_percentage": 100.0,
        "group_by": "region", "allowed_failures": 1,
    }])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True  # only "west" group should fail; 1 allowed


def test_pattern_matching_database_mode_against_real_duckdb(duckdb_resource):
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE TABLE emails (email VARCHAR)")
        conn.execute("INSERT INTO emails VALUES ('a@x.com'), ('b@x.com'), ('c@x.com')")

    c = _component(database_resource_key="duckdb", table_name="emails", data_source_type="database",
                   pattern_matching=[{"column": "email", "regex_pattern": r"^[^@]+@[^@]+\.[a-z]+$", "match_percentage": 100.0}])
    check_fn = _single_check(c)
    context = dg.build_asset_check_context(resources={"duckdb": duckdb_resource})
    result = check_fn(context)
    assert result.passed is True


# ---------------------------------------------------------------------------
# Value set validation
# ---------------------------------------------------------------------------

def test_value_set_validation_passes_when_all_values_allowed():
    df = pd.DataFrame({"status": ["active", "inactive", "active", "pending"]})
    c = _component(value_set_validation=[{"column": "status", "allowed_values": ["active", "inactive", "pending"]}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True


def test_value_set_validation_fails_on_value_outside_allowed_set():
    df = pd.DataFrame({"status": ["active", "inactive", "deleted", "deleted", "deleted"]})
    c = _component(value_set_validation=[{"column": "status", "allowed_values": ["active", "inactive"], "min_pct": 95.0}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False


def test_value_set_validation_blocking_true_reaches_the_asset_check_spec():
    c = _component(value_set_validation=[{"column": "status", "allowed_values": ["active"], "blocking": True}])
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "value_set") is True


def test_value_set_validation_group_by_respects_allowed_failures():
    # Was a real bug: the grouped branch hardcoded `passed = len(failed_groups)
    # == 0`, silently ignoring the real, documented value_set_allowed_failures
    # config field. Fixed by reading it. Name kept for history.
    df = pd.DataFrame({
        "region": ["east"] * 3 + ["west"] * 3,
        "status": ["active", "active", "active", "deleted", "deleted", "deleted"],
    })
    c = _component(value_set_validation=[{
        "column": "status", "allowed_values": ["active"], "match_percentage": 100.0,
        "group_by": "region", "allowed_failures": 1,
    }])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True  # only "west" group should fail; 1 allowed


def test_value_set_validation_database_mode_against_real_duckdb(duckdb_resource):
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE TABLE statuses (status VARCHAR)")
        conn.execute("INSERT INTO statuses VALUES ('active'), ('inactive'), ('active')")

    c = _component(database_resource_key="duckdb", table_name="statuses", data_source_type="database",
                   value_set_validation=[{"column": "status", "allowed_values": ["active", "inactive"]}])
    check_fn = _single_check(c)
    context = dg.build_asset_check_context(resources={"duckdb": duckdb_resource})
    result = check_fn(context)
    assert result.passed is True


# ---------------------------------------------------------------------------
# Data type check
# ---------------------------------------------------------------------------

def test_data_type_check_passes_when_types_match():
    df = pd.DataFrame({"amount": [1.0, 2.0, 3.0], "name": ["a", "b", "c"]})
    c = _component(data_type_check=[{"columns": [
        {"column": "amount", "expected_type": "float"},
        {"column": "name", "expected_type": "string"},
    ]}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is True


def test_data_type_check_fails_on_type_mismatch():
    df = pd.DataFrame({"amount": ["not", "a", "number"]})
    c = _component(data_type_check=[{"columns": [{"column": "amount", "expected_type": "float"}]}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False


def test_data_type_check_blocking_true_reaches_the_asset_check_spec():
    c = _component(data_type_check=[{"columns": [{"column": "amount", "expected_type": "float"}], "blocking": True}])
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "data_type") is True


def test_data_type_check_missing_column_is_reported_as_failed():
    df = pd.DataFrame({"other": [1, 2, 3]})
    c = _component(data_type_check=[{"columns": [{"column": "amount", "expected_type": "float"}]}])
    check_fn = _single_check(c)
    result = check_fn(dg.build_asset_check_context(), df)
    assert result.passed is False


def test_data_type_check_database_mode_against_real_duckdb(duckdb_resource):
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE TABLE typed (amount DOUBLE, name VARCHAR)")
        conn.execute("INSERT INTO typed VALUES (1.0, 'a'), (2.0, 'b')")

    c = _component(database_resource_key="duckdb", table_name="typed", data_source_type="database",
                   data_type_check=[{"columns": [{"column": "amount", "expected_type": "float"}]}])
    check_fn = _single_check(c)
    context = dg.build_asset_check_context(resources={"duckdb": duckdb_resource})
    result = check_fn(context)
    assert result.passed is True
