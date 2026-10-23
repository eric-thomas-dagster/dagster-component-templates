"""Real, committed tests for a bug an external demo-builder hit: `blocking:
true` on a `custom_sql_check` never reached the resulting `AssetCheckSpec` --
the check always behaved as non-blocking regardless of what was configured.

Root cause, confirmed by reading the whole check-creation pipeline:

1. `CustomSqlCheckConfig` (and its siblings `DataframeQueryCheckConfig`,
   `CustomDataframeCheckConfig`) were declared directly as plain
   `pydantic.BaseModel`, NOT inheriting `BaseCheckConfig` the way every other
   of the ~18 check types does -- so they had no `blocking` field at all.
   Pydantic's default `extra="ignore"` silently drops an unknown `blocking:`
   key with no error, so the setting just vanished.

2. Deeper and broader than the one reported case: `blocking` IS a real,
   declared field on `BaseCheckConfig` (every other check type), but it was
   NEVER READ anywhere in this ~8000-line component -- none of the ~54
   `@asset_check(...)` call sites passed Dagster's own `blocking` parameter.
   So `blocking: true` had zero effect on ANY check type, not just
   custom_sql_check.

Fixed both: the 3 missing config classes now inherit `BaseCheckConfig`, and
every `@asset_check(...)` call site now threads `blocking=` through from the
check's own config via a new shared `_get_check_blocking` helper.
"""
import importlib.util
import pathlib

import dagster as dg

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_blocking", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


def _asset_component(**kwargs):
    c = EnhancedDataQualityChecks(**kwargs)
    c._asset_name = "orders"
    c._partitions_def_for_checks = None
    return c


def _blocking_of(check_defs, name_substring: str) -> bool:
    for d in check_defs:
        for spec in d.check_specs:
            if name_substring in spec.name:
                return spec.blocking
    raise AssertionError(f"no check spec matching {name_substring!r} in {check_defs}")


def test_custom_sql_check_blocking_true_reaches_the_asset_check_spec():
    """This is the exact bug reported: blocking: true on a custom_sql_check."""
    c = _asset_component(
        database_resource_key="duckdb",
        custom_sql_check=[{"sql_query": "SELECT 1", "blocking": True}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "custom_sql_check") is True


def test_custom_sql_check_blocking_defaults_to_false():
    c = _asset_component(
        database_resource_key="duckdb",
        custom_sql_check=[{"sql_query": "SELECT 1"}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "custom_sql_check") is False


def test_dataframe_query_check_blocking_true_reaches_the_asset_check_spec():
    c = _asset_component(
        data_source_type="dataframe",
        dataframe_query_check=[{"query": "df", "blocking": True}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "dataframe_query_check") is True


def test_custom_dataframe_check_blocking_true_reaches_the_asset_check_spec():
    c = _asset_component(
        data_source_type="dataframe",
        custom_dataframe_check=[{"python_code": "result = True", "blocking": True}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "custom_dataframe_check") is True


def test_row_count_check_blocking_true_reaches_the_asset_check_spec():
    """Confirms the fix for the OTHER ~17 check types that already had a
    `blocking` field declared, but it was never read anywhere."""
    c = _asset_component(
        data_source_type="dataframe",
        row_count_check=[{"min_rows": 1, "blocking": True}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "row_count") is True


def test_row_count_check_blocking_false_by_default():
    c = _asset_component(
        data_source_type="dataframe",
        row_count_check=[{"min_rows": 1}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "row_count") is False


def test_null_check_blocking_true_reaches_the_asset_check_spec():
    c = _asset_component(
        data_source_type="dataframe",
        null_check=[{"columns": ["id"], "blocking": True}],
    )
    checks = c._build_single_asset_checks()
    assert _blocking_of(checks, "null_check") is True
