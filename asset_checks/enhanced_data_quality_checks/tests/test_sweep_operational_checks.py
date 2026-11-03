"""Comprehensive, fully-real coverage for cross_table_validation,
duration_anomaly_check, and deeper coverage of the custom/query check family
(custom_sql_check, dataframe_query_check, custom_dataframe_check) beyond what
test_custom_check_group_by.py already covers (group_by/allowed_failures).

Everything here is real: a real, temp-file-backed DuckDB database via
dagster_duckdb.DuckDBResource, real pandas DataFrames, and a real
Dagster AssetCheckSpec built via the actual `_build_single_asset_checks()`
dispatch path (the same path real YAML config goes through). Nothing in this
component makes a paid/external call, so nothing is mocked except the
Dagster *instance* event log for duration_anomaly_check (which needs
historical run timestamps that don't exist in a unit test), following the
precedent in test_historical_data_lookup.py.

Bugs found while writing this file:

1. `severity` is declared on every check config (BaseCheckConfig.severity)
   but `_execute_custom_sql_check` / `_execute_dataframe_query_check` /
   `_execute_custom_dataframe_check` never read it or pass it to
   `AssetCheckResult(...)` at all -- so a configured `severity: "WARN"` has
   zero effect; Dagster's own default (`AssetCheckSeverity.ERROR`) always
   wins. Still open -- captured as `xfail(strict=True)`. See
   test_custom_sql_check_severity_is_declared_but_not_wired.

2. FIXED (no longer xfail): `_create_cross_table_validation_check`
   (component.py) read `component._get_check_config_value('source_database')`
   (unprefixed) to decide what to add to `required_resource_keys`, while
   `_apply_nested_check_config` stored this value under the PREFIXED key
   `cross_table_source_database`. Root-caused and fixed at the source in
   `_get_check_config_value` itself (it now falls back to the
   check-type-prefixed key when the bare one is absent), which also fixed
   several other check types with the identical bug. See
   test_cross_table_validation_source_database_missing_from_required_resource_keys.

3. `_execute_cross_table_column_values_validation` is an unimplemented stub
   that always returns `passed=False, description="Column values validation
   not yet implemented"` regardless of data -- confirmed by reading the
   code, not assumed. Captured as a non-xfail test asserting the exact
   documented stub behavior, since it is self-documenting rather than a
   silent landmine.
"""
import importlib.util
import pathlib
import types

import dagster as dg
import pandas as pd
import pytest
from dagster import AssetCheckSeverity, AssetKey, DagsterEventType
from dagster_duckdb import DuckDBResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_sweep_operational", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks
CustomSqlCheckConfig = _mod.CustomSqlCheckConfig
DataframeQueryCheckConfig = _mod.DataframeQueryCheckConfig
CustomDataframeCheckConfig = _mod.CustomDataframeCheckConfig


def _build(component: EnhancedDataQualityChecks, asset_name="orders"):
    """Mirror real YAML-driven construction: set the private fields
    `_build_single_asset_checks` needs, then return the real
    @asset_check-decorated functions it produces."""
    component._asset_name = asset_name
    component._asset_key = AssetKey([asset_name])
    component._partitions_def_for_checks = None
    return component._build_single_asset_checks()


def _run_check(check_fn, fake_context):
    """A check built via _build_single_asset_checks is an AssetChecksDefinition.
    Direct invocation through Dagster's own __call__ demands a real
    BaseDirectExecutionContext, which a lightweight fake context isn't -- so
    call the underlying undecorated function directly instead, exactly like
    test_historical_data_lookup.py calls component methods directly."""
    return check_fn.node_def.compute_fn.decorated_fn(fake_context)


# ─────────────────────────── custom_sql_check ───────────────────────────


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


class _NoOpLog:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass


@pytest.fixture
def fake_db_context(duckdb_with_orders):
    return types.SimpleNamespace(resources=types.SimpleNamespace(duckdb=duckdb_with_orders), log=_NoOpLog())


@pytest.fixture
def orders_df():
    return pd.DataFrame({
        "region": ["east", "east", "west", "west", "north"],
        "amount": [10.0, 20.0, 5.0, 1000.0, 3.0],
    })


def test_custom_sql_check_non_grouped_happy_path_passes(duckdb_with_orders, fake_db_context):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT COUNT(*) FROM orders",
        expected_result=5,
        comparison="equals",
    )
    result = c._execute_custom_sql_check(fake_db_context, check_cfg, "duckdb")
    assert result.passed is True


def test_custom_sql_check_non_grouped_failure_path_fails_cleanly(duckdb_with_orders, fake_db_context):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT COUNT(*) FROM orders",
        expected_result=999,
        comparison="equals",
    )
    result = c._execute_custom_sql_check(fake_db_context, check_cfg, "duckdb")
    assert result.passed is False


def test_custom_sql_check_invalid_query_returns_failed_result_not_a_crash(duckdb_with_orders, fake_db_context):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT COUNT(*) FROM table_that_does_not_exist",
        expected_result=5,
    )
    # Must not raise -- the executor's own try/except must catch this.
    result = c._execute_custom_sql_check(fake_db_context, check_cfg, "duckdb")
    assert result.passed is False
    assert "failed" in result.description.lower()


def test_custom_sql_check_blocking_true_reaches_real_asset_check_spec(duckdb_with_orders):
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        custom_sql_check=[{
            "sql_query": "SELECT COUNT(*) FROM orders",
            "expected_result": 5,
            "blocking": True,
        }],
    )
    checks = _build(c)
    assert len(checks) == 1
    specs = list(checks[0].check_specs)
    assert len(specs) == 1
    assert specs[0].blocking is True


def test_custom_sql_check_severity_is_declared_but_not_wired(duckdb_with_orders, fake_db_context):
    # Was a real bug: severity was declared but never read or passed to
    # AssetCheckResult(...), so Dagster's own ERROR default always won.
    # Fixed by reading check_cfg.severity and overriding the shared
    # executor's result. Name kept for history.
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = CustomSqlCheckConfig(
        sql_query="SELECT COUNT(*) FROM orders",
        expected_result=999,  # force a failure so severity is meaningful
        severity="WARN",
    )
    result = c._execute_custom_sql_check(fake_db_context, check_cfg, "duckdb")
    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN


# ───────────────────────── dataframe_query_check ─────────────────────────


def test_dataframe_query_check_non_grouped_happy_path_passes(duckdb_with_orders, orders_df, fake_db_context):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = DataframeQueryCheckConfig(query="len(df)", expected_result=5, comparison="equals")
    result = c._execute_dataframe_query_check(fake_db_context, orders_df, check_cfg)
    assert result.passed is True


def test_dataframe_query_check_non_grouped_failure_path_fails_cleanly(duckdb_with_orders, orders_df, fake_db_context):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = DataframeQueryCheckConfig(query="len(df)", expected_result=999, comparison="equals")
    result = c._execute_dataframe_query_check(fake_db_context, orders_df, check_cfg)
    assert result.passed is False


def test_dataframe_query_check_invalid_query_returns_failed_result_not_a_crash(duckdb_with_orders, orders_df, fake_db_context):
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = DataframeQueryCheckConfig(query="df['does_not_exist'].sum()", expected_result=5)
    result = c._execute_dataframe_query_check(fake_db_context, orders_df, check_cfg)
    assert result.passed is False
    assert "failed" in result.description.lower()


def test_dataframe_query_check_blocking_true_reaches_real_asset_check_spec():
    c = EnhancedDataQualityChecks(
        data_source_type="dataframe",
        table_name="orders",
        dataframe_query_check=[{
            "query": "len(df)",
            "expected_result": 5,
            "blocking": True,
        }],
    )
    checks = _build(c)
    assert len(checks) == 1
    specs = list(checks[0].check_specs)
    assert specs[0].blocking is True


def test_dataframe_query_check_severity_is_declared_but_not_wired(duckdb_with_orders, orders_df, fake_db_context):
    # Was the same severity-not-wired bug as custom_sql_check. Fixed. Name
    # kept for history.
    c = EnhancedDataQualityChecks(database_resource_key="duckdb", table_name="orders")
    check_cfg = DataframeQueryCheckConfig(query="len(df)", expected_result=999, severity="WARN")
    result = c._execute_dataframe_query_check(fake_db_context, orders_df, check_cfg)
    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN


# ───────────────────────── custom_dataframe_check ─────────────────────────


def test_custom_dataframe_check_non_grouped_happy_path_passes():
    df = pd.DataFrame({"price": [10.0, 20.0], "list_price": [12.0, 25.0]})
    c = EnhancedDataQualityChecks(database_resource_key=None, table_name="pricing")
    check_cfg = CustomDataframeCheckConfig(
        python_code="len(df[df['price'] > df['list_price']]) == 0",
        expected_result=True,
    )
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace())
    result = c._execute_custom_dataframe_check(fake_context, df, check_cfg)
    assert result.passed is True


def test_custom_dataframe_check_non_grouped_failure_path_fails_cleanly():
    df = pd.DataFrame({"price": [10.0, 1000.0], "list_price": [12.0, 25.0]})
    c = EnhancedDataQualityChecks(database_resource_key=None, table_name="pricing")
    check_cfg = CustomDataframeCheckConfig(
        python_code="len(df[df['price'] > df['list_price']]) == 0",
        expected_result=True,
    )
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace())
    result = c._execute_custom_dataframe_check(fake_context, df, check_cfg)
    assert result.passed is False


def test_custom_dataframe_check_invalid_code_returns_failed_result_not_a_crash():
    df = pd.DataFrame({"price": [10.0, 20.0]})
    c = EnhancedDataQualityChecks(database_resource_key=None, table_name="pricing")
    check_cfg = CustomDataframeCheckConfig(python_code="df['does_not_exist'].sum()", expected_result=5)
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace())
    result = c._execute_custom_dataframe_check(fake_context, df, check_cfg)
    assert result.passed is False
    assert "failed" in result.description.lower()


def test_custom_dataframe_check_blocking_true_reaches_real_asset_check_spec():
    c = EnhancedDataQualityChecks(
        data_source_type="dataframe",
        table_name="pricing",
        custom_dataframe_check=[{
            "python_code": "len(df) == 2",
            "expected_result": True,
            "blocking": True,
        }],
    )
    checks = _build(c, asset_name="pricing")
    assert len(checks) == 1
    specs = list(checks[0].check_specs)
    assert specs[0].blocking is True


def test_custom_dataframe_check_severity_is_declared_but_not_wired():
    # Was the same severity-not-wired bug. Fixed. Name kept for history.
    df = pd.DataFrame({"price": [10.0, 1000.0], "list_price": [12.0, 25.0]})
    c = EnhancedDataQualityChecks(database_resource_key=None, table_name="pricing")
    check_cfg = CustomDataframeCheckConfig(
        python_code="len(df[df['price'] > df['list_price']]) == 0",
        expected_result=True,
        severity="WARN",
    )
    fake_context = types.SimpleNamespace(resources=types.SimpleNamespace())
    result = c._execute_custom_dataframe_check(fake_context, df, check_cfg)
    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN


# ───────────────────────── cross_table_validation ─────────────────────────


@pytest.fixture
def duckdb_with_orders_and_source(tmp_path):
    """One DuckDB file, two tables -- `orders` (the "destination") and
    `orders_source` (the "source") -- joined by `id`."""
    db_path = str(tmp_path / "cross_table.duckdb")
    resource = DuckDBResource(database=db_path)
    with resource.get_connection() as conn:
        conn.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, amount DOUBLE)")
        conn.execute(
            "INSERT INTO orders VALUES (1, 'east', 10.0), (2, 'east', 20.0), (3, 'west', 5.0)"
        )
        conn.execute("CREATE TABLE orders_source (id INTEGER, region VARCHAR, amount DOUBLE)")
        conn.execute(
            "INSERT INTO orders_source VALUES (1, 'east', 10.0), (2, 'east', 20.0), (3, 'west', 5.0)"
        )
    return resource


@pytest.fixture
def fake_cross_table_context(duckdb_with_orders_and_source):
    return types.SimpleNamespace(
        resources=types.SimpleNamespace(duckdb=duckdb_with_orders_and_source), log=_NoOpLog()
    )


def test_cross_table_validation_row_count_passes_when_tables_match(fake_cross_table_context):
    # Was a real bug: the non-grouped branch of
    # _execute_cross_table_row_count_validation built 'SELECT COUNT(DISTINCT
    # dest.*) as dest_count, COUNT(DISTINCT src.*) as src_count ...' --
    # COUNT(DISTINCT table.*) is invalid SQL on DuckDB ('Binder Error: STAR
    # expression is only allowed as the root element of an expression'), so
    # this validation_type always failed via the executor's own except
    # clause regardless of whether the tables actually matched. Fixed by
    # counting a representative join column instead (dest's is always
    # non-null for an existing dest row; src's is null wherever the LEFT
    # JOIN found no match), mirroring the aggregate validation's existing
    # dest/src pattern. Name kept for history.
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        cross_table_validation=[{
            "source_table": "orders_source",
            "join_columns": ["id"],
            "validation_type": "row_count",
        }],
    )
    checks = _build(c)
    assert len(checks) == 1
    result = _run_check(checks[0], fake_cross_table_context)
    assert result.passed is True


def test_cross_table_validation_row_count_fails_for_genuinely_diverging_tables(fake_cross_table_context, duckdb_with_orders_and_source):
    """A genuine divergence: an extra row in `orders` (the destination) with
    no matching id in `orders_source`. A LEFT JOIN from dest to src can only
    ever detect rows missing FROM the source side (dest's join column is
    always non-null; src's is null on no match) -- adding unmatched rows to
    the source side alone is invisible to this query shape, which is why the
    earlier version of this test (before the COUNT(DISTINCT table.*) bug was
    fixed) inserted into the wrong table and could never have been a real
    divergence regardless of that bug."""
    with duckdb_with_orders_and_source.get_connection() as conn:
        conn.execute("INSERT INTO orders VALUES (4, 'north', 3.0)")

    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        cross_table_validation=[{
            "source_table": "orders_source",
            "join_columns": ["id"],
            "validation_type": "row_count",
            "tolerance": 5.0,
        }],
    )
    checks = _build(c)
    result = _run_check(checks[0], fake_cross_table_context)
    assert result.passed is False


def test_cross_table_validation_aggregate_passes_when_sums_match(fake_cross_table_context):
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        cross_table_validation=[{
            "source_table": "orders_source",
            "join_columns": ["id"],
            "validation_type": "aggregate",
            "aggregate_column": "amount",
            "aggregate_function": "sum",
        }],
    )
    checks = _build(c)
    result = _run_check(checks[0], fake_cross_table_context)
    assert result.passed is True


def test_cross_table_validation_aggregate_fails_when_sums_diverge(fake_cross_table_context, duckdb_with_orders_and_source):
    with duckdb_with_orders_and_source.get_connection() as conn:
        conn.execute("UPDATE orders_source SET amount = 99999 WHERE id = 1")

    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        cross_table_validation=[{
            "source_table": "orders_source",
            "join_columns": ["id"],
            "validation_type": "aggregate",
            "aggregate_column": "amount",
            "aggregate_function": "sum",
            "tolerance": 5.0,
        }],
    )
    checks = _build(c)
    result = _run_check(checks[0], fake_cross_table_context)
    assert result.passed is False


def test_cross_table_validation_column_values_is_an_unimplemented_stub(fake_cross_table_context):
    """Confirmed by reading _execute_cross_table_column_values_validation
    directly: it is a stub that always returns passed=False regardless of
    data, with a description that says so. Not a silent landmine -- it's
    self-documenting -- but worth a test that pins the documented behavior
    so a future change here is deliberate, not accidental."""
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        cross_table_validation=[{
            "source_table": "orders_source",
            "join_columns": ["id"],
            "validation_type": "column_values",
        }],
    )
    checks = _build(c)
    result = _run_check(checks[0], fake_cross_table_context)
    assert result.passed is False
    assert "not yet implemented" in result.description.lower()


def test_cross_table_validation_source_database_missing_from_required_resource_keys():
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        table_name="orders",
        cross_table_validation=[{
            "source_table": "orders_source",
            "source_database": "other_duckdb",
            "join_columns": ["id"],
            "validation_type": "row_count",
        }],
    )
    checks = _build(c)
    required = checks[0].node_def.required_resource_keys
    assert "other_duckdb" in required, (
        f"expected 'other_duckdb' in required_resource_keys, got {required!r}"
    )


# ───────────────────────── duration_anomaly_check ─────────────────────────


class _FakeEventLogEntry:
    def __init__(self, run_id=None, step_key=None, timestamp=None, dagster_event_type=None):
        self.run_id = run_id
        self.step_key = step_key
        self.timestamp = timestamp
        self.dagster_event_type = dagster_event_type


class _FakeRecord:
    def __init__(self, event_log_entry):
        self.event_log_entry = event_log_entry


class _FakeRecordsResult:
    def __init__(self, records):
        self.records = records


class _FakeDurationInstance:
    """Models exactly what _execute_duration_anomaly_check reads:
    instance.fetch_materializations(asset_key, limit=...).records, each with
    .event_log_entry.{run_id,step_key}; then, per run_id,
    instance.get_records_for_run(run_id, of_type={...}).records, each with
    .event_log_entry.{step_key, dagster_event_type, timestamp}."""

    def __init__(self, mat_records, run_events_by_run_id):
        self._mat_records = mat_records
        self._run_events_by_run_id = run_events_by_run_id

    def fetch_materializations(self, asset_key, limit=None):
        records = self._mat_records[:limit] if limit else self._mat_records
        return _FakeRecordsResult(records)

    def get_records_for_run(self, run_id, of_type=None):
        return _FakeRecordsResult(self._run_events_by_run_id.get(run_id, []))


def _build_duration_fixture(durations_most_recent_first, step_key="the_step"):
    """durations_most_recent_first[0] is the CURRENT run; the rest are the
    historical baseline, matching _execute_duration_anomaly_check's own
    `durations[0]` / `durations[1:]` split."""
    mat_records = []
    run_events = {}
    base_ts = 1_700_000_000.0
    for i, duration in enumerate(durations_most_recent_first):
        run_id = f"run_{i}"
        mat_records.append(_FakeRecord(_FakeEventLogEntry(run_id=run_id, step_key=step_key)))
        start_ts = base_ts + i * 10_000  # arbitrary spacing between runs
        run_events[run_id] = [
            _FakeRecord(_FakeEventLogEntry(
                step_key=step_key, dagster_event_type=DagsterEventType.STEP_START, timestamp=start_ts,
            )),
            _FakeRecord(_FakeEventLogEntry(
                step_key=step_key, dagster_event_type=DagsterEventType.STEP_SUCCESS, timestamp=start_ts + duration,
            )),
        ]
    return _FakeDurationInstance(mat_records, run_events)


def test_duration_anomaly_check_passes_when_duration_is_normal():
    historical = [10.0, 11.0, 9.0, 10.5, 9.5, 10.2, 9.8, 10.1, 9.9, 10.0]
    durations = [10.3] + historical  # current run is well within normal range
    instance = _build_duration_fixture(durations)

    c = EnhancedDataQualityChecks(table_name="orders", duration_anomaly_check=[{
        "method": "z_score", "threshold": 2.0, "history": len(historical), "min_history": 3,
    }])
    checks = _build(c)
    fake_context = types.SimpleNamespace(instance=instance)
    result = _run_check(checks[0], fake_context)
    assert result.passed is True


def test_duration_anomaly_check_fails_when_duration_is_a_clear_outlier():
    historical = [10.0, 11.0, 9.0, 10.5, 9.5, 10.2, 9.8, 10.1, 9.9, 10.0]
    durations = [500.0] + historical  # current run is a massive outlier
    instance = _build_duration_fixture(durations)

    c = EnhancedDataQualityChecks(table_name="orders", duration_anomaly_check=[{
        "method": "z_score", "threshold": 2.0, "history": len(historical), "min_history": 3,
    }])
    checks = _build(c)
    fake_context = types.SimpleNamespace(instance=instance)
    result = _run_check(checks[0], fake_context)
    assert result.passed is False


def test_duration_anomaly_check_skips_when_history_is_insufficient():
    durations = [10.0, 9.0]  # fewer than min_history
    instance = _build_duration_fixture(durations)

    c = EnhancedDataQualityChecks(table_name="orders", duration_anomaly_check=[{
        "method": "z_score", "history": 20, "min_history": 5,
    }])
    checks = _build(c)
    fake_context = types.SimpleNamespace(instance=instance)
    result = _run_check(checks[0], fake_context)
    assert result.passed is True  # skipped, not failed
    status = result.metadata["status"].value if "status" in result.metadata else ""
    assert "skip" in status.lower() or "insufficient" in status.lower()


def test_duration_anomaly_check_iqr_method_fails_on_outlier():
    historical = [10.0, 11.0, 9.0, 10.5, 9.5, 10.2, 9.8, 10.1, 9.9, 10.0]
    durations = [500.0] + historical
    instance = _build_duration_fixture(durations)

    c = EnhancedDataQualityChecks(table_name="orders", duration_anomaly_check=[{
        "method": "iqr", "threshold": 1.5, "history": len(historical), "min_history": 3,
    }])
    checks = _build(c)
    fake_context = types.SimpleNamespace(instance=instance)
    result = _run_check(checks[0], fake_context)
    assert result.passed is False


def test_duration_anomaly_check_blocking_true_reaches_real_asset_check_spec():
    # Was a real bug: DurationAnomalyCheckConfig was a plain pydantic.BaseModel,
    # not inheriting BaseCheckConfig like every other check config (added
    # later than the others, missed in the earlier blocking-flag fix). Fixed
    # by inheriting BaseCheckConfig. Name kept for history.
    c = EnhancedDataQualityChecks(table_name="orders", duration_anomaly_check=[{
        "blocking": True,
    }])
    checks = _build(c)
    specs = list(checks[0].check_specs)
    assert specs[0].blocking is True
