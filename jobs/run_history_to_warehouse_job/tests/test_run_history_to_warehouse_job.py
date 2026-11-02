"""Real, committed tests for RunHistoryToWarehouseJobComponent.

This component shipped with no tests and a crash bug that would have hit
EVERY real invocation: `_row()` read `run.start_time` / `run.end_time` /
`run.mode` directly off a `DagsterRun` (returned by `get_runs()`), but
`DagsterRun` has none of those attributes -- `start_time`/`end_time`/
`create_timestamp` only exist on the `RunRecord` wrapper returned by
`get_run_records()`, and `mode` was removed from Dagster years ago and never
existed on either. Separately, `RunsFilter(updated_after=...)` requires a
real `datetime.datetime` (confirmed via `inspect.signature` against the
installed dagster==1.13.24); the original code passed a raw epoch float.

Live-validated end-to-end against a real, disposable `postgres:16-alpine`
Docker container (real runs, real job execution, real rows read back from
Postgres) before writing this suite -- see the component's README
"Validation notes". These committed tests follow this repo's established
convention for SQL-destination components (see
assets/sinks/dataframe_to_redshift/tests/): the real DagsterInstance /
real job execution path is exercised directly (no mocks), and only the
external DB driver's `connect()` call is mocked, matching how
dataframe_to_redshift mocks `sqlalchemy.create_engine`.
"""
from datetime import datetime

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def real_run_records():
    """A real DagsterInstance with a real successful run and a real failed
    run, so `_row()` is exercised against the actual shape `get_run_records`
    returns -- not a hand-rolled stand-in that could hide an attribute
    mismatch like the one this suite exists to catch.
    """
    with dg.instance_for_test() as instance:

        @dg.job
        def ok_job():
            @dg.op
            def o():
                return 1
            o()

        @dg.job
        def bad_job():
            @dg.op
            def b():
                raise Exception("boom")
            b()

        ok_job.execute_in_process(instance=instance)
        try:
            bad_job.execute_in_process(instance=instance, raise_on_error=False)
        except Exception:
            pass

        yield instance, instance.get_run_records(limit=10)


def test_row_builds_from_real_run_record_no_attribute_error(mod, real_run_records):
    """Regression test for the `DagsterRun`-vs-`RunRecord` attribute bug."""
    _instance, records = real_run_records
    assert len(records) == 2

    for record in records:
        row = mod._row(record)
        assert set(row.keys()) == {
            "run_id", "job_name", "status", "create_timestamp", "start_time",
            "end_time", "duration_seconds", "root_run_id", "parent_run_id", "tags_json",
        }
        assert "mode" not in row
        assert isinstance(row["run_id"], str) and row["run_id"]
        assert row["status"] in ("SUCCESS", "FAILURE")
        # real completed runs always have real numeric start/end/duration
        assert isinstance(row["start_time"], float)
        assert isinstance(row["end_time"], float)
        assert row["end_time"] >= row["start_time"]
        assert row["duration_seconds"] == pytest.approx(row["end_time"] - row["start_time"])
        assert isinstance(row["create_timestamp"], float)


def test_row_handles_in_flight_run_without_crashing(mod):
    """An export job sweeping the whole instance will see its own in-flight
    run (status STARTED, no end_time yet) -- that must not crash _row()."""
    with dg.instance_for_test() as instance:

        @dg.job
        def slow_job():
            @dg.op
            def s():
                return 1
            s()

        slow_job.execute_in_process(instance=instance)
        records = instance.get_run_records(limit=1)
        record = records[0]
        # Simulate "still running" by zeroing out end_time, the shape a real
        # in-flight run has at query time.
        in_flight = record._replace(end_time=None)
        row = mod._row(in_flight)
        assert row["end_time"] is None
        assert row["duration_seconds"] is None
        assert row["start_time"] is not None


def test_export_op_queries_with_real_datetime_not_float(mod, monkeypatch):
    """`RunsFilter(updated_after=...)` requires a real datetime.datetime on
    installed dagster (confirmed via inspect.signature) -- a raw float was
    the original bug. Monkeypatch the instance's own get_run_records to spy
    on exactly what gets passed, while still running the real op end to end."""
    calls = []

    with dg.instance_for_test() as instance:
        original_get_run_records = instance.get_run_records

        def spying_get_run_records(*args, **kwargs):
            calls.append(kwargs.get("filters"))
            return original_get_run_records(*args, **kwargs)

        monkeypatch.setattr(instance, "get_run_records", spying_get_run_records)
        monkeypatch.setattr(mod, "_merge_postgres", lambda cfg, rows: len(rows))

        component = mod.RunHistoryToWarehouseJobComponent(
            job_name="export_run_history",
            schedule=None,
            warehouse="postgres",
        )
        defs = component.build_defs(context=None)
        job = defs.get_job_def("export_run_history")
        result = job.execute_in_process(instance=instance)
        assert result.success

    # Two get_run_records calls: the cursor lookup (no updated_after) and the
    # main sweep (updated_after required, must be a real datetime).
    assert len(calls) == 2
    cursor_filter, sweep_filter = calls
    assert cursor_filter.updated_after is None
    assert isinstance(sweep_filter.updated_after, datetime)


def test_merge_postgres_writes_ten_columns_no_mode(mod, monkeypatch):
    """`mode` was removed from the row shape (DagsterRun has no such
    attribute) -- the Postgres INSERT must match: 10 columns, 10
    placeholders, no `mode`."""
    executed = []

    class _FakeCursor:
        def execute(self, sql, params):
            executed.append((sql, params))

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()
        def commit(self):
            pass
        def close(self):
            pass

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://fake/fake")
    monkeypatch.setattr("psycopg2.connect", lambda dsn: _FakeConn())

    cfg = mod.RunHistoryToWarehouseJobComponent(
        job_name="x", schedule=None, warehouse="postgres", postgres_schema="public", table="dagster_run_history",
    )
    rows = [{
        "run_id": "r1", "job_name": "j", "status": "SUCCESS", "create_timestamp": 1.0,
        "start_time": 2.0, "end_time": 3.0, "duration_seconds": 1.0,
        "root_run_id": None, "parent_run_id": None, "tags_json": "{}",
    }]
    merged = mod._merge_postgres(cfg, rows)
    assert merged == 1
    assert len(executed) == 1
    sql, params = executed[0]
    assert "mode" not in sql
    assert sql.count("%s") == 10
    assert len(params) == 10


def test_merge_snowflake_writes_ten_columns_no_mode(mod, monkeypatch):
    import sys
    import types

    executed = []

    class _FakeCursor:
        def execute(self, sql, *a):
            executed.append(("execute", sql))
        def executemany(self, sql, seq):
            executed.append(("executemany", sql, list(seq)))

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()
        def commit(self):
            pass
        def close(self):
            pass

    fake_connector = types.ModuleType("snowflake.connector")
    fake_connector.connect = lambda **kwargs: _FakeConn()
    fake_snowflake = types.ModuleType("snowflake")
    fake_snowflake.connector = fake_connector
    monkeypatch.setitem(sys.modules, "snowflake", fake_snowflake)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_connector)
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "fake")

    cfg = mod.RunHistoryToWarehouseJobComponent(
        job_name="x", schedule=None, warehouse="snowflake",
        snowflake_account="a", snowflake_user="u", snowflake_warehouse="w",
        snowflake_database="d", snowflake_schema="PUBLIC", table="DAGSTER_RUN_HISTORY",
    )
    rows = [{
        "run_id": "r1", "job_name": "j", "status": "SUCCESS", "create_timestamp": 1.0,
        "start_time": 2.0, "end_time": 3.0, "duration_seconds": 1.0,
        "root_run_id": None, "parent_run_id": None, "tags_json": "{}",
    }]
    merged = mod._merge_snowflake(cfg, rows)
    assert merged == 1
    executemany_calls = [c for c in executed if c[0] == "executemany"]
    assert len(executemany_calls) == 1
    _, insert_sql, seq = executemany_calls[0]
    assert insert_sql.count("%s") == 10
    assert len(seq[0]) == 10
    merge_sql = [c[1] for c in executed if c[0] == "execute" and "MERGE INTO" in c[1]][0]
    assert "s.mode" not in merge_sql
    assert "s.tags_json" in merge_sql


def test_unknown_warehouse_raises(mod):
    cfg = mod.RunHistoryToWarehouseJobComponent(job_name="x", schedule=None, warehouse="redshift")
    with pytest.raises(ValueError, match="Unknown warehouse"):
        mod._merge_to_warehouse(cfg, [], None)


def test_build_defs_schedule_optional(mod):
    with_schedule = mod.RunHistoryToWarehouseJobComponent(
        job_name="x", schedule="*/5 * * * *", default_status="RUNNING", warehouse="postgres",
    )
    defs = with_schedule.build_defs(context=None)
    schedules = list(defs.schedules or [])
    assert len(schedules) == 1
    assert schedules[0].default_status == dg.DefaultScheduleStatus.RUNNING

    without_schedule = mod.RunHistoryToWarehouseJobComponent(
        job_name="y", schedule=None, warehouse="postgres",
    )
    defs2 = without_schedule.build_defs(context=None)
    assert not list(defs2.schedules or [])
