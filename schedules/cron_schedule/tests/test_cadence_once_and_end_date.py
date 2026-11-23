"""Tests for the Airflow-migration gaps added to CronScheduleComponent:

- cadence='once' (Airflow's @once): fires a single run via a cursor-gated sensor,
  never a ScheduleDefinition. Verified live first (tick 1 -> RunRequest + cursor set,
  tick 2 -> SkipReason) before being wired into the component.
- end_date: a tick-level skip (un-partitioned) and the partitions_def's own real
  end_date (partitioned), verified live via ScheduleDefinition's execution_fn +
  evaluate_tick against a real job/repository_def.
- add_logical_date_tag: tags each run with Airflow's logical_date concept -- the
  scheduled trigger time (un-partitioned) or the partition window's start
  (partitioned).
"""
import datetime
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("cron_schedule_component_cadence", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

CronScheduleComponent = _mod.CronScheduleComponent


@pytest.fixture
def asset_def():
    @dg.asset(name="orders")
    def orders():
        return 1
    return orders


def _comp(**kw):
    kw.setdefault("schedule_name", "s")
    kw.setdefault("asset_keys", ["orders"])
    return CronScheduleComponent(**kw)


# ---- cadence validation ----

def test_cadence_once_rejects_cron_expression():
    with pytest.raises(Exception, match="cron_expression"):
        _comp(cadence="once", cron_expression="0 9 * * *")


def test_cadence_once_rejects_partitioning():
    with pytest.raises(Exception, match="partition_type"):
        _comp(cadence="once", partition_type="daily", partition_start="2026-01-01")


def test_cadence_once_rejects_end_date():
    with pytest.raises(Exception, match="end_date"):
        _comp(cadence="once", end_date="2026-01-01")


def test_cadence_once_rejects_add_logical_date_tag():
    with pytest.raises(Exception, match="add_logical_date_tag"):
        _comp(cadence="once", add_logical_date_tag=True)


def test_unknown_cadence_rejected():
    with pytest.raises(Exception, match="cadence"):
        _comp(cadence="bogus")


# ---- cadence='once': builds a sensor that fires exactly once ----

def test_cadence_once_builds_sensor_not_schedule(asset_def):
    defs = _comp(cadence="once").build_defs(None)
    assert defs.schedules == [] or not list(defs.schedules or [])
    assert len(list(defs.sensors)) == 1


def test_cadence_once_fires_exactly_one_run(asset_def):
    defs = _comp(cadence="once").build_defs(None)
    full_defs = dg.Definitions(assets=[asset_def], jobs=list(defs.jobs), sensors=list(defs.sensors))
    sensor = list(defs.sensors)[0]

    with dg.instance_for_test() as instance:
        ctx1 = dg.build_sensor_context(instance=instance, definitions=full_defs)
        result1 = sensor(ctx1)
        assert isinstance(result1, dg.RunRequest)

        ctx2 = dg.build_sensor_context(instance=instance, definitions=full_defs, cursor=ctx1.cursor)
        result2 = sensor(ctx2)
        assert isinstance(result2, dg.SkipReason)


# ---- end_date: un-partitioned path skips once past the cutoff ----

def test_end_date_unpartitioned_skips_after_cutoff(asset_def):
    comp = _comp(cron_expression="0 9 * * *", end_date="2026-01-01")
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    full_defs = dg.Definitions(assets=[asset_def], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        ctx_before = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2025, 12, 1, tzinfo=datetime.timezone.utc)
        )
        data_before = sched.evaluate_tick(ctx_before)
        assert len(data_before.run_requests) == 1

        ctx_after = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2026, 2, 1, tzinfo=datetime.timezone.utc)
        )
        data_after = sched.evaluate_tick(ctx_after)
        assert data_after.run_requests == []
        assert "end_date" in data_after.skip_message


def test_add_logical_date_tag_unpartitioned(asset_def):
    comp = _comp(cron_expression="0 9 * * *", add_logical_date_tag=True)
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    full_defs = dg.Definitions(assets=[asset_def], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        ctx = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2025, 12, 1, 9, tzinfo=datetime.timezone.utc)
        )
        data = sched.evaluate_tick(ctx)
        assert data.run_requests[0].tags["logical_date"] == "2025-12-01T09:00:00+00:00"


def test_static_tags_still_applied_alongside_logical_date(asset_def):
    comp = _comp(cron_expression="0 9 * * *", add_logical_date_tag=True, tags={"team": "data"})
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]

    with dg.instance_for_test() as instance:
        ctx = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2025, 12, 1, tzinfo=datetime.timezone.utc)
        )
        data = sched.evaluate_tick(ctx)
        assert data.run_requests[0].tags["team"] == "data"
        assert "logical_date" in data.run_requests[0].tags


# ---- end_date / add_logical_date_tag on the partitioned path ----

def test_partitioned_requires_explicit_cron_when_advanced(asset_def):
    comp = _comp(partition_type="daily", partition_start="2020-01-01", add_logical_date_tag=True)
    with pytest.raises(ValueError, match="cron_expression is required"):
        comp.build_defs(None)


def test_partitioned_end_date_caps_partitions_def(asset_def):
    comp = _comp(
        partition_type="daily", partition_start="2026-01-01", end_date="2026-01-05",
        cron_expression="0 0 * * *",
    )
    defs = comp.build_defs(None)
    job = list(defs.jobs)[0]
    assert job.partitions_def.get_partition_keys() == ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]


def test_partitioned_add_logical_date_tag_resolves_window_start(asset_def):
    @dg.asset(name="orders2", partitions_def=dg.DailyPartitionsDefinition(start_date="2020-01-01"))
    def orders2(context):
        return context.partition_key

    comp = _comp(
        asset_keys=["orders2"], partition_type="daily", partition_start="2020-01-01",
        add_logical_date_tag=True, cron_expression="0 0 * * *",
    )
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    full_defs = dg.Definitions(assets=[orders2], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        repo_def = full_defs.get_repository_def()
        ctx = dg.build_schedule_context(instance=instance, repository_def=repo_def)
        data = sched.evaluate_tick(ctx)
        assert len(data.run_requests) == 1
        rr = data.run_requests[0]
        assert rr.partition_key is not None
        assert "logical_date" in rr.tags

        result = dg.materialize(
            [orders2], instance=instance, partition_key=rr.partition_key, tags=rr.tags,
        )
        assert result.success


def test_skip_first_tick_skips_until_a_run_has_actually_fired(asset_def):
    comp = _comp(cron_expression="0 9 * * 1", skip_first_tick=True)
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]

    with dg.instance_for_test() as instance:
        ctx1 = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2025, 12, 1, 9, tzinfo=datetime.timezone.utc)
        )
        data1 = sched.evaluate_tick(ctx1)
        assert data1.run_requests == []
        assert "skip_first_tick" in data1.skip_message

        # Once a run exists tagged with this schedule's name, later ticks fire normally.
        dg.materialize([asset_def], instance=instance, tags={"dagster/schedule_name": "s"})
        ctx2 = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2025, 12, 8, 9, tzinfo=datetime.timezone.utc)
        )
        data2 = sched.evaluate_tick(ctx2)
        assert len(data2.run_requests) == 1


def test_skip_first_tick_false_fires_on_the_first_tick(asset_def):
    comp = _comp(cron_expression="0 9 * * 1", skip_first_tick=False)
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]

    with dg.instance_for_test() as instance:
        ctx = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2025, 12, 1, 9, tzinfo=datetime.timezone.utc)
        )
        data = sched.evaluate_tick(ctx)
        assert len(data.run_requests) == 1


def test_skip_first_tick_partitioned(asset_def):
    @dg.asset(name="orders3", partitions_def=dg.DailyPartitionsDefinition(start_date="2020-01-01"))
    def orders3(context):
        return context.partition_key

    comp = _comp(
        asset_keys=["orders3"], partition_type="daily", partition_start="2020-01-01",
        skip_first_tick=True, cron_expression="0 0 * * *",
    )
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    full_defs = dg.Definitions(assets=[orders3], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        repo_def = full_defs.get_repository_def()
        ctx = dg.build_schedule_context(instance=instance, repository_def=repo_def)
        data = sched.evaluate_tick(ctx)
        assert data.run_requests == []
        assert "skip_first_tick" in data.skip_message


def test_add_interval_tags_requires_partitioning():
    with pytest.raises(ValueError, match="add_interval_tags requires partition_type"):
        _comp(cron_expression="0 9 * * *", add_interval_tags=True)


def test_cadence_once_rejects_add_interval_tags():
    with pytest.raises(ValueError, match="add_interval_tags"):
        _comp(cadence="once", add_interval_tags=True)


def test_partitioned_add_interval_tags_resolves_window_bounds(asset_def):
    @dg.asset(name="orders3", partitions_def=dg.DailyPartitionsDefinition(start_date="2020-01-01"))
    def orders3(context):
        return context.partition_key

    comp = _comp(
        asset_keys=["orders3"], partition_type="daily", partition_start="2020-01-01",
        add_interval_tags=True, cron_expression="0 0 * * *",
    )
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    full_defs = dg.Definitions(assets=[orders3], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        repo_def = full_defs.get_repository_def()
        ctx = dg.build_schedule_context(instance=instance, repository_def=repo_def)
        data = sched.evaluate_tick(ctx)
        rr = data.run_requests[0]
        assert "data_interval_start" in rr.tags
        assert "data_interval_end" in rr.tags
        assert rr.tags["data_interval_start"] < rr.tags["data_interval_end"]
