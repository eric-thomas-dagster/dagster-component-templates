"""Tests for CronScheduleComponent's `calendar_condition` -- a calendar gate on top of a cron
expression for conditions cron alone can't express: the actual last day of the month (28-31,
varies) and business-day (Mon-Fri) boundaries. Exercised two ways: `_calendar_condition_matches`
directly against known calendar facts (unit-level, no Dagster machinery), and a real
ScheduleDefinition's `evaluate_tick` at specific simulated dates (same pattern as
test_cadence_once_and_end_date.py's end_date/add_logical_date_tag tests).
"""
import datetime
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("cron_schedule_component_calendar", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

CronScheduleComponent = _mod.CronScheduleComponent
_calendar_condition_matches = _mod._calendar_condition_matches


def _comp(**kw):
    kw.setdefault("schedule_name", "s")
    kw.setdefault("asset_keys", ["orders"])
    return CronScheduleComponent(**kw)


@pytest.fixture
def asset_def():
    @dg.asset(name="orders")
    def orders():
        return 1
    return orders


# ---- _calendar_condition_matches: known calendar facts ----
def test_last_day_of_month_28_day_february():
    assert _calendar_condition_matches("last_day_of_month", datetime.date(2026, 2, 28)) is True
    assert _calendar_condition_matches("last_day_of_month", datetime.date(2026, 2, 27)) is False


def test_last_day_of_month_31_day_month():
    assert _calendar_condition_matches("last_day_of_month", datetime.date(2026, 1, 31)) is True
    assert _calendar_condition_matches("last_day_of_month", datetime.date(2026, 1, 30)) is False


def test_last_day_of_month_leap_year_february():
    assert _calendar_condition_matches("last_day_of_month", datetime.date(2028, 2, 29)) is True  # 2028 is a leap year
    assert _calendar_condition_matches("last_day_of_month", datetime.date(2028, 2, 28)) is False


def test_last_business_day_of_month_when_month_end_is_a_weekend():
    # 2026-01-31 is a Saturday -> last business day is 2026-01-30 (Friday)
    assert datetime.date(2026, 1, 31).weekday() == 5
    assert _calendar_condition_matches("last_business_day_of_month", datetime.date(2026, 1, 30)) is True
    assert _calendar_condition_matches("last_business_day_of_month", datetime.date(2026, 1, 31)) is False
    assert _calendar_condition_matches("last_business_day_of_month", datetime.date(2026, 1, 29)) is False


def test_last_business_day_of_month_when_month_end_is_already_a_weekday():
    # 2026-02-28 is a Saturday, so walk back to Friday 2026-02-27
    assert datetime.date(2026, 2, 28).weekday() == 5
    assert _calendar_condition_matches("last_business_day_of_month", datetime.date(2026, 2, 27)) is True
    # 2026-04-30 is a Thursday -- already a business day, no walk-back needed
    assert datetime.date(2026, 4, 30).weekday() == 3
    assert _calendar_condition_matches("last_business_day_of_month", datetime.date(2026, 4, 30)) is True


def test_first_business_day_of_month_when_the_1st_is_a_weekend():
    # 2026-03-01 is a Sunday -> first business day is 2026-03-02 (Monday)
    assert datetime.date(2026, 3, 1).weekday() == 6
    assert _calendar_condition_matches("first_business_day_of_month", datetime.date(2026, 3, 2)) is True
    assert _calendar_condition_matches("first_business_day_of_month", datetime.date(2026, 3, 1)) is False
    assert _calendar_condition_matches("first_business_day_of_month", datetime.date(2026, 3, 3)) is False


def test_first_business_day_of_month_when_the_1st_is_already_a_weekday():
    # 2026-04-01 is a Wednesday
    assert datetime.date(2026, 4, 1).weekday() == 2
    assert _calendar_condition_matches("first_business_day_of_month", datetime.date(2026, 4, 1)) is True


# ---- model validation ----
def test_unknown_calendar_condition_is_a_clear_error():
    with pytest.raises(ValueError, match="calendar_condition must be one of"):
        _comp(cron_expression="0 9 * * *", calendar_condition="nope")


def test_calendar_condition_incompatible_with_cadence_once():
    with pytest.raises(ValueError, match="cadence='once' has no recurring calendar"):
        _comp(cadence="once", calendar_condition="last_day_of_month")


def test_calendar_condition_requires_explicit_cron_in_partitioned_path():
    comp = _comp(partition_type="daily", partition_start="2020-01-01", calendar_condition="last_day_of_month")
    with pytest.raises(ValueError, match="cron_expression is required"):
        comp.build_defs(None)


# ---- real ScheduleDefinition.evaluate_tick at specific simulated dates ----
def test_unpartitioned_last_day_of_month_fires_only_on_the_true_last_day(asset_def):
    comp = _comp(cron_expression="0 23 28-31 * *", calendar_condition="last_day_of_month")
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    dg.Definitions(assets=[asset_def], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        # 2026-02-28 is the real last day of Feb 2026 -- fires
        ctx_last = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2026, 2, 28, 23, tzinfo=datetime.timezone.utc)
        )
        data_last = sched.evaluate_tick(ctx_last)
        assert len(data_last.run_requests) == 1

        # 2026-02-29 doesn't exist in 2026 (not a leap year), but 2026-01-29/30 are real
        # candidate-cron ticks (28-31) that are NOT the true last day -- must be skipped
        ctx_not_last = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2026, 1, 29, 23, tzinfo=datetime.timezone.utc)
        )
        data_not_last = sched.evaluate_tick(ctx_not_last)
        assert data_not_last.run_requests == []
        assert "calendar_condition" in data_not_last.skip_message


def test_unpartitioned_last_business_day_of_month_skips_a_weekend_month_end(asset_def):
    comp = _comp(cron_expression="0 23 28-31 * *", calendar_condition="last_business_day_of_month")
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    dg.Definitions(assets=[asset_def], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        # 2026-01-31 is a Saturday -- real last day, but not a business day -- skipped
        ctx_weekend = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2026, 1, 31, 23, tzinfo=datetime.timezone.utc)
        )
        assert sched.evaluate_tick(ctx_weekend).run_requests == []

        # 2026-01-30 is the preceding Friday -- the real last BUSINESS day -- fires
        ctx_business = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=datetime.datetime(2026, 1, 30, 23, tzinfo=datetime.timezone.utc)
        )
        assert len(sched.evaluate_tick(ctx_business).run_requests) == 1


def test_partitioned_first_business_day_of_month_skip_path():
    """The partitioned branch's calendar_condition gate runs BEFORE partition-window resolution
    (get_last_partition_window(), which resolves relative to the real wall clock, not the
    simulated scheduled_execution_time below) -- so the SkipReason path is what's deterministically
    testable here; the matching-date/fires-a-real-RunRequest behavior is proven by the unpartitioned
    tests above, which share the exact same _calendar_condition_matches call and gate placement."""
    @dg.asset(name="orders2", partitions_def=dg.DailyPartitionsDefinition(start_date="2020-01-01"))
    def orders2(context):
        return context.partition_key

    comp = _comp(
        asset_keys=["orders2"], partition_type="daily", partition_start="2020-01-01",
        cron_expression="0 9 1-3 * *", calendar_condition="first_business_day_of_month",
    )
    defs = comp.build_defs(None)
    sched = list(defs.schedules)[0]
    job = list(defs.jobs)[0]
    full_defs = dg.Definitions(assets=[orders2], jobs=[job], schedules=[sched])

    with dg.instance_for_test() as instance:
        repo_def = full_defs.get_repository_def()

        # 2026-03-01 is a Sunday -- first business day is 2026-03-02 -- this date never matches
        ctx_sunday = dg.build_schedule_context(
            instance=instance, repository_def=repo_def,
            scheduled_execution_time=datetime.datetime(2026, 3, 1, 9, tzinfo=datetime.timezone.utc),
        )
        data_sunday = sched.evaluate_tick(ctx_sunday)
        assert data_sunday.run_requests == []
        assert "calendar_condition" in data_sunday.skip_message
