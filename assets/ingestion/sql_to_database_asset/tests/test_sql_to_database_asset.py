"""Committed tests for SQLToDatabaseAssetComponent's backfill_policy
support, added alongside the rest of the repo's backfill_policy rollout.

This component writes directly to SQL via df.to_sql() and returns a bare
MaterializeResult with no IO-managed value -- no IO manager constraint to
work around. It has a genuinely MIXED shape though: time-based partitions
(daily/weekly/monthly/hourly) with a watermark_column already get a real
bounded [start, end) window via context.partition_time_window, which
dagster itself keeps correctly range-aware for a backfill range with NO
code change needed -- so a single_run backfill over those just widens to
ONE combined query spanning the whole range (efficient, not one query per
day). Static/dynamic/multi partitions have no natural time window and
read context.partition_key directly (for an open-ended watermark AND for
destination_table templating) -- these genuinely need a per-key loop,
same shape as the "_to_database_asset" family, since destination_table
may legitimately differ per key.

Uses real temp-file SQLite databases for both source and destination --
no network/API to mock for this component at all.
"""
import dagster as dg
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def src_db(tmp_path):
    db_path = tmp_path / "src.db"
    engine = create_engine(f"sqlite:///{db_path}")
    # event_date carries a mid-day time component (not bare "2024-01-02")
    # so lexicographic string comparison against the bounded window's
    # isoformat()'d (tz-aware, midnight) bounds behaves correctly -- a
    # bare date-only string would sort BEFORE ">= 2024-01-02T00:00:00+00:00"
    # since it's a strict prefix, incorrectly excluding same-day rows.
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "total": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "event_date": [
            "2024-01-02T12:00:00", "2024-01-02T18:00:00",
            "2024-01-03T12:00:00",
            "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00",
        ],
    })
    df.to_sql("events", engine, index=False)
    engine.dispose()
    return f"sqlite:///{db_path}"


def _make_component(mod, src_url, dst_url, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_SRC_DB", src_url)
    monkeypatch.setenv("FAKE_DST_DB", dst_url)
    kwargs = dict(
        asset_name="events_sync",
        source_url_env_var="FAKE_SRC_DB",
        destination_url_env_var="FAKE_DST_DB",
        source_table="events",
        destination_table="events_sink",
        if_exists="append",
    )
    kwargs.update(overrides)
    return mod.SQLToDatabaseAssetComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        partition_key=partition_key,
        tags=tags or {},
    )


def _row_count(db_url, table):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        n = conn.execute(text(f"select count(*) from {table}")).scalar()
    engine.dispose()
    return n


def test_single_partition_bounded_window_reads_one_days_rows(mod, src_db, tmp_path, monkeypatch):
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    component = _make_component(
        mod, src_db, dst_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        watermark_column="event_date",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    assert _row_count(dst_url, "events_sink") == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, src_db, tmp_path, monkeypatch):
    component = _make_component(
        mod, src_db, f"sqlite:///{tmp_path / 'dst.db'}", monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        watermark_column="event_date", backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, src_db, tmp_path, monkeypatch):
    component = _make_component(
        mod, src_db, f"sqlite:///{tmp_path / 'dst.db'}", monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        watermark_column="event_date",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_over_time_based_partition_uses_one_combined_query(mod, src_db, tmp_path, monkeypatch):
    """Real bug before the fix (shared with the other 10 Tier C
    components): context.partition_key (used to build a per-day bounded
    window) doesn't exist for a multi-partition single_run backfill --
    has_partition_key is False for a range. Confirmed fixed: a 3-day
    single_run backfill now issues ONE combined query spanning
    context.partition_time_window's full range (not one query per day --
    dagster's own partition_time_window is already range-aware) and writes
    all 6 matching rows in one go."""
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    component = _make_component(
        mod, src_db, dst_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        watermark_column="event_date", backfill_policy="single_run",
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-04",
        },
    )
    assert result.success
    assert _row_count(dst_url, "events_sink") == 6


def test_single_run_backfill_with_templated_destination_table_on_bounded_range_raises(mod, src_db, tmp_path, monkeypatch):
    """A time-based partition range collapses to ONE combined query -- it
    can't be split across multiple {partition_key}-templated destination
    tables, so this must raise a clear error rather than silently writing
    everything to a literal '...{partition_key}...' table name."""
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    component = _make_component(
        mod, src_db, dst_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        watermark_column="event_date", backfill_policy="single_run",
        destination_table="events_sink_{partition_key}",
    )
    with pytest.raises(Exception, match="partition_key"):
        _materialize(
            component,
            tags={
                "dagster/asset_partition_range_start": "2024-01-02",
                "dagster/asset_partition_range_end": "2024-01-04",
            },
        )


def test_single_run_backfill_over_static_partition_writes_each_partitions_own_table(mod, src_db, tmp_path, monkeypatch):
    """Static partitions have no natural time window -- each key must be
    processed independently (same shape as the _to_database_asset family),
    since destination_table may legitimately differ per key."""
    dst_url = f"sqlite:///{tmp_path / 'dst.db'}"
    component = _make_component(
        mod, src_db, dst_url, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        destination_table="events_sink_{partition_key}",
        backfill_policy="single_run",
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": "store_a",
            "dagster/asset_partition_range_end": "store_c",
        },
    )
    assert result.success
    assert _row_count(dst_url, "events_sink_store_a") == 6
    assert _row_count(dst_url, "events_sink_store_b") == 6
    assert _row_count(dst_url, "events_sink_store_c") == 6
