"""Real, committed tests for `partition_type: 'cron'`.

Reported gap (component-feedback/2026-10-21-lmr-registry-gaps.md): the date
axis supported only daily/weekly/monthly/hourly/static/dynamic -- no way to
express a "weekdays only" cadence (LMR Partners is reconciling market data
that doesn't exist on weekends). Dagster's own `TimeWindowPartitionsDefinition`
accepts an arbitrary `cron_schedule`, confirmed directly
(`TimeWindowPartitionsDefinition(cron_schedule="0 0 * * 1-5", start="2024-01-01",
fmt="%Y-%m-%d")` produces exactly Mon-Fri partition keys, skipping weekends).
`partition_type='cron'` wraps that: `partition_cron_schedule` is the cron
expression, `partition_cron_fmt` (default `%Y-%m-%d`) only needs overriding
for a sub-daily cadence.
"""
import dagster as dg
import pytest
import sqlalchemy

from .conftest import load_component_module


@pytest.fixture()
def db_url(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'cron.db'}"
    sqlalchemy.create_engine(url).dispose()
    monkeypatch.setenv("TEST_CRON_URL", url)
    return url


def _comp(mod, **kw):
    args = dict(
        asset_name="out_asset",
        connection_url_env_var="TEST_CRON_URL",
        return_dataframe=True,
        sql="SELECT 1 AS x",
    )
    args.update(kw)
    return mod.SqlTransformComponent(**args)


def test_weekday_only_cron_schedule_skips_weekends():
    mod = load_component_module()
    comp = _comp(
        mod,
        partition_type="cron",
        partition_start="2024-01-01",
        partition_cron_schedule="0 0 * * 1-5",
    )
    assets = comp.build_defs(None).assets
    pdef = assets[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
    keys = pdef.get_partition_keys(current_time=__import__("datetime").datetime(2024, 1, 10))
    # 2024-01-06/07 is a Sat/Sun -- must be absent.
    assert "2024-01-05" in keys
    assert "2024-01-06" not in keys
    assert "2024-01-07" not in keys
    assert "2024-01-08" in keys


def test_cron_requires_cron_schedule():
    mod = load_component_module()
    comp = _comp(mod, partition_type="cron", partition_start="2024-01-01")
    try:
        comp.build_defs(None)
        assert False, "expected ValueError"
    except ValueError as e:
        assert "partition_cron_schedule" in str(e) or "cron_schedule" in str(e)


def test_cron_requires_partition_start():
    mod = load_component_module()
    comp = _comp(mod, partition_type="cron", partition_cron_schedule="0 0 * * 1-5")
    try:
        comp.build_defs(None)
        assert False, "expected ValueError"
    except ValueError as e:
        assert "partition_start" in str(e)


def test_weekday_cron_materializes_for_real_and_resolves_partition_key(db_url):
    mod = load_component_module()
    comp = _comp(
        mod,
        partition_type="cron",
        partition_start="2024-01-01",
        partition_cron_schedule="0 0 * * 1-5",
        sql="SELECT '{{ partition_key }}' AS dt",
    )
    assets = comp.build_defs(None).assets
    result = dg.materialize(
        assets, partition_key="2024-01-05",
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    df = result.output_for_node("out_asset")
    assert df["dt"].iloc[0] == "2024-01-05"


def test_cron_dimension_in_partition_dimensions_multi_axis_shape():
    mod = load_component_module()
    comp = _comp(
        mod,
        partition_dimensions=[
            {"name": "date", "type": "cron", "start": "2024-01-01", "cron_schedule": "0 0 * * 1-5"},
            {"name": "region", "type": "static", "values": ["us", "eu"]},
        ],
    )
    assets = comp.build_defs(None).assets
    pdef = assets[0].partitions_def
    assert isinstance(pdef, dg.MultiPartitionsDefinition)
    date_axis = next(d for d in pdef.partitions_defs if d.name == "date")
    assert isinstance(date_axis.partitions_def, dg.TimeWindowPartitionsDefinition)
