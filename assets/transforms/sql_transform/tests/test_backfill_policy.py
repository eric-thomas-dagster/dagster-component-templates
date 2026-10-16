"""SqlTransformComponent: partitioning + backfill_policy.

Real SQL against a real SQLite file through the component's own SQLAlchemy
path (return_dataframe=true). Nothing is mocked. A single_run backfill is
simulated with the dagster/asset_partition_range_* run tags and
dg.InMemoryIOManager() (the default filesystem IO manager cannot persist a
multi-partition output).

Real bug fixed alongside: partition_type/partition_start were declared fields
but build_defs never built a partitions_def, so the asset was silently
unpartitioned and `{{ partition_key }}` never rendered (StrictUndefined).
"""
import dagster as dg
import pandas as pd
import pytest
import sqlalchemy

from .conftest import load_component_module

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-01",
    "dagster/asset_partition_range_end": "2024-01-02",
}


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_url(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'events.db'}"
    eng = sqlalchemy.create_engine(url)
    pd.DataFrame({
        "id": range(6),
        "event_date": ["2024-01-01"] * 2 + ["2024-01-02"] * 2 + ["2024-01-03"] * 2,
    }).to_sql("events", eng, index=False)
    eng.dispose()
    monkeypatch.setenv("TEST_SQL_TRANSFORM_URL", url)
    return url


WINDOW_SQL = (
    "SELECT * FROM events WHERE event_date >= substr('{{ partition_time_window_start }}', 1, 10) "
    "AND event_date < substr('{{ partition_time_window_end }}', 1, 10) ORDER BY id"
)


def _comp(mod, sql=WINDOW_SQL, **kw):
    args = dict(asset_name="out_asset", connection_url_env_var="TEST_SQL_TRANSFORM_URL",
                return_dataframe=True, sql=sql,
                partition_type="daily", partition_start="2024-01-01")
    args.update(kw)
    return mod.SqlTransformComponent(**args)


def _run(comp, partition_key=None, tags=None):
    assets = comp.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()},
                            partition_key=partition_key, tags=tags or {})
    return assets, result


def test_partition_type_now_actually_partitions_the_asset(mod, db_url):
    assets, _ = _run(_comp(mod), partition_key="2024-01-01")
    assert assets[0].partitions_def is not None


def test_single_partition_renders_partition_key_and_window(mod, db_url):
    comp = _comp(mod, sql="SELECT * FROM events WHERE event_date = '{{ partition_key }}' ORDER BY id")
    _, result = _run(comp, partition_key="2024-01-02")
    assert result.success
    assert result.output_for_node("out_asset")["id"].tolist() == [2, 3]
    _, result = _run(_comp(mod), partition_key="2024-01-02")
    assert result.output_for_node("out_asset")["id"].tolist() == [2, 3]


def test_backfill_policy_wiring(mod, db_url):
    assert _comp(mod, backfill_policy="single_run").build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    assert _comp(mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=4).build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=4)
    assert _comp(mod).build_defs(None).assets[0].backfill_policy is None
    assert _comp(mod, partition_type=None, partition_start=None, backfill_policy="single_run").build_defs(None).assets[0].backfill_policy is None


def test_single_run_range_renders_window_spanning_whole_range(mod, db_url):
    _, result = _run(_comp(mod, backfill_policy="single_run"), tags=RANGE_TAGS)
    assert result.success
    # 2024-01-01 .. 2024-01-02 inclusive; 2024-01-03 must be excluded
    assert result.output_for_node("out_asset")["id"].tolist() == [0, 1, 2, 3]


def test_single_run_range_leaves_partition_key_undefined_loudly(mod, db_url):
    comp = _comp(mod, sql="SELECT * FROM events WHERE event_date = '{{ partition_key }}'",
                 backfill_policy="single_run")
    assets = comp.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()},
                            tags=RANGE_TAGS, raise_on_error=False)
    assert not result.success


def test_partition_keys_list_available_in_range_run(mod, db_url):
    comp = _comp(mod, sql="SELECT {{ partition_keys | length }} AS n", backfill_policy="single_run")
    _, result = _run(comp, tags=RANGE_TAGS)
    assert result.output_for_node("out_asset")["n"].tolist() == [2]
