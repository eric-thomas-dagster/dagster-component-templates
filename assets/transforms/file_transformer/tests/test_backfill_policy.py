"""FileTransformerComponent: backfill_policy + range-aware partition filtering
of the upstream DataFrame (upstream-asset mode), verified through real
dg.materialize() runs that write a real CSV into tmp_path. A single_run
backfill is simulated with the dagster/asset_partition_range_* run tags."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-01",
    "dagster/asset_partition_range_end": "2024-01-02",
}


@pytest.fixture()
def mod():
    return load_component_module()


def _src_df():
    return pd.DataFrame({
        "id": range(6),
        "event_date": ["2024-01-01"] * 2 + ["2024-01-02"] * 2 + ["2024-01-03"] * 2,
    })


def _run(mod, tmp_path, partitioned, partition_key=None, tags=None, **kw):
    df = _src_df()

    @dg.asset(name="src")
    def src():
        return df

    args = dict(asset_name="out_asset", upstream_asset_key="src",
                output_format="csv", output_directory=str(tmp_path))
    if partitioned:
        args.update(partition_type="daily", partition_start="2024-01-01",
                    partition_date_column="event_date")
    args.update(kw)
    comp = mod.FileTransformerComponent(**args)
    assets = comp.build_defs(None).assets
    result = dg.materialize(
        [src, *assets],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key, tags=tags or {},
    )
    assert result.success
    return assets, pd.read_csv(tmp_path / "out_asset.csv")


def test_backfill_policy_wiring(mod, tmp_path):
    assets, _ = _run(mod, tmp_path, True, partition_key="2024-01-01", backfill_policy="single_run")
    assert assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    assets, _ = _run(mod, tmp_path, True, partition_key="2024-01-01",
                     backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert assets[0].backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)
    assets, _ = _run(mod, tmp_path, True, partition_key="2024-01-01")
    assert assets[0].backfill_policy is None


def test_single_partition_writes_that_days_rows(mod, tmp_path):
    _, out = _run(mod, tmp_path, True, partition_key="2024-01-02")
    assert out["id"].tolist() == [2, 3]


def test_single_run_range_writes_exactly_the_ranges_rows(mod, tmp_path):
    """Pre-fix: has_partition_key is False for a range, so the file contained
    all 6 rows (incl. 2024-01-03) instead of the 4 in the requested range."""
    _, out = _run(mod, tmp_path, True, backfill_policy="single_run", tags=RANGE_TAGS)
    assert out["id"].tolist() == [0, 1, 2, 3]
