"""cached_asset wrapping a partitioned inner asset: the wrapper used to (a)
drop the inner asset's backfill_policy and (b) key the cache on "" for any
partition RANGE, so two different ranges would collide on one cache entry.
Real parquet cache on tmp_path; nothing paid/network to mock."""
import dagster as dg
import pandas as pd

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")
EXECUTIONS = []


def _inner():
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def events(context: dg.AssetExecutionContext) -> pd.DataFrame:
        EXECUTIONS.append(list(context.partition_keys))
        return pd.DataFrame({"day": list(context.partition_keys)})
    return events


def _wrapped(tmp_path):
    mod = load_component_module()
    comp = mod.CachedAssetComponent(cache_dir=str(tmp_path / "cache"), wraps={"type": "x.Y"})
    return comp._wrap_single_asset(_inner())


def _run(asset_def, start, end):
    return dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()},
        tags={"dagster/asset_partition_range_start": start, "dagster/asset_partition_range_end": end},
    )


def test_wrapper_keeps_inner_backfill_policy(tmp_path):
    assert _wrapped(tmp_path).backfill_policy == dg.BackfillPolicy.single_run()


def test_distinct_ranges_get_distinct_cache_entries_and_same_range_hits(tmp_path):
    EXECUTIONS.clear()
    w = _wrapped(tmp_path)
    r1 = _run(w, "2024-01-02", "2024-01-03")
    assert r1.success and len(r1.output_for_node("events")) == 2
    r2 = _run(w, "2024-01-04", "2024-01-06")     # different range -> must NOT be a cache hit
    assert r2.success
    assert sorted(r2.output_for_node("events")["day"]) == ["2024-01-04", "2024-01-05", "2024-01-06"]
    assert EXECUTIONS == [["2024-01-02", "2024-01-03"], ["2024-01-04", "2024-01-05", "2024-01-06"]]
    r3 = _run(w, "2024-01-02", "2024-01-03")     # same range -> hit, inner not re-run
    assert r3.success and len(EXECUTIONS) == 2
    assert sorted(r3.output_for_node("events")["day"]) == ["2024-01-02", "2024-01-03"]
