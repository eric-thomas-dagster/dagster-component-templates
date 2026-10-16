"""timeout_asset wrapping a partitioned inner asset: the wrapper used to drop
the inner backfill_policy, and `context.partition_key` raises for a range so
per_partition_timeout overrides were silently ignored (default used) for a
single_run backfill. Real wrapped asset run through dg.materialize()."""
import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _inner():
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def work(context: dg.AssetExecutionContext):
        return dg.MaterializeResult(metadata={"n": len(context.partition_keys)})
    return work


def _wrapped():
    mod = load_component_module()
    comp = mod.TimeoutAssetComponent(
        timeout_seconds=100,
        per_partition_timeout={"2024-01-02": 10, "2024-01-03": 20},
        wraps={"type": "x.Y"},
    )
    return comp._wrap_single_asset(_inner())


def _timeout_meta(result):
    ev = [e for e in result.get_asset_materialization_events()][0]
    return ev.materialization.metadata["timeout_seconds"].value


def test_policy_preserved():
    assert _wrapped().backfill_policy == dg.BackfillPolicy.single_run()


def test_single_partition_uses_its_override():
    r = dg.materialize([_wrapped()], partition_key="2024-01-02")
    assert r.success and _timeout_meta(r) == 10


def test_range_sums_per_partition_overrides():
    r = dg.materialize([_wrapped()], tags={
        "dagster/asset_partition_range_start": "2024-01-02",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    # 10 + 20 + default 100 for the un-overridden 2024-01-04
    assert _timeout_meta(r) == 130
