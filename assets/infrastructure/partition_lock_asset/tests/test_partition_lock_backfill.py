"""partition_lock_asset wrapping a partitioned inner asset had two real gaps:

1. _wrap_single_asset dropped the inner asset's backfill_policy entirely (unlike cached_asset,
   sla_asset, and timeout_asset, which all forward backfill_policy=asset_def.backfill_policy) --
   wrapping a single_run-backfillable asset silently reverted it to Dagster's default.
2. _get_partition_key caught ANY exception from context.partition_key (including the
   DagsterInvariantViolationError a partition-key RANGE raises) and fell back to the literal
   "__unpartitioned__" -- once backfill_policy is actually forwarded, a real single_run backfill
   reaches this with a genuine range, and every key in it would collapse onto the same lock name,
   losing per-partition isolation and letting unrelated backfill ranges collide on one lock.

Real dg.materialize() runs against a real DagsterInstance.ephemeral() (the event_log lock backend
needs a real instance to observe/acquire against); nothing is mocked."""
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
    comp = mod.PartitionLockAssetComponent(ttl_seconds=60, on_conflict="wait", backend="event_log")
    return comp._wrap_single_asset(_inner())


def _lock_meta(result, key="partition_key"):
    ev = next(e for e in result.get_asset_materialization_events())
    return ev.materialization.metadata[key].value


def test_backfill_policy_is_forwarded_from_the_wrapped_asset():
    assert _wrapped().backfill_policy == dg.BackfillPolicy.single_run()


def test_single_partition_lock_key_is_the_bare_partition_key():
    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([_wrapped()], instance=instance, partition_key="2024-01-02")
    assert result.success
    assert _lock_meta(result) == "2024-01-02"


def test_range_lock_key_is_the_joined_range_not_unpartitioned():
    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([_wrapped()], instance=instance, tags={
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-03",
    })
    assert result.success
    assert _lock_meta(result) == "2024-01-01,2024-01-02,2024-01-03"
