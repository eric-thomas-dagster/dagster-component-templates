"""multi_approver_gate's backfill_policy was missing entirely (field + wiring). Before this fix,
`raw_key = context.partition_key if context.has_partition_key else default_approval_key` silently collapsed
any partition-key RANGE onto the generic default_approval_key, so a backfill range's quorum votes would
never be found under their own vote-file prefix. Now a range builds a joined key instead. Real
dg.materialize() run (not direct invocation); votes are real files on disk, nothing is mocked."""
import json

import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _build(tmp_path, backfill_policy=None):
    mod = load_component_module()
    comp = mod.MultiApproverGateComponent(
        asset_name="gate",
        upstream_asset_key="upstream_x",
        approval_dir=str(tmp_path),
        required_approvers=1,
        approver_allowlist=["alice"],
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def _upstream():
    @dg.asset(name="upstream_x", partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def upstream_x():
        return "content"
    return upstream_x


def test_backfill_policy_single_run_sets_a_real_backfill_policy(tmp_path):
    gate = _build(tmp_path, backfill_policy="single_run")
    assert gate.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_counts_votes_under_the_joined_key(tmp_path):
    gate = _build(tmp_path, backfill_policy="single_run")
    (tmp_path / "2024-01-01,2024-01-02.alice.json").write_text(json.dumps({"approved": True}))
    # A stray vote filed against "default" must NOT count for this range.
    (tmp_path / "default.alice.json").write_text(json.dumps({"approved": False}))

    result = dg.materialize(
        [gate, _upstream()],
        tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-02",
        },
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    mats = result.asset_materializations_for_node(gate.node_def.name)
    status = mats[-1].metadata["status"].value
    assert status == "approved", f"expected the joined-key vote to be counted, got status={status!r}"
