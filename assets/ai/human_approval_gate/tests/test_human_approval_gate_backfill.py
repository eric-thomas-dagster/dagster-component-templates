"""human_approval_gate's backfill_policy was missing entirely (field + wiring). Before this fix,
`raw_key = context.partition_key if context.has_partition_key else default_approval_key` silently collapsed
any partition-key RANGE onto the generic default_approval_key token file, so a backfill range could never
find its own approval token (and, worse, would collide with an unrelated unpartitioned run's token). Now a
range builds a joined key instead. Real dg.materialize() run (not direct invocation); the approval token is
a real file on disk, nothing is mocked."""
import json

import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _build(tmp_path, backfill_policy=None):
    mod = load_component_module()
    comp = mod.HumanApprovalGateComponent(
        asset_name="gate",
        upstream_asset_key="upstream_x",
        approval_dir=str(tmp_path),
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


def test_single_run_over_a_range_reads_the_joined_key_token_not_default(tmp_path):
    gate = _build(tmp_path, backfill_policy="single_run")
    (tmp_path / "2024-01-01,2024-01-02.json").write_text(json.dumps({"approved": True, "approver": "me"}))
    # A stray "default.json" must NOT be what gets read for a range.
    (tmp_path / "default.json").write_text(json.dumps({"approved": False}))

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
    assert status == "approved", f"expected the joined-key token to be read, got status={status!r}"


def test_single_partition_still_uses_its_own_key_not_default(tmp_path):
    gate = _build(tmp_path, backfill_policy="single_run")
    (tmp_path / "2024-01-01.json").write_text(json.dumps({"approved": True, "approver": "me"}))
    result = dg.materialize([gate, _upstream()], partition_key="2024-01-01")
    assert result.success
    mats = result.asset_materializations_for_node(gate.node_def.name)
    assert mats[-1].metadata["status"].value == "approved"
