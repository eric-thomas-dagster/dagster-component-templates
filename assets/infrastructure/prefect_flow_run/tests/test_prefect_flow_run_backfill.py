"""prefect_flow_run's backfill_policy was missing, and its partition-token logic only handled a single
partition_key (`str(context.partition_key) if context.has_partition_key else ""`) -- a single_run
backfill hands the asset a partition-key RANGE, where context.partition_key raises and has_partition_key
is False, so partition_key/window_start/window_end silently went empty instead of reflecting the range.
Now it uses the canonical range-safe _partition_vars helper like its siblings. Real dg.materialize()
run; only prefect.deployments.run_deployment is stubbed (the actual external call)."""
import dagster as dg

from .conftest import install_fake_prefect, load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _asset(backfill_policy=None):
    run_deployment = install_fake_prefect()
    mod = load_component_module()
    comp = mod.PrefectFlowRunAssetComponent(
        asset_name="infra",
        deployment_name="my-flow/my-deployment",
        parameters={"env": "{partition_key}"},
        wait_for_result=False,
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    return asset, run_deployment


def test_backfill_policy_single_run_sets_a_real_backfill_policy():
    asset, _ = _asset(backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_joins_partition_keys_into_one_deployment_run():
    asset, run_deployment = _asset(backfill_policy="single_run")
    result = dg.materialize(
        [asset],
        resources={"io_manager": dg.InMemoryIOManager()},
        tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    assert run_deployment.last_kwargs["parameters"] == {"env": "2024-01-01,2024-01-02,2024-01-03"}
