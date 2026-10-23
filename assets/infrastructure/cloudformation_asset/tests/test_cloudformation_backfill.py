"""cloudformation_asset's backfill_policy was missing entirely (field + wiring). A single_run backfill
hands the asset a partition-key RANGE; cloudformation_asset already builds its {partition_key} tokens
via the canonical range-safe _partition_vars helper, so wiring single_run should make one stack
create/update cover the whole range via a comma-joined partition_key stack parameter. Real
dg.materialize() run; only boto3.Session is mocked (the actual AWS call)."""
import dagster as dg

from .conftest import FakeCfClient, FakeSession, load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _asset(monkeypatch, backfill_policy=None):
    mod = load_component_module()
    import boto3
    monkeypatch.setattr(boto3, "Session", FakeSession)
    comp = mod.CloudFormationAssetComponent(
        asset_name="infra",
        stack_name="my-stack",
        template_url="https://example.com/template.yaml",
        parameters={"env": "{partition_key}"},
        poll_interval_seconds=0,
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def test_backfill_policy_single_run_sets_a_real_backfill_policy(monkeypatch):
    asset = _asset(monkeypatch, backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_joins_partition_keys_into_one_stack_create(monkeypatch):
    asset = _asset(monkeypatch, backfill_policy="single_run")
    result = dg.materialize([asset], tags={
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-03",
    })
    assert result.success
    params = {p["ParameterKey"]: p["ParameterValue"] for p in FakeCfClient.create_stack_kwargs["Parameters"]}
    assert params == {"env": "2024-01-01,2024-01-02,2024-01-03"}
