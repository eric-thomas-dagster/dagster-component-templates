"""terraform_asset's backfill_policy was missing entirely (field + wiring). A single_run backfill hands
the asset a partition-key RANGE; terraform_asset already builds its {partition_key}/{partition_start}/
{partition_end} tokens via the canonical range-safe _partition_vars helper, so wiring single_run should
make one terraform plan/apply cover the whole range via a comma-joined partition_key token. Real
dg.materialize() run (not direct invocation, since partition_time_window raises under direct-invocation
style testing); only subprocess.run is mocked."""
import subprocess

import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _asset(monkeypatch, calls, backfill_policy=None, vars_=None):
    mod = load_component_module()

    def fake_run(cmd, cwd, env, capture_output, text):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    comp = mod.TerraformAssetComponent(
        asset_name="infra",
        working_dir="/tmp",
        vars=vars_ or {"env": "{partition_key}"},
        init_on_run=False,
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def test_backfill_policy_single_run_sets_a_real_backfill_policy(monkeypatch):
    calls = []
    asset = _asset(monkeypatch, calls, backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_leaves_dagsters_default(monkeypatch):
    calls = []
    asset = _asset(monkeypatch, calls, backfill_policy=None)
    assert asset.backfill_policy is None


def test_single_run_over_a_range_joins_partition_keys_into_one_plan(monkeypatch):
    calls = []
    asset = _asset(monkeypatch, calls, backfill_policy="single_run")
    result = dg.materialize([asset], tags={
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-03",
    })
    assert result.success
    plan_cmd = next(c for c in calls if "plan" in c)
    var_idx = plan_cmd.index("-var") + 1
    assert plan_cmd[var_idx] == "env=2024-01-01,2024-01-02,2024-01-03"
