"""pulumi_asset's backfill_policy was missing entirely (field + wiring). A single_run backfill hands
the asset a partition-key RANGE; pulumi_asset already builds its {partition_key}/{partition_start}/
{partition_end} tokens via the canonical range-safe _partition_vars helper, so wiring single_run should
make one pulumi preview cover the whole range via a comma-joined partition_key token passed through
--config. Real dg.materialize() run; only subprocess.run is mocked."""
import subprocess

import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _asset(monkeypatch, calls, backfill_policy=None):
    mod = load_component_module()

    def fake_run(cmd, cwd, env, capture_output, text):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    comp = mod.PulumiAssetComponent(
        asset_name="infra",
        stack_name="org/project/dev",
        working_dir="/tmp",
        operation="preview",
        config={"env": "{partition_key}"},
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def test_backfill_policy_single_run_sets_a_real_backfill_policy(monkeypatch):
    asset = _asset(monkeypatch, [], backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_joins_partition_keys_into_one_preview(monkeypatch):
    calls = []
    asset = _asset(monkeypatch, calls, backfill_policy="single_run")
    result = dg.materialize([asset], tags={
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-03",
    })
    assert result.success
    preview_cmd = next(c for c in calls if "preview" in c)
    config_idx = preview_cmd.index("--config") + 1
    assert preview_cmd[config_idx] == "env=2024-01-01,2024-01-02,2024-01-03"
