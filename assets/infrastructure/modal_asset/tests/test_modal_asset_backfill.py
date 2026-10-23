"""modal_asset's backfill_policy was missing entirely (field + wiring). A single_run backfill hands
the asset a partition-key RANGE; modal_asset already builds its {partition_key} tokens via the
canonical range-safe _partition_vars helper, so wiring single_run should make one 'modal run' cover
the whole range via a comma-joined partition_key token in its JSON kwargs payload. The modal SDK isn't
installed in this environment, so the component naturally falls back to its subprocess CLI path, which
is what's exercised here. Real dg.materialize() run; only subprocess.Popen is mocked."""
import json

import dagster as dg

from .conftest import FakePopen, load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _asset(monkeypatch, backfill_policy=None):
    mod = load_component_module()
    monkeypatch.setattr(mod.subprocess, "Popen", FakePopen)
    comp = mod.ModalAssetComponent(
        asset_name="infra",
        modal_function="myapp::myfn",
        app_file="myapp.py",
        function_name="myfn",
        kwargs={"env": "{partition_key}"},
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def test_backfill_policy_single_run_sets_a_real_backfill_policy(monkeypatch):
    asset = _asset(monkeypatch, backfill_policy="single_run")
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_joins_partition_keys_into_one_modal_run(monkeypatch):
    asset = _asset(monkeypatch, backfill_policy="single_run")
    result = dg.materialize([asset], tags={
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-03",
    })
    assert result.success
    cmd = FakePopen.last_cmd
    payload = json.loads(cmd[-1])
    assert payload == {"env": "2024-01-01,2024-01-02,2024-01-03"}
