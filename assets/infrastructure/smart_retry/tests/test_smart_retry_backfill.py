"""smart_retry shell compute: DAGSTER_PARTITION_KEY used to be set only when
has_partition_key was True, which is False for a single_run backfill range --
so the command ran once with NO partition key. Real subprocess, real
dg.materialize(); nothing to mock."""
import dagster as dg

from .conftest import load_component_module


def _component(tmp_path, **kw):
    mod = load_component_module()
    log = tmp_path / "keys.log"
    return mod.SmartRetryComponent(
        asset_name="sr", compute={"kind": "shell", "cmd": f'echo "${{DAGSTER_PARTITION_KEY:-none}}" >> {log}'},
        partition_type="daily", partition_start="2024-01-01", **kw,
    ), log


def _run(c, partition_key=None, tags=None):
    return dg.materialize(c.build_defs(None).assets, partition_key=partition_key, tags=tags or {})


def test_single_partition(tmp_path):
    c, log = _component(tmp_path)
    assert _run(c, partition_key="2024-01-03").success
    assert log.read_text().split() == ["2024-01-03"]


def test_wiring(tmp_path):
    c, _ = _component(tmp_path, backfill_policy="single_run")
    assert c.build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    c2, _ = _component(tmp_path)
    assert c2.build_defs(None).assets[0].backfill_policy is None


def test_single_run_backfill_runs_command_per_partition(tmp_path):
    c, log = _component(tmp_path, backfill_policy="single_run")
    r = _run(c, tags={"dagster/asset_partition_range_start": "2024-01-02",
                      "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert log.read_text().split() == ["2024-01-02", "2024-01-03", "2024-01-04"]
