"""ecs_pipes_asset: real end-to-end tests of the demo-mode seam, partition
handling, metadata_checks, and run_task_params building.

Only the actual AWS ECS/CloudWatch API calls would be faked in real-mode use
(via `_get_ecs_client`/`_get_logs_client`) -- these tests exercise `demo_mode`,
where NOTHING about the real `PipesECSClient`/`PipesCloudWatchMessageReader`
code is mocked: a real local script opens a real `dagster_pipes.
open_dagster_pipes()` context, and the component's own internal
`_DemoEcsClient`/`_DemoLogsClient` fakes only the boto3 `ecs`/`logs` API
surface those real classes call. `dg.materialize()` is real throughout.
"""
import os
import stat
import sys

import dagster as dg
import pytest
from dagster._core.errors import DagsterAssetCheckFailedError

from .conftest import load_component_module

mod = load_component_module()
EcsPipesAssetComponent = mod.EcsPipesAssetComponent

FAKE_TASK = """#!{python}
import os
from dagster_pipes import open_dagster_pipes
with open_dagster_pipes() as pipes:
    pk = pipes.extras.get("partition_key", "<none>")
    with open(os.environ["FAKE_TASK_LOG"], "a") as f:
        f.write(pk + "\\n")
    pipes.report_asset_materialization(metadata={{"echo": pk, "schema_valid_rate": 0.99, "docs_failed": 0}})
"""


@pytest.fixture()
def fake_task(tmp_path):
    script = tmp_path / "fake_task.py"
    script.write_text(FAKE_TASK.format(python=sys.executable))
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "task.log"
    os.environ["FAKE_TASK_LOG"] = str(log)
    yield str(script), log
    os.environ.pop("FAKE_TASK_LOG", None)


def _component(**kw):
    attrs = dict(
        asset_name="gpu_batch", task_definition="x:1", container_name="c", cluster="cl",
        subnets=["subnet-1"], aws_region="us-east-2", log_group="/ecs/x",
    )
    attrs.update(kw)
    return EcsPipesAssetComponent(**attrs)


def test_demo_mode_materializes_via_real_pipes_protocol(fake_task):
    script, log = fake_task
    comp = _component(local_script_path=script)
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=True)
    assert result.success
    assert log.read_text().splitlines() == ["<none>"]


def test_demo_mode_passes_partition_key(fake_task):
    script, log = fake_task
    comp = _component(local_script_path=script, partition_type="daily", partition_start="2024-01-01")
    result = dg.materialize(comp.build_defs(None).assets, partition_key="2024-01-03", raise_on_error=True)
    assert result.success
    assert log.read_text().splitlines() == ["2024-01-03"]


def test_backfill_policy_wiring():
    comp = _component(local_script_path="x.py", backfill_policy="single_run", partition_type="daily", partition_start="2024-01-01")
    assert comp.build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    assert _component(local_script_path="x.py").build_defs(None).assets[0].backfill_policy is None
    # backfill_policy is only honored when the asset is actually partitioned.
    unpartitioned = _component(local_script_path="x.py", backfill_policy="single_run")
    assert unpartitioned.build_defs(None).assets[0].backfill_policy is None


def test_single_run_backfill_launches_once_per_partition(fake_task):
    script, log = fake_task
    comp = _component(local_script_path=script, backfill_policy="single_run", partition_type="daily", partition_start="2024-01-01")
    result = dg.materialize(
        comp.build_defs(None).assets, raise_on_error=True,
        tags={"dagster/asset_partition_range_start": "2024-01-02", "dagster/asset_partition_range_end": "2024-01-04"},
    )
    assert result.success
    assert log.read_text().splitlines() == ["2024-01-02", "2024-01-03", "2024-01-04"]
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert {k: v.value for k, v in md.items() if k.endswith("/echo")} == {
        "2024-01-02/echo": "2024-01-02", "2024-01-03/echo": "2024-01-03", "2024-01-04/echo": "2024-01-04",
    }


def test_metadata_check_passes_when_threshold_met(fake_task):
    script, _ = fake_task
    comp = _component(
        local_script_path=script,
        metadata_checks=[{"name": "schema_valid_rate", "metadata_key": "schema_valid_rate", "comparator": "gte", "threshold": 0.98, "severity": "ERROR"}],
    )
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=True)
    assert result.success
    evals = result.get_asset_check_evaluations()
    assert len(evals) == 1 and evals[0].check_name == "schema_valid_rate" and evals[0].passed


def test_metadata_check_fails_and_blocks_when_error_severity(fake_task):
    script, _ = fake_task
    comp = _component(
        local_script_path=script,
        metadata_checks=[{"name": "schema_valid_rate", "metadata_key": "schema_valid_rate", "comparator": "gte", "threshold": 0.999, "severity": "ERROR"}],
    )
    with pytest.raises(DagsterAssetCheckFailedError):
        dg.materialize(comp.build_defs(None).assets, raise_on_error=True)


def test_metadata_check_warn_severity_does_not_block(fake_task):
    script, _ = fake_task
    comp = _component(
        local_script_path=script,
        metadata_checks=[{"name": "schema_valid_rate", "metadata_key": "schema_valid_rate", "comparator": "gte", "threshold": 0.999, "severity": "WARN"}],
    )
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=True)
    assert result.success
    assert not result.get_asset_check_evaluations()[0].passed


def test_mutually_exclusive_launch_config_raises():
    with pytest.raises(Exception, match="at most one"):
        _component(local_script_path="x.py", capacity_provider="gpu", launch_type="FARGATE")


def test_demo_mode_requires_local_script_path():
    with pytest.raises(Exception, match="local_script_path"):
        _component(local_script_path=None)


def test_run_task_params_capacity_provider_sugar():
    comp = _component(local_script_path="x.py", capacity_provider="gpu-g6e")
    params = comp._build_run_task_params()
    assert params["capacityProviderStrategy"] == [{"capacityProvider": "gpu-g6e", "weight": 1}]


def test_run_task_params_capacity_provider_strategy_passthrough():
    strategy = [{"capacityProvider": "gpu-g6e", "weight": 2}, {"capacityProvider": "gpu-g5", "weight": 1}]
    comp = _component(local_script_path="x.py", capacity_provider_strategy=strategy)
    assert comp._build_run_task_params()["capacityProviderStrategy"] == strategy


def test_run_task_params_launch_type():
    comp = _component(local_script_path="x.py", launch_type="FARGATE")
    assert comp._build_run_task_params()["launchType"] == "FARGATE"


def test_run_task_params_network_config():
    comp = _component(local_script_path="x.py", security_groups=["sg-1"], assign_public_ip="ENABLED")
    net = comp._build_run_task_params()["networkConfiguration"]["awsvpcConfiguration"]
    assert net == {"subnets": ["subnet-1"], "securityGroups": ["sg-1"], "assignPublicIp": "ENABLED"}


def test_run_task_params_container_command_and_environment():
    comp = _component(local_script_path="x.py", container_command=["python", "run.py"], container_environment={"FOO": "bar"})
    override = comp._build_run_task_params()["overrides"]["containerOverrides"][0]
    assert override == {"name": "c", "command": ["python", "run.py"], "environment": [{"name": "FOO", "value": "bar"}]}


def test_run_task_params_overrides_deep_merge():
    comp = _component(local_script_path="x.py", run_task_params_overrides={"cluster": "override-cluster", "propagateTags": "TASK_DEFINITION"})
    params = comp._build_run_task_params()
    assert params["cluster"] == "override-cluster"
    assert params["propagateTags"] == "TASK_DEFINITION"
    assert params["taskDefinition"] == "x:1"  # unrelated keys untouched
