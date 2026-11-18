"""Real, committed tests for K8sJobAssetComponent. Mocks dagster_k8s.execute_k8s_job (the one
real external call -- requires a real k8s cluster) via monkeypatch, exactly as this repo's
convention mocks other paid/external calls, and asserts on the real arguments it receives.
"""
import pytest

from .conftest import materialize

pytest.importorskip("dagster_k8s")


def _patch_execute(monkeypatch, mod):
    calls = []

    def fake_execute_k8s_job(**kwargs):
        calls.append(kwargs)

    import dagster_k8s
    monkeypatch.setattr(dagster_k8s, "execute_k8s_job", fake_execute_k8s_job)
    return calls


def test_env_vars_dict_is_converted_to_key_equals_value_strings(mod, monkeypatch):
    """Real pre-existing bug: execute_k8s_job's env_vars is List[str] of "KEY=value" (parsed via
    dagster._core.utils.parse_env_var), not a dict -- passing the dict directly iterates its
    KEYS only, each treated as a bare var name to read from the LAUNCHER's own environment."""
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", env_vars={"FOO": "bar", "BAZ": "qux"},
    )
    materialize(component)
    assert sorted(calls[0]["env_vars"]) == ["BAZ=qux", "FOO=bar"]


def test_no_env_vars_passes_none(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["env_vars"] is None


def test_image_pull_policy_and_secrets_and_service_account_pass_through(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", image_pull_policy="IfNotPresent",
        image_pull_secrets=["my-secret"], service_account_name="ml-runner",
    )
    materialize(component)
    assert calls[0]["image_pull_policy"] == "IfNotPresent"
    assert calls[0]["image_pull_secrets"] == [{"name": "my-secret"}]
    assert calls[0]["service_account_name"] == "ml-runner"


def test_volumes_and_volume_mounts_pass_through_raw(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    volumes = [{"name": "data", "emptyDir": {}}]
    volume_mounts = [{"name": "data", "mountPath": "/data"}]
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", volumes=volumes, volume_mounts=volume_mounts,
    )
    materialize(component)
    assert calls[0]["volumes"] == volumes
    assert calls[0]["volume_mounts"] == volume_mounts


def test_timeout_seconds_maps_to_the_real_timeout_param(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", timeout_seconds=120)
    materialize(component)
    assert calls[0]["timeout"] == 120


def test_cpu_and_memory_limits_build_resources_dict(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", cpu_limit="500m", memory_limit="1Gi")
    materialize(component)
    assert calls[0]["resources"] == {"limits": {"cpu": "500m", "memory": "1Gi"}}


def test_no_limits_passes_none_resources(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["resources"] is None


def test_command_override_passes_through(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", command=["python", "run.py"])
    materialize(component)
    assert calls[0]["command"] == ["python", "run.py"]


def test_retry_policy_wires_through(mod, monkeypatch):
    _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", retry_policy_max_retries=2, retry_policy_delay_seconds=5,
    )
    asset_def = component.build_defs(context=None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 2 and rp.delay == 5


def test_static_partition_type_wires_through(mod, monkeypatch):
    import dagster as dg
    _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", partition_type="static", partition_values="a,b,c",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="b",
    )
    assert result.success
