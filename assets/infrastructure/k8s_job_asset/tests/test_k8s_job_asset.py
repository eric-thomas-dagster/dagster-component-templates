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


# --- use_pipes: PipesK8sClient variant --------------------------------------------------

def _patch_pipes_client(monkeypatch, mod, materialize_result):
    calls = []

    class FakeInvocation:
        def get_materialize_result(self):
            return materialize_result

    class FakePipesK8sClient:
        def __init__(self, *a, **k):
            pass

        def run(self, **kwargs):
            calls.append(kwargs)
            return FakeInvocation()

    import dagster_k8s
    monkeypatch.setattr(dagster_k8s, "PipesK8sClient", FakePipesK8sClient)
    return calls


def test_use_pipes_false_by_default_calls_execute_k8s_job_not_pipes(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    pipes_calls = _patch_pipes_client(monkeypatch, mod, materialize_result=None)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert len(calls) == 1
    assert len(pipes_calls) == 0


def test_use_pipes_true_calls_pipes_client_not_execute_k8s_job(mod, monkeypatch):
    import dagster as dg
    execute_calls = _patch_execute(monkeypatch, mod)
    pipes_calls = _patch_pipes_client(
        monkeypatch, mod, materialize_result=dg.MaterializeResult(metadata={"ok": dg.MetadataValue.bool(True)}),
    )
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", use_pipes=True)
    result = materialize(component)
    assert result.success
    assert len(execute_calls) == 0
    assert len(pipes_calls) == 1
    assert pipes_calls[0]["image"] == "img:v1"


def test_use_pipes_passes_extras_and_env(mod, monkeypatch):
    import dagster as dg
    pipes_calls = _patch_pipes_client(monkeypatch, mod, materialize_result=dg.MaterializeResult())
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", use_pipes=True,
        pipes_extras={"region": "us-east-1"}, env_vars={"FOO": "bar"},
    )
    materialize(component)
    assert pipes_calls[0]["extras"] == {"region": "us-east-1"}
    assert pipes_calls[0]["env"] == {"FOO": "bar"}


def test_use_pipes_builds_base_pod_spec_from_resources_volumes_and_service_account(mod, monkeypatch):
    import dagster as dg
    pipes_calls = _patch_pipes_client(monkeypatch, mod, materialize_result=dg.MaterializeResult())
    volumes = [{"name": "data", "emptyDir": {}}]
    volume_mounts = [{"name": "data", "mountPath": "/data"}]
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", use_pipes=True,
        cpu_limit="500m", memory_limit="1Gi", volumes=volumes, volume_mounts=volume_mounts,
        service_account_name="ml-runner", image_pull_secrets=["my-secret"], image_pull_policy="Always",
    )
    materialize(component)
    spec = pipes_calls[0]["base_pod_spec"]
    assert spec["containers"][0]["resources"] == {"limits": {"cpu": "500m", "memory": "1Gi"}}
    assert spec["containers"][0]["volume_mounts"] == volume_mounts
    assert spec["containers"][0]["image_pull_policy"] == "Always"
    assert spec["volumes"] == volumes
    assert spec["service_account_name"] == "ml-runner"
    assert spec["image_pull_secrets"] == [{"name": "my-secret"}]


def test_use_pipes_with_no_overrides_passes_no_base_pod_spec(mod, monkeypatch):
    import dagster as dg
    pipes_calls = _patch_pipes_client(monkeypatch, mod, materialize_result=dg.MaterializeResult())
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", use_pipes=True)
    materialize(component)
    assert pipes_calls[0]["base_pod_spec"] is None


def test_use_pipes_timeout_maps_to_pod_wait_timeout(mod, monkeypatch):
    import dagster as dg
    pipes_calls = _patch_pipes_client(monkeypatch, mod, materialize_result=dg.MaterializeResult())
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", use_pipes=True, timeout_seconds=90)
    materialize(component)
    assert pipes_calls[0]["pod_wait_timeout"] == 90.0


# --- new fields: args / labels / node_selector / tolerations / affinity / envFrom / kubeconfig ---

def test_args_labels_envfrom_passthrough_to_execute_k8s_job(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", args=["--verbose"], labels={"team": "ml"},
        env_from_config_maps=["app-config"], env_from_secrets=["app-secret"],
    )
    materialize(component)
    assert calls[0]["args"] == ["--verbose"]
    assert calls[0]["labels"] == {"team": "ml"}
    assert calls[0]["env_config_maps"] == ["app-config"]
    assert calls[0]["env_secrets"] == ["app-secret"]


def test_node_selector_tolerations_affinity_build_pod_spec_config(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    tolerations = [{"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}]
    affinity = {"node_affinity": {"requiredDuringSchedulingIgnoredDuringExecution": {}}}
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", node_selector={"disktype": "ssd"},
        tolerations=tolerations, affinity=affinity,
    )
    materialize(component)
    assert calls[0]["pod_spec_config"] == {
        "node_selector": {"disktype": "ssd"}, "tolerations": tolerations, "affinity": affinity,
    }


def test_no_scheduling_overrides_passes_no_pod_spec_config(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["pod_spec_config"] is None


def test_kubeconfig_file_disables_incluster_config(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", kubeconfig_file="/home/me/.kube/config")
    materialize(component)
    assert calls[0]["kubeconfig_file"] == "/home/me/.kube/config"
    assert calls[0]["load_incluster_config"] is False


def test_no_kubeconfig_file_keeps_incluster_config_true(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["kubeconfig_file"] is None
    assert calls[0]["load_incluster_config"] is True


def test_use_pipes_args_labels_and_scheduling_fields_build_base_pod_spec_and_meta(mod, monkeypatch):
    import dagster as dg
    pipes_calls = _patch_pipes_client(monkeypatch, mod, materialize_result=dg.MaterializeResult())
    tolerations = [{"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}]
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", use_pipes=True,
        args=["--verbose"], labels={"team": "ml"}, node_selector={"disktype": "ssd"},
        tolerations=tolerations, env_from_config_maps=["app-config"], env_from_secrets=["app-secret"],
    )
    materialize(component)
    spec = pipes_calls[0]["base_pod_spec"]
    assert spec["containers"][0]["args"] == ["--verbose"]
    assert spec["containers"][0]["env_from"] == [
        {"config_map_ref": {"name": "app-config"}}, {"secret_ref": {"name": "app-secret"}},
    ]
    assert spec["node_selector"] == {"disktype": "ssd"}
    assert spec["tolerations"] == tolerations
    assert pipes_calls[0]["base_pod_meta"] == {"labels": {"team": "ml"}}


def test_use_pipes_kubeconfig_file_passed_to_client_constructor(mod, monkeypatch):
    import dagster as dg
    kwargs_seen = {}

    class FakeInvocation:
        def get_materialize_result(self):
            return dg.MaterializeResult()

    class FakePipesK8sClient:
        def __init__(self, **kwargs):
            kwargs_seen.update(kwargs)

        def run(self, **kwargs):
            return FakeInvocation()

    import dagster_k8s
    monkeypatch.setattr(dagster_k8s, "PipesK8sClient", FakePipesK8sClient)

    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", use_pipes=True, kubeconfig_file="/home/me/.kube/config",
    )
    materialize(component)
    assert kwargs_seen["kubeconfig_file"] == "/home/me/.kube/config"
    assert kwargs_seen["load_incluster_config"] is False


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


def test_no_job_name_override_by_default(mod, monkeypatch):
    """Real pre-existing bug: k8s_job_name was hardcoded to context.run.run_id[:32], which is
    IDENTICAL across every step of the same run -- two k8s_job_asset steps in one run collided on
    the same Kubernetes Job name. Leaving k8s_job_name unset now lets dagster-k8s derive its own
    name, which it already guarantees is unique per run AND per op/asset."""
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["k8s_job_name"] is None


def test_explicit_k8s_job_name_passes_through(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", k8s_job_name="my-fixed-job-name")
    materialize(component)
    assert calls[0]["k8s_job_name"] == "my-fixed-job-name"


def test_kube_context_passed_as_private_kwarg_to_execute_k8s_job(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", kubeconfig_file="/home/me/.kube/config", kube_context="staging",
    )
    materialize(component)
    assert calls[0]["_kubeconfig_file_context"] == "staging"


def test_kube_context_passed_to_pipes_client_constructor(mod, monkeypatch):
    import dagster as dg

    kwargs_seen = {}

    class FakeInvocation:
        def get_materialize_result(self):
            return dg.MaterializeResult()

    class FakePipesK8sClient:
        def __init__(self, **kwargs):
            kwargs_seen.update(kwargs)

        def run(self, **kwargs):
            return FakeInvocation()

    import dagster_k8s
    monkeypatch.setattr(dagster_k8s, "PipesK8sClient", FakePipesK8sClient)

    component = mod.K8sJobAssetComponent(
        asset_name="t", image="img:v1", use_pipes=True,
        kubeconfig_file="/home/me/.kube/config", kube_context="staging",
    )
    materialize(component)
    assert kwargs_seen["kube_context"] == "staging"


def test_delete_failed_k8s_jobs_unset_omits_kwarg(mod, monkeypatch):
    """Unset: don't pass delete_failed_k8s_jobs at all, so dagster-k8s's own default (True) applies."""
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert "delete_failed_k8s_jobs" not in calls[0]


def test_delete_failed_k8s_jobs_false_passes_through(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", delete_failed_k8s_jobs=False)
    materialize(component)
    assert calls[0]["delete_failed_k8s_jobs"] is False


def test_ttl_seconds_after_finished_merged_into_job_spec_config(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1", ttl_seconds_after_finished=3600)
    materialize(component)
    assert calls[0]["job_spec_config"] == {"ttl_seconds_after_finished": 3600}


def test_no_ttl_passes_none_job_spec_config(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.K8sJobAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["job_spec_config"] is None
