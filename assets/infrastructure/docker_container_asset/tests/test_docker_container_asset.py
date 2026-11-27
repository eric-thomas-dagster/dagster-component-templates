"""Real, committed tests for DockerContainerAssetComponent. Mocks dagster_docker.execute_docker_container
(the one real external call -- requires a real Docker daemon) via monkeypatch, exactly as this repo's
convention mocks other paid/external calls, and asserts on the real arguments it receives.
"""
import pytest

from .conftest import materialize

pytest.importorskip("dagster_docker")


def _patch_execute(monkeypatch, mod):
    calls = []

    def fake_execute_docker_container(**kwargs):
        calls.append(kwargs)

    import dagster_docker
    monkeypatch.setattr(dagster_docker, "execute_docker_container", fake_execute_docker_container)
    return calls


def test_env_vars_dict_is_converted_to_key_equals_value_strings(mod, monkeypatch):
    """Real pre-existing bug: execute_docker_container's env_vars is List[str] of "KEY=value" --
    a bare "KEY" entry (no "=") is read from the LAUNCHER's own environment. Passing the dict
    directly iterates its KEYS only, each treated as a bare var name, silently ignoring the
    configured values."""
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", env_vars={"FOO": "bar", "BAZ": "qux"},
    )
    materialize(component)
    assert sorted(calls[0]["env_vars"]) == ["BAZ=qux", "FOO=bar"]


def test_no_env_vars_passes_none(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["env_vars"] is None


def test_command_and_entrypoint_pass_through_separately(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", entrypoint=["/bin/sh", "-c"], command=["python", "run.py"],
    )
    materialize(component)
    assert calls[0]["entrypoint"] == ["/bin/sh", "-c"]
    assert calls[0]["command"] == ["python", "run.py"]


def test_no_entrypoint_passes_none(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["entrypoint"] is None


def test_network_wraps_in_list(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", network="my-net")
    materialize(component)
    assert calls[0]["networks"] == ["my-net"]


def test_no_network_passes_none(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["networks"] is None


def test_registry_credentials_pass_through(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    registry = {"url": "my-registry.example.com", "username": "me", "password": "secret"}
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", registry=registry)
    materialize(component)
    assert calls[0]["registry"] == registry


def test_no_registry_passes_none(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["registry"] is None


def test_container_kwargs_pass_through_raw(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    container_kwargs = {"working_dir": "/app", "user": "1000:1000", "mem_limit": "512m"}
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", container_kwargs=container_kwargs,
    )
    materialize(component)
    assert calls[0]["container_kwargs"] == container_kwargs


def test_no_container_kwargs_passes_none(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1")
    materialize(component)
    assert calls[0]["container_kwargs"] is None


def test_deps_wires_into_asset_deps(mod, monkeypatch):
    import dagster as dg
    _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", deps=["upstream_asset"])
    asset_def = component.build_defs(context=None).assets[0]
    assert dg.AssetKey("upstream_asset") in asset_def.asset_deps[dg.AssetKey("t")]


def test_entrypoint_and_command_accept_comma_separated_strings(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", entrypoint="/bin/sh, -c", command="python, run.py",
    )
    materialize(component)
    assert calls[0]["entrypoint"] == ["/bin/sh", "-c"]
    assert calls[0]["command"] == ["python", "run.py"]
