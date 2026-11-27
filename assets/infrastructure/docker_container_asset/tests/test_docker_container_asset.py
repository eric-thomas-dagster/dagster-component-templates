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


def test_numeric_looking_env_var_value_is_coerced_to_str(mod, monkeypatch):
    """Dagster's component-YAML loading can hand a str-typed field an int/bool even when the
    YAML source was quoted (confirmed via a direct repro against create_defs_folder_sandbox) --
    env_vars is Dict[str, str], so a numeric-looking value like {"N": 1} fails Pydantic
    validation unless coerced back to a string first."""
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", env_vars={"N": 1, "FLAG": True},
    )
    materialize(component)
    assert sorted(calls[0]["env_vars"]) == ["FLAG=true", "N=1"]


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


def test_force_pull_pre_pulls_on_delegate_path(mod, monkeypatch):
    """force_pull doesn't require the direct path -- it just pre-pulls via a throwaway client
    before calling execute_docker_container (which only pulls on a cache miss)."""
    _patch_execute(monkeypatch, mod)
    pulled = []

    class _FakeClient:
        class images:
            @staticmethod
            def pull(image):
                pulled.append(image)

    import docker
    monkeypatch.setattr(docker, "from_env", lambda: _FakeClient())
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", force_pull=True)
    materialize(component)
    assert pulled == ["img:v1"]


def test_auto_remove_mounts_ports_merge_into_container_kwargs_on_delegate_path(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", auto_remove=True,
        mounts=[{"target": "/data", "source": "my-vol", "type": "volume"}],
        ports={"8080/tcp": 8080},
    )
    materialize(component)
    kwargs = calls[0]["container_kwargs"]
    assert kwargs["auto_remove"] is True
    assert kwargs["ports"] == {"8080/tcp": 8080}
    import docker.types
    assert len(kwargs["mounts"]) == 1
    assert isinstance(kwargs["mounts"][0], docker.types.Mount)


def test_container_kwargs_with_name_key_is_not_mutated_in_place(mod, monkeypatch):
    """container_kwargs passed by the caller must not be mutated when auto_remove/mounts/ports
    merge a copy on top of it."""
    calls = _patch_execute(monkeypatch, mod)
    original = {"working_dir": "/app"}
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", container_kwargs=original, auto_remove=True,
    )
    materialize(component)
    assert original == {"working_dir": "/app"}
    assert calls[0]["container_kwargs"]["auto_remove"] is True


class _FakeContainer:
    def __init__(self, exit_code=0, log_lines=(b"hello\n",)):
        self._exit_code = exit_code
        self._log_lines = log_lines
        self.started = False

    def start(self):
        self.started = True

    def logs(self, **kwargs):
        return iter(self._log_lines)

    def wait(self):
        return {"StatusCode": self._exit_code}


def _fake_direct_client(monkeypatch, mod, exit_code=0, log_lines=(b"hello\n",), image_not_found_once=False):
    """Patch the `docker` module import inside component.py's direct-client path."""
    import docker
    import docker.errors

    create_calls = []
    pull_calls = []
    state = {"raised": False}

    class _Images:
        @staticmethod
        def pull(image):
            pull_calls.append(image)

    class _Containers:
        @staticmethod
        def create(image, **kwargs):
            create_calls.append({"image": image, **kwargs})
            if image_not_found_once and not state["raised"]:
                state["raised"] = True
                raise docker.errors.ImageNotFound("no such image")
            return _FakeContainer(exit_code=exit_code, log_lines=log_lines)

    class _Networks:
        @staticmethod
        def get(name):
            return type("N", (), {"connect": staticmethod(lambda c: None)})()

    class _FakeClient:
        images = _Images()
        containers = _Containers()
        networks = _Networks()
        login_calls = []

        def login(self, **kwargs):
            self.login_calls.append(kwargs)

    client = _FakeClient()
    monkeypatch.setattr(docker, "from_env", lambda: client)
    monkeypatch.setattr(docker, "DockerClient", lambda **kwargs: client)
    return client, create_calls, pull_calls


def test_container_name_switches_to_direct_path_and_sets_real_name(mod, monkeypatch):
    client, create_calls, _ = _fake_direct_client(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", container_name="my-fixed-name")
    materialize(component)
    assert create_calls[0]["name"] == "my-fixed-name"


def test_container_name_does_not_call_delegate_execute(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    _fake_direct_client(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", container_name="fixed")
    materialize(component)
    assert calls == []


def test_direct_path_retries_on_image_not_found(mod, monkeypatch):
    client, create_calls, pull_calls = _fake_direct_client(monkeypatch, mod, image_not_found_once=True)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", container_name="fixed")
    materialize(component)
    assert len(create_calls) == 2
    assert pull_calls == ["img:v1"]


def test_direct_path_raises_on_nonzero_exit(mod, monkeypatch):
    _fake_direct_client(monkeypatch, mod, exit_code=1)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", container_name="fixed")
    with pytest.raises(Exception, match="exit code 1"):
        materialize(component)


def test_retrieve_output_captures_logs_as_metadata(mod, monkeypatch):
    _fake_direct_client(monkeypatch, mod, log_lines=(b"line one\n", b"line two\n"))
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", retrieve_output=True)
    result = materialize(component)
    mats = result.get_asset_materialization_events()
    metadata = mats[0].materialization.metadata
    assert "container_output" in metadata
    assert "line one" in metadata["container_output"].value
    assert "line two" in metadata["container_output"].value


def test_retrieve_output_switches_to_direct_path_even_without_container_name(mod, monkeypatch):
    calls = _patch_execute(monkeypatch, mod)
    _fake_direct_client(monkeypatch, mod)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", retrieve_output=True)
    materialize(component)
    assert calls == []


def test_docker_base_url_switches_to_direct_path_with_base_url(mod, monkeypatch):
    import docker
    client, create_calls, _ = _fake_direct_client(monkeypatch, mod)
    captured = {}
    monkeypatch.setattr(docker, "DockerClient", lambda **kwargs: captured.update(kwargs) or client)
    component = mod.DockerContainerAssetComponent(asset_name="t", image="img:v1", docker_base_url="tcp://remote:2376")
    materialize(component)
    assert captured["base_url"] == "tcp://remote:2376"


def test_tls_requires_both_cert_and_key(mod):
    with pytest.raises(Exception, match="tls_client_key_path"):
        mod.DockerContainerAssetComponent(
            asset_name="t", image="img:v1", tls_client_cert_path="/cert.pem",
        )
    with pytest.raises(Exception, match="tls_client_cert_path"):
        mod.DockerContainerAssetComponent(
            asset_name="t", image="img:v1", tls_client_key_path="/key.pem",
        )


def test_tls_config_builds_real_tls_config_object(mod, monkeypatch, tmp_path):
    import docker
    client, create_calls, _ = _fake_direct_client(monkeypatch, mod)
    captured = {}
    monkeypatch.setattr(docker, "DockerClient", lambda **kwargs: captured.update(kwargs) or client)
    # docker.tls.TLSConfig's real constructor validates these paths with os.path.isfile, so the
    # test must point at real files, not fabricated nonexistent paths.
    ca = tmp_path / "ca.pem"; cert = tmp_path / "cert.pem"; key = tmp_path / "key.pem"
    ca.write_text("ca"); cert.write_text("cert"); key.write_text("key")
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", docker_base_url="tcp://remote:2376",
        tls_ca_cert_path=str(ca), tls_client_cert_path=str(cert), tls_client_key_path=str(key),
    )
    materialize(component)
    assert isinstance(captured["tls"], docker.tls.TLSConfig)


def test_registry_login_called_on_direct_path(mod, monkeypatch):
    client, create_calls, _ = _fake_direct_client(monkeypatch, mod)
    registry = {"url": "my-registry.example.com", "username": "me", "password": "secret"}
    component = mod.DockerContainerAssetComponent(
        asset_name="t", image="img:v1", container_name="fixed", registry=registry,
    )
    materialize(component)
    assert client.login_calls == [{"registry": "my-registry.example.com", "username": "me", "password": "secret"}]
