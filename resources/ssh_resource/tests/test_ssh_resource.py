import shlex
import warnings
from unittest.mock import MagicMock

import pytest

from .conftest import FakeSSHClient, install_fake_paramiko


def _resource(mod, **overrides):
    kwargs = dict(host="ssh.example.com", username="demo")
    kwargs.update(overrides)
    return mod.SSHResource(**kwargs)


def test_run_command_returns_stdout_stderr_exit_code(mod, monkeypatch):
    fake_client = FakeSSHClient({"echo hi": (["hi"], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    result = _resource(mod).run_command("echo hi")

    assert result["stdout"] == "hi\n"
    assert result["stderr"] == ""
    assert result["exit_code"] == 0
    assert fake_client.connected
    assert fake_client.closed


def test_non_zero_exit_code_does_not_raise(mod, monkeypatch):
    fake_client = FakeSSHClient({"false": ([], 1, "boom")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    result = _resource(mod).run_command("false")

    assert result["exit_code"] == 1
    assert result["stderr"] == "boom"


def test_environment_is_exported_via_shell_prefix(mod, monkeypatch):
    fake_client = FakeSSHClient({"export FOO=bar BAZ='qux quux'; echo $FOO": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    result = _resource(mod).run_command("echo $FOO", environment={"FOO": "bar", "BAZ": "qux quux"})

    assert result["exit_code"] == 0
    assert fake_client.exec_calls == ["export FOO=bar BAZ='qux quux'; echo $FOO"]


def test_working_dir_prefix(mod, monkeypatch):
    fake_client = FakeSSHClient({"cd /opt/app && ls": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    result = _resource(mod).run_command("ls", working_dir="/opt/app")

    assert result["exit_code"] == 0
    assert fake_client.exec_calls == ["cd /opt/app && ls"]


def test_no_environment_leaves_command_unprefixed(mod, monkeypatch):
    fake_client = FakeSSHClient({"echo hi": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    _resource(mod).run_command("echo hi")

    assert fake_client.exec_calls == ["echo hi"]


def test_component_registers_resource():
    import pathlib
    import importlib.util

    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("ssh_resource_component", here / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    component = mod.SSHResourceComponent(
        host_env_var="MY_SSH_HOST", username_env_var="MY_SSH_USER",
    )
    defs = component.build_defs(context=None)
    resource = defs.resources["ssh_resource"]
    assert isinstance(resource, mod.SSHResource)


def test_timeout_zero_is_not_replaced_by_default(mod, monkeypatch):
    seen_timeouts = []

    class TimeoutCapturingClient(FakeSSHClient):
        def exec_command(self, command, timeout=None):
            seen_timeouts.append(timeout)
            return super().exec_command(command, timeout=timeout)

    fake_client = TimeoutCapturingClient({"echo hi": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    _resource(mod, timeout_seconds=300).run_command("echo hi", timeout=0)

    assert seen_timeouts == [0]


def test_private_key_path_falls_back_through_key_types(mod, monkeypatch, tmp_path):
    """RSA fails to parse, Ed25519 succeeds -- the same fallback chain the PEM-content
    path already had, now applied to key-FILE loading too."""
    fake_client = FakeSSHClient({"echo hi": ([], 0, "")})
    fake_paramiko = install_fake_paramiko(monkeypatch, lambda: fake_client, key_accepts=("Ed25519Key",))

    key_file = tmp_path / "id_ed25519"
    key_file.write_text("fake-ed25519-key-content")

    result = _resource(mod, private_key_path=str(key_file)).run_command("echo hi")

    assert result["exit_code"] == 0
    assert fake_paramiko.RSAKey.loaded_via is None  # tried, failed
    assert fake_paramiko.Ed25519Key.loaded_via == "file"  # succeeded


def test_working_dir_is_shell_quoted(mod, monkeypatch):
    dangerous_dir = "/opt/app; rm -rf /"
    quoted = shlex.quote(dangerous_dir)
    fake_client = FakeSSHClient({f"cd {quoted} && ls": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    result = _resource(mod).run_command("ls", working_dir=dangerous_dir)

    assert result["exit_code"] == 0
    assert fake_client.exec_calls == [f"cd {quoted} && ls"]


def test_known_hosts_check_false_warns(mod, monkeypatch):
    fake_client = FakeSSHClient({"echo hi": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    with pytest.warns(UserWarning, match="known_hosts_check=False"):
        _resource(mod, known_hosts_check=False).run_command("echo hi")


def test_known_hosts_check_true_does_not_warn(mod, monkeypatch):
    fake_client = FakeSSHClient({"echo hi": ([], 0, "")})
    install_fake_paramiko(monkeypatch, lambda: fake_client)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _resource(mod, known_hosts_check=True).run_command("echo hi")


def test_component_resolves_literal_host_and_username(mod):
    component = mod.SSHResourceComponent(host="literal.example.com", username="demo-user")
    defs = component.build_defs(context=None)
    resource = defs.resources["ssh_resource"]
    assert resource.host == "literal.example.com"
    assert resource.username == "demo-user"


def test_component_requires_host_or_host_env_var(mod):
    component = mod.SSHResourceComponent(username="demo-user")
    with pytest.raises(ValueError, match="host"):
        component.build_defs(context=None)


def test_component_requires_username_or_username_env_var(mod):
    component = mod.SSHResourceComponent(host="literal.example.com")
    with pytest.raises(ValueError, match="username"):
        component.build_defs(context=None)
