from unittest.mock import MagicMock

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
