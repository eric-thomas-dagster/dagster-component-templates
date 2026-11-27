"""Shared test helpers for SSHResource. Mocks only the paramiko/SSH connection
boundary -- never a live SSH server."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType
from typing import Dict, List

import pytest


class FakeStdout:
    def __init__(self, lines: List[str], exit_code: int):
        self._text = "".join(line if line.endswith("\n") else line + "\n" for line in lines)
        self.channel = type("Channel", (), {"recv_exit_status": lambda self_: exit_code})()

    def read(self):
        return self._text.encode()


class FakeStdin:
    def close(self):
        pass


class FakeStderr:
    def __init__(self, text: str = ""):
        self._text = text

    def read(self):
        return self._text.encode()


class FakeSSHClient:
    """Maps an exact command string to (stdout_lines, exit_code, stderr_text);
    records every exec_command call. Connection lifecycle methods are no-ops."""

    def __init__(self, command_responses: Dict[str, tuple]):
        self.command_responses = command_responses
        self.exec_calls: List[str] = []
        self.connected = False
        self.closed = False

    def set_missing_host_key_policy(self, policy):
        pass

    def load_system_host_keys(self):
        pass

    def connect(self, hostname, port, username, pkey, password, timeout):
        self.connected = True

    def exec_command(self, command, timeout=None):
        self.exec_calls.append(command)
        spec = self.command_responses.get(command, ([], 0, ""))
        lines, exit_code = spec[0], spec[1]
        stderr_text = spec[2] if len(spec) > 2 else ""
        return FakeStdin(), FakeStdout(lines, exit_code), FakeStderr(stderr_text)

    def close(self):
        self.closed = True


class _FakeSSHException(Exception):
    pass


def _make_key_cls(name, accepts):
    """A fake paramiko key class whose from_private_key/from_private_key_file raise
    _FakeSSHException unless `accepts` is True -- lets tests simulate "this PEM/file
    is actually an Ed25519 key, so RSA (tried first) raises and Ed25519 succeeds"."""

    class _Key:
        loaded_via = None

        @classmethod
        def from_private_key(cls, stream):
            if not accepts:
                raise _FakeSSHException(f"not a valid {name} key")
            cls.loaded_via = "content"
            return object()

        @classmethod
        def from_private_key_file(cls, path):
            if not accepts:
                raise _FakeSSHException(f"not a valid {name} key")
            cls.loaded_via = "file"
            return object()

    _Key.__name__ = name
    return _Key


def install_fake_paramiko(monkeypatch, fake_client_factory, *, key_accepts=("RSAKey", "Ed25519Key", "ECDSAKey")):
    """`key_accepts` lists which key classes (by name) successfully load a key --
    the rest raise, simulating the real RSA-then-Ed25519-then-ECDSA fallback chain."""
    fake_mod = types.ModuleType("paramiko")
    fake_mod.SSHClient = fake_client_factory
    fake_mod.AutoAddPolicy = lambda *a, **kw: object()
    fake_mod.PKey = object
    fake_mod.RSAKey = _make_key_cls("RSAKey", "RSAKey" in key_accepts)
    fake_mod.Ed25519Key = _make_key_cls("Ed25519Key", "Ed25519Key" in key_accepts)
    fake_mod.ECDSAKey = _make_key_cls("ECDSAKey", "ECDSAKey" in key_accepts)
    fake_mod.SSHException = _FakeSSHException
    monkeypatch.setitem(sys.modules, "paramiko", fake_mod)
    return fake_mod


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("ssh_resource_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()
