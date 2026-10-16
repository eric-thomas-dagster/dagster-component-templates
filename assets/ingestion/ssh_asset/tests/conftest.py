"""Shared test helpers for SSHAssetComponent.

The real `paramiko` package isn't installed in this test environment, and
this component does a module-level `import paramiko` (not lazy), so a
fake module must be injected into sys.modules BEFORE the component module
itself is loaded. Uses monkeypatch.setitem (not a bare assignment) so it's
undone after each test rather than permanently shadowing a real `paramiko`
for the rest of the test session if one is ever installed (a real cross-
test pollution bug of exactly this shape was found+fixed elsewhere in this
session's test suite).
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType
from typing import Dict, List

import pytest


class FakeStdout:
    def __init__(self, lines: List[str], exit_code: int):
        self._lines = list(lines) + [""]
        self._idx = 0
        self.channel = type("Channel", (), {"recv_exit_status": lambda self_: exit_code})()

    def readline(self):
        line = self._lines[self._idx] if self._idx < len(self._lines) else ""
        self._idx += 1
        return line


class FakeStdin:
    def close(self):
        pass


class FakeStderr:
    def __init__(self, text: str = ""):
        self._text = text

    def read(self):
        return self._text.encode()


class FakeSSHClient:
    """Maps an exact command string to (stdout_lines, exit_code); records
    every exec_command call. Connection lifecycle methods are no-ops."""

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
        lines, exit_code = self.command_responses.get(command, ([], 0))
        return FakeStdin(), FakeStdout(lines, exit_code), FakeStderr()

    def open_sftp(self):
        raise NotImplementedError("SFTP not used in these tests")

    def close(self):
        self.closed = True


def install_fake_paramiko(monkeypatch, fake_client_factory):
    """`fake_client_factory` is called with no args each time the component
    does `paramiko.SSHClient()` -- return a fresh FakeSSHClient (or reuse a
    single instance across the whole test, matching the component's own
    one-connection-per-run design)."""
    fake_mod = types.ModuleType("paramiko")
    fake_mod.SSHClient = fake_client_factory
    fake_mod.AutoAddPolicy = lambda *a, **kw: object()
    fake_mod.PKey = object
    fake_mod.RSAKey = type("RSAKey", (), {})
    fake_mod.Ed25519Key = type("Ed25519Key", (), {})
    fake_mod.ECDSAKey = type("ECDSAKey", (), {})
    fake_mod.SSHException = Exception
    monkeypatch.setitem(sys.modules, "paramiko", fake_mod)


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "ssh_asset_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod(monkeypatch):
    # A throwaway fake is enough just to satisfy the module-level `import
    # paramiko` -- each test installs its own real fake client afterward.
    install_fake_paramiko(monkeypatch, lambda: FakeSSHClient({}))
    return load_component_module()
