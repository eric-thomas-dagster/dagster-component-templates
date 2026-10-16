"""Loads component.py by file path so tests don't depend on package install."""
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

COMMITS: list = []


class _LocalFS:
    """Stands in for the s3fs filesystem (no lakeFS server here): maps 'repo/branch/key' paths to a tmp dir.
    Real parquet is written/read by the manager through these file handles."""

    def __init__(self, root: Path):
        self.root = root

    def open(self, path, mode):
        p = self.root / path
        if "w" in mode:
            p.parent.mkdir(parents=True, exist_ok=True)
        return open(p, mode)


def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_lakefs_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class Backend:
    PREFIX = "repo/main/dagster/assets"

    def __init__(self, mod, root, monkeypatch, commit=False):
        monkeypatch.setattr(mod.LakeFSIOManager, "_get_fs", lambda self_: _LocalFS(root))
        self.root = root
        self.manager = mod.LakeFSIOManager(
            repository="repo", endpoint_url="http://lakefs:8000", access_key="k", secret_key="s",
            commit_per_materialization=commit,
        )

    def _path(self, asset, key):
        rel = f"{asset}.parquet" if key is None else f"{asset}/{key}.parquet"
        return self.root / self.PREFIX / rel

    def has(self, asset, key):
        return self._path(asset, key).exists()

    def read(self, asset, key):
        return pd.read_parquet(self._path(asset, key))


def _install_fake_lakefs(monkeypatch):
    COMMITS.clear()

    class _Branch:
        def commit(self, message):
            COMMITS.append(message)
            return types.SimpleNamespace(id=f"c{len(COMMITS)}")

    class _Repo:
        def __init__(self, name, client=None):
            pass

        def branch(self, name):
            return _Branch()

    lakefs = types.ModuleType("lakefs")
    lakefs.Repository = _Repo
    client = types.ModuleType("lakefs.client")
    client.Client = lambda **kw: object()
    lakefs.client = client
    monkeypatch.setitem(sys.modules, "lakefs", lakefs)
    monkeypatch.setitem(sys.modules, "lakefs.client", client)


@pytest.fixture
def be(mod, tmp_path, monkeypatch):
    return Backend(mod, tmp_path, monkeypatch)


@pytest.fixture
def be_commit(mod, tmp_path, monkeypatch):
    _install_fake_lakefs(monkeypatch)
    return Backend(mod, tmp_path, monkeypatch, commit=True)
