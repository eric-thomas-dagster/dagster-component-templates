"""Loads component.py by file path so tests don't depend on package install."""
import importlib.util
import sys
import types
from pathlib import Path

import pytest
import io
import pandas as pd

def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_s3_parquet_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class _Writer(io.BytesIO):
    def __init__(self, store, path):
        super().__init__()
        self._store, self._path = store, path

    def close(self):
        if not self.closed:
            self._store[self._path] = self.getvalue()
        super().close()


class FakeFS:
    """In-memory stand-in for the network filesystem only (open() by 'bucket/key' path)."""
    store = {}

    def __init__(self, *a, **k):
        pass

    def open(self, path, mode="rb"):
        if "w" in mode:
            return _Writer(self.store, path)
        return io.BytesIO(self.store[path])


@pytest.fixture
def store(monkeypatch):
    FakeFS.store = {}
    fake = types.ModuleType("s3fs")
    fake.S3FileSystem = FakeFS
    monkeypatch.setitem(sys.modules, "s3fs", fake)
    return FakeFS.store


@pytest.fixture
def manager(mod):
    return mod.S3ParquetIOManager(bucket="bkt", prefix="p")


def full_key(rel):
    return f"bkt/p/{rel}.parquet"


def decode(raw):
    return pd.read_parquet(io.BytesIO(raw))
