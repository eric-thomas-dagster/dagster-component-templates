"""Loads component.py by file path. The azure SDK is an optional import (component.py falls back to None); only the network client is faked, so no SDK is needed."""
import importlib.util
import sys
import types
from pathlib import Path

import pytest
import io
import pandas as pd

def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_azure_blob_parquet_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class _Blob:
    def __init__(self, store, key):
        self.store, self.key = store, key

    def upload_blob(self, data, overwrite=False):
        self.store[self.key] = bytes(data)

    def download_blob(self):
        data = self.store[self.key]
        return types.SimpleNamespace(readall=lambda: data)


class FakeBlobServiceClient:
    """In-memory stand-in for the network client only."""
    store = {}

    def __init__(self, *a, **k):
        pass

    @classmethod
    def from_connection_string(cls, cs):
        return cls()

    def get_blob_client(self, container, blob):
        return _Blob(self.store, f"{container}/{blob}")


@pytest.fixture
def store(mod, monkeypatch):
    FakeBlobServiceClient.store = {}
    monkeypatch.setattr(mod, "BlobServiceClient", FakeBlobServiceClient)
    return FakeBlobServiceClient.store


@pytest.fixture
def manager(mod):
    return mod.AzureBlobParquetIOManager(account_name="acct", container="box", prefix="p", connection_string="x")


def full_key(rel):
    return f"box/p/{rel}.parquet"


def decode(raw):
    return pd.read_parquet(io.BytesIO(raw))
