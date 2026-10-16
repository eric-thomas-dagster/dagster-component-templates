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
    spec = importlib.util.spec_from_file_location("_s3_csv_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class FakeS3:
    store = {}

    def put_object(self, Bucket, Key, Body):
        self.store[f"{Bucket}/{Key}"] = bytes(Body)

    def get_object(self, Bucket, Key):
        data = self.store[f"{Bucket}/{Key}"]
        return {"Body": types.SimpleNamespace(read=lambda: data)}


@pytest.fixture
def store(mod, monkeypatch):
    FakeS3.store = {}
    monkeypatch.setattr(mod.boto3, "client", lambda *a, **k: FakeS3())
    return FakeS3.store


@pytest.fixture
def manager(mod):
    return mod.S3CsvIOManager(bucket="bkt", prefix="p")


def full_key(rel):
    return f"bkt/p/{rel}.csv"


def decode(raw):
    return pd.read_csv(io.StringIO(raw.decode()))
