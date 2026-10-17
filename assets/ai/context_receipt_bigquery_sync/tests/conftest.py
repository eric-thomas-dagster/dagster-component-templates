"""Loads the real component.py and the REAL ContextReceiptStore by path; BigQuery is a DuckDB-backed fake that executes the real SQL."""
import importlib.util
import pathlib
import sys
from types import ModuleType

import pytest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # bq_fake


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod  # dataclasses in the store module need to be importable by name
    spec.loader.exec_module(mod)
    return mod


def load_component_module() -> ModuleType:
    return _load(HERE.parent / "component.py", "context_receipt_bigquery_sync_under_test")


def load_store_module() -> ModuleType:
    return _load(HERE.parents[3] / "resources" / "context_receipt_store" / "component.py", "context_receipt_store_for_sync_tests")


@pytest.fixture
def comp():
    return load_component_module()


@pytest.fixture
def store_mod():
    return load_store_module()


@pytest.fixture
def store(store_mod, tmp_path):
    return store_mod.ContextReceiptStore(str(tmp_path / "receipts.db"))


@pytest.fixture
def bq(monkeypatch):
    from bq_fake import FakeBigQueryClient, install_fake_bigquery
    client = FakeBigQueryClient(project="proj")
    client.create_dataset("proj.ctx", exists_ok=True)
    module = install_fake_bigquery(client)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", module)
    return client, module
