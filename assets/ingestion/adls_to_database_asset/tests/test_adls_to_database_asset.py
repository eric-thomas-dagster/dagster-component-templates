"""Committed tests for ADLSToDatabaseAssetComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

Unlike s3_to_database_asset (one fixed destination table, concatenate all
days and write once), this component's blob_name AND table_name can BOTH
vary per partition key -- real-world use looks like a per-store/per-tenant
static partition writing to its own table, not just a date range. So a
single_run backfill here processes and writes each partition independently
rather than concatenating into one frame; the real regression this guards
is writing to the WRONG (or a single shared) table when multiple distinct
destination tables are involved.

Mocks ONLY azure.storage.blob.BlobServiceClient (a fake client exposing
.get_blob_client(container, blob) -> object with .download_blob().readall())
and uses a real temp-file SQLite database for the write side.
"""
import sys
import types
from io import BytesIO
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module


def _install_fake_azure_module(monkeypatch):
    """The real `azure-storage-blob` package isn't installed in this test
    environment (and shouldn't need to be, just to test our own partition-
    range handling) -- inject a minimal fake `azure.storage.blob` module
    into sys.modules so the component's lazy `from azure.storage.blob
    import BlobServiceClient` resolves to our fake instead of failing on
    import. Uses monkeypatch.setitem/setattr (not a bare assignment/
    setdefault) so every change is undone after this test -- a bare
    `sys.modules["azure"] = types.ModuleType(...)` would otherwise
    permanently shadow a real `azure` package for the rest of the test
    session if one is ever installed alongside this repo's other cloud
    SDKs (confirmed as a real cross-test failure mode for the equivalent
    `google` stub in gcs_to_database_asset's tests)."""
    if "azure" in sys.modules:
        azure_mod = sys.modules["azure"]
    else:
        azure_mod = types.ModuleType("azure")
        monkeypatch.setitem(sys.modules, "azure", azure_mod)

    if "azure.storage" in sys.modules:
        storage_mod = sys.modules["azure.storage"]
    else:
        storage_mod = types.ModuleType("azure.storage")
        monkeypatch.setitem(sys.modules, "azure.storage", storage_mod)
        monkeypatch.setattr(azure_mod, "storage", storage_mod, raising=False)

    if "azure.storage.blob" in sys.modules and hasattr(sys.modules["azure.storage.blob"], "BlobServiceClient"):
        blob_mod = sys.modules["azure.storage.blob"]
    else:
        blob_mod = types.ModuleType("azure.storage.blob")
        blob_mod.BlobServiceClient = MagicMock()
        monkeypatch.setitem(sys.modules, "azure.storage.blob", blob_mod)
        monkeypatch.setattr(storage_mod, "blob", blob_mod, raising=False)


@pytest.fixture()
def mod():
    return load_component_module()


class FakeBlobServiceClient:
    """Maps "container/blob" to canned CSV bytes; records every get_blob_client call."""

    def __init__(self, files: dict):
        self.files = files
        self.calls = []

    def get_blob_client(self, container, blob):
        self.calls.append((container, blob))
        key = f"{container}/{blob}"
        if key not in self.files:
            raise KeyError(f"no such fake blob: {key}")
        content = self.files[key]
        blob_obj = MagicMock()
        blob_obj.download_blob.return_value.readall.return_value = content.encode()
        return blob_obj


def _make_component(mod, db_path, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_ADLS_CONN_STR", "fake-connection-string")
    kwargs = dict(
        asset_name="adls_sales",
        connection_string_env_var="FAKE_ADLS_CONN_STR",
        database_url=f"sqlite:///{db_path}",
        table_name="sales",
        if_exists="append",
    )
    kwargs.update(overrides)
    return mod.ADLSToDatabaseAssetComponent(**kwargs)


def _materialize(component, fake_client, container, blob_name, monkeypatch, partition_key=None, tags=None):
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake_client):
        return dg.materialize(
            [asset_def],
            run_config={"ops": {component.asset_name: {"config": {"container_name": container, "blob_name": blob_name}}}},
            partition_key=partition_key,
            tags=tags or {},
        )


def test_single_partition_reads_one_blob_and_writes_it(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({"my-container/sales_store_a.csv": "id,total\n1,10.0\n2,20.0\n"})
    component = _make_component(
        mod, db_path, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    result = _materialize(component, fake, "my-container", "sales_{partition_key}.csv", monkeypatch, partition_key="store_a")
    assert result.success
    assert len(fake.calls) == 1

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        rows = conn.execute(text("select count(*) from sales")).scalar()
    assert rows == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, tmp_path, monkeypatch):
    component = _make_component(
        mod, tmp_path / "test.db", monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, tmp_path, monkeypatch):
    component = _make_component(
        mod, tmp_path / "test.db", monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_writes_each_partitions_own_table(mod, tmp_path, monkeypatch):
    """Real bug before the fix: the asset read exactly ONE blob (templated
    from a single context.partition_key) per call, which doesn't exist for
    a multi-partition single_run backfill. Confirmed fixed: a 3-key
    single_run backfill now reads each store's own blob and writes it to
    that store's OWN table (table_name also templated by partition_key) --
    not concatenated into one shared table, since the destination itself
    varies per key here."""
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({
        "my-container/sales_store_a.csv": "id,total\n1,10.0\n2,20.0\n",
        "my-container/sales_store_b.csv": "id,total\n3,30.0\n",
        "my-container/sales_store_c.csv": "id,total\n4,40.0\n5,50.0\n6,60.0\n",
    })
    component = _make_component(
        mod, db_path, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        table_name="sales_{partition_key}",
        backfill_policy="single_run",
    )
    result = _materialize(
        component, fake, "my-container", "sales_{partition_key}.csv", monkeypatch,
        tags={
            "dagster/asset_partition_range_start": "store_a",
            "dagster/asset_partition_range_end": "store_c",
        },
    )
    assert result.success
    assert len(fake.calls) == 3

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text("select count(*) from sales_store_a")).scalar() == 2
        assert conn.execute(text("select count(*) from sales_store_b")).scalar() == 1
        assert conn.execute(text("select count(*) from sales_store_c")).scalar() == 3
