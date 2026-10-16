"""Committed tests for GCSToDatabaseAssetComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.
Same shape as adls_to_database_asset's own tests (see that file for the
full rationale): object_name AND table_name can both vary per partition
key, so a single_run backfill processes and writes each partition
independently rather than concatenating into one frame.

Mocks ONLY google.cloud.storage.Client (a fake client exposing
.bucket(name).blob(object_name) -> object with .download_as_bytes()) and
uses a real temp-file SQLite database for the write side.
"""
import sys
import types
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _install_fake_google_cloud_storage_module():
    """The real `google-cloud-storage` package isn't installed in this test
    environment -- inject a minimal fake `google.cloud.storage` module into
    sys.modules so the component's lazy `from google.cloud import storage`
    resolves to our fake instead of failing on import."""
    google_mod = sys.modules.setdefault("google", types.ModuleType("google"))
    cloud_mod = sys.modules.setdefault("google.cloud", types.ModuleType("google.cloud"))
    storage_mod = sys.modules.setdefault("google.cloud.storage", types.ModuleType("google.cloud.storage"))
    google_mod.cloud = cloud_mod
    cloud_mod.storage = storage_mod
    if not hasattr(storage_mod, "Client"):
        storage_mod.Client = MagicMock()


class FakeGCSClient:
    """Maps "bucket/object" to canned CSV bytes; records every .blob() call."""

    def __init__(self, files: dict):
        self.files = files
        self.calls = []

    def bucket(self, bucket_name):
        return _FakeBucket(self, bucket_name)


class _FakeBucket:
    def __init__(self, client, bucket_name):
        self._client = client
        self._bucket_name = bucket_name

    def blob(self, object_name):
        self._client.calls.append((self._bucket_name, object_name))
        key = f"{self._bucket_name}/{object_name}"
        content = self._client.files[key]
        blob_obj = MagicMock()
        blob_obj.download_as_bytes.return_value = content.encode()
        return blob_obj


def _make_component(mod, db_path, **overrides):
    kwargs = dict(
        asset_name="gcs_sales",
        database_url=f"sqlite:///{db_path}",
        table_name="sales",
        if_exists="append",
    )
    kwargs.update(overrides)
    return mod.GCSToDatabaseAssetComponent(**kwargs)


def _materialize(component, fake_client, bucket, object_name, partition_key=None, tags=None):
    _install_fake_google_cloud_storage_module()
    asset_def = component.build_defs(context=None).assets[0]
    with patch("google.cloud.storage.Client", return_value=fake_client):
        return dg.materialize(
            [asset_def],
            run_config={"ops": {component.asset_name: {"config": {"bucket_name": bucket, "object_name": object_name}}}},
            partition_key=partition_key,
            tags=tags or {},
        )


def test_single_partition_reads_one_object_and_writes_it(mod, tmp_path):
    db_path = tmp_path / "test.db"
    fake = FakeGCSClient({"my-bucket/sales_store_a.csv": "id,total\n1,10.0\n2,20.0\n"})
    component = _make_component(
        mod, db_path, partition_type="static", partition_values="store_a,store_b,store_c",
    )
    result = _materialize(component, fake, "my-bucket", "sales_{partition_key}.csv", partition_key="store_a")
    assert result.success
    assert len(fake.calls) == 1

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        rows = conn.execute(text("select count(*) from sales")).scalar()
    assert rows == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, tmp_path):
    component = _make_component(
        mod, tmp_path / "test.db",
        partition_type="static", partition_values="store_a,store_b,store_c",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, tmp_path):
    component = _make_component(
        mod, tmp_path / "test.db",
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_writes_each_partitions_own_table(mod, tmp_path):
    """Real bug before the fix: the asset read exactly ONE GCS object
    (templated from a single context.partition_key) per call, which
    doesn't exist for a multi-partition single_run backfill. Confirmed
    fixed: a 3-key single_run backfill now reads each store's own object
    and writes it to that store's OWN table (table_name also templated by
    partition_key) -- not concatenated into one shared table, since the
    destination itself varies per key here."""
    db_path = tmp_path / "test.db"
    fake = FakeGCSClient({
        "my-bucket/sales_store_a.csv": "id,total\n1,10.0\n2,20.0\n",
        "my-bucket/sales_store_b.csv": "id,total\n3,30.0\n",
        "my-bucket/sales_store_c.csv": "id,total\n4,40.0\n5,50.0\n6,60.0\n",
    })
    component = _make_component(
        mod, db_path,
        partition_type="static", partition_values="store_a,store_b,store_c",
        table_name="sales_{partition_key}",
        backfill_policy="single_run",
    )
    result = _materialize(
        component, fake, "my-bucket", "sales_{partition_key}.csv",
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
