"""Real gap reported against this registry (dagster-demo-factory component
feedback, 2026-10-30): both BigQuery and GCS clients were built inline inside
the `@asset`'s closure, with no seam a subclass could override -- the only
way to swap in a fake for tests/demos was to monkeypatch
`google.cloud.bigquery`/`google.cloud.storage` at the module level,
process-wide, before the component even loaded.

Fixed with two small instance methods, `_make_bigquery_client(creds, project,
location)` and `_make_storage_client(creds, project)`, that the asset body
calls instead of constructing `bigquery.Client`/`storage.Client` directly --
and a `demo_mode` field that bypasses the real GCP calls (and the
google-cloud-bigquery/storage imports) entirely.

These tests fake `google.cloud.bigquery` / `google.cloud.storage` /
`google.oauth2.service_account` via sys.modules (those packages aren't
installed in this environment -- component.py only imports them lazily,
inside the asset body, specifically so a demo/test run never needs them
installed at all), restoring the previous entries afterward so this doesn't
leak into other test files.
"""
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
BigQueryExportToGcsAssetComponent = mod.BigQueryExportToGcsAssetComponent

_FAKE_CREDS = {"project_id": "fake-project", "type": "service_account"}


class _FakeExtractJob:
    def result(self):
        return None


class _FakeTable:
    num_bytes = 2048


class _FakeBigQueryClient:
    def __init__(self, credentials=None, project=None, location=None):
        self.credentials, self.project, self.location = credentials, project, location
        self.extract_calls = []

    def extract_table(self, source_table_id, destination_uris, job_config):
        self.extract_calls.append((source_table_id, destination_uris))
        return _FakeExtractJob()

    def get_table(self, source_table_id):
        return _FakeTable()


class _FakeBucketRef:
    def __init__(self, name):
        self.name = name


class _FakeBlob:
    def __init__(self, bucket_name, name):
        self.bucket = _FakeBucketRef(bucket_name)
        self.name = name
        self.size = 4096
        self.content_type = "application/octet-stream"
        self.updated = None


class _FakeStorageClient:
    def __init__(self, credentials=None, project=None):
        self.credentials, self.project = credentials, project

    def list_blobs(self, bucket_name, prefix=None, max_results=None):
        return [_FakeBlob(bucket_name, f"{prefix}000000000000.parquet")]


@pytest.fixture
def fake_google_cloud_modules():
    """Install minimal fake google.cloud.bigquery / google.cloud.storage /
    google.oauth2.service_account modules, restoring whatever (if anything)
    was there before."""
    saved = {k: sys.modules.get(k) for k in (
        "google", "google.cloud", "google.cloud.bigquery", "google.cloud.storage",
        "google.oauth2", "google.oauth2.service_account",
    )}

    bigquery_mod = types.ModuleType("google.cloud.bigquery")
    bigquery_mod.Client = _FakeBigQueryClient
    bigquery_mod.ExtractJobConfig = lambda: types.SimpleNamespace(destination_format=None, compression=None)

    storage_mod = types.ModuleType("google.cloud.storage")
    storage_mod.Client = _FakeStorageClient

    service_account_mod = types.ModuleType("google.oauth2.service_account")
    service_account_mod.Credentials = types.SimpleNamespace(
        from_service_account_info=staticmethod(lambda info: info)
    )

    google_mod = types.ModuleType("google")
    cloud_mod = types.ModuleType("google.cloud")
    oauth2_mod = types.ModuleType("google.oauth2")
    google_mod.cloud = cloud_mod
    cloud_mod.bigquery = bigquery_mod
    cloud_mod.storage = storage_mod
    google_mod.oauth2 = oauth2_mod
    oauth2_mod.service_account = service_account_mod

    sys.modules["google"] = google_mod
    sys.modules["google.cloud"] = cloud_mod
    sys.modules["google.cloud.bigquery"] = bigquery_mod
    sys.modules["google.cloud.storage"] = storage_mod
    sys.modules["google.oauth2"] = oauth2_mod
    sys.modules["google.oauth2.service_account"] = service_account_mod

    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


def test_demo_mode_needs_no_credentials_or_google_cloud_packages():
    comp = BigQueryExportToGcsAssetComponent(
        asset_name="exports/orders_parquet",
        source_table_id="proj.ds.orders",
        destination_uri="gs://bucket/exports/orders_*.parquet",
        demo_mode=True,
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    output_value = result.asset_value(dg.AssetKey.from_user_string("exports/orders_parquet"))
    assert isinstance(output_value, pd.DataFrame)
    assert len(output_value) >= 1
    assert "uri" in output_value.columns


def test_client_seam_is_overridable_without_monkeypatching_google_cloud_modules(fake_google_cloud_modules):
    """The exact gap reported: a subclass overriding just these two methods
    should be enough to run the real (non-demo_mode) code path against a
    fake, with no process-wide monkeypatching of google.cloud.* needed."""

    class _FakeClientComponent(BigQueryExportToGcsAssetComponent):
        def _make_bigquery_client(self, credentials, project_id, location):
            return _FakeBigQueryClient(credentials, project_id, location)

        def _make_storage_client(self, credentials, project_id):
            return _FakeStorageClient(credentials, project_id)

    comp = _FakeClientComponent(
        asset_name="exports/orders_parquet",
        source_table_id="proj.ds.orders",
        destination_uri="gs://bucket/exports/orders_*.parquet",
        credentials=_FAKE_CREDS,
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    output_value = result.asset_value(dg.AssetKey.from_user_string("exports/orders_parquet"))
    assert list(output_value["uri"]) == ["gs://bucket/exports/orders_000000000000.parquet"]
