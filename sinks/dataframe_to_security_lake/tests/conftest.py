"""Shared test helpers for DataframeToSecurityLakeComponent.

Loads component.py via importlib. Only the external AWS service boundary is
faked: the S3 client (`upload_fileobj` records the real parquet bytes the
component produced) -- everything else (OCSF eventDay bucketing, parquet
encoding, key layout, resource dispatch) runs for real under dg.materialize().
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataframe_to_security_lake_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeS3Client:
    def __init__(self, label="client"):
        self.label = label
        self.uploads = []  # (bucket, key, parquet_bytes)

    def upload_fileobj(self, fileobj, bucket, key):
        self.uploads.append((bucket, key, fileobj.read()))


class FakeS3Resource:
    """Stands in for the registered dagster-aws S3Resource (exposes .get_client())."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client
