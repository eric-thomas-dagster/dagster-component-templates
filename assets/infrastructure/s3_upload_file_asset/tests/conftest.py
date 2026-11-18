"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("s3_upload_file_asset_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


class FakeS3Client:
    def __init__(self, existing_keys=None, upload_delay_seconds=0):
        self.existing_keys = set(existing_keys or [])
        self.upload_calls = []
        self.upload_delay_seconds = upload_delay_seconds

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        if Key in self.existing_keys:
            return {}
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")

    def upload_file(self, Filename, Bucket, Key, **kwargs):
        if self.upload_delay_seconds:
            import time
            time.sleep(self.upload_delay_seconds)
        self.upload_calls.append({"Filename": Filename, "Bucket": Bucket, "Key": Key, **kwargs})
