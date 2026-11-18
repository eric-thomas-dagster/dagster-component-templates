"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("s3_cleanup_job_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


class FakeS3Client:
    """Stands in for the real boto3 S3 client -- list_objects_v2 (paginated) + delete_objects."""

    def __init__(self, objects):
        self.objects = objects  # [{"Key": ..., "LastModified": ...}]
        self.delete_calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        matching = [o for o in self.objects if o["Key"].startswith(Prefix)]
        yield {"Contents": matching}

    def delete_objects(self, Bucket, Delete):
        self.delete_calls.append(Delete["Objects"])
