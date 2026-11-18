"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("s3_create_object_asset_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


def materialize(component, assets_extra=None, **kwargs):
    asset_def = component.build_defs(context=None).assets[0]
    assets = ([*assets_extra, asset_def] if assets_extra else [asset_def])
    return dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, **kwargs)


class FakeS3Client:
    def __init__(self, existing_keys=None, put_delay_seconds=0):
        self.existing_keys = set(existing_keys or [])
        self.put_calls = []
        self.put_delay_seconds = put_delay_seconds

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        if Key in self.existing_keys:
            return {}
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")

    def put_object(self, **kwargs):
        if self.put_delay_seconds:
            import time
            time.sleep(self.put_delay_seconds)
        self.put_calls.append(kwargs)
