"""Real, committed tests for the one-asset-per-instance gap reported (for the
ExternalKafkaAsset/ExternalSqlAsset/ExternalSftpPathAsset siblings, and
independently confirmed here too): `sources:` lets one component instance
declare several S3 bucket/prefix sources sharing one region_name and (with
create_observation_sensor=true) one sensor instead of one per source.

Also confirms the same resource-kwarg crash bug found in
ExternalSqlAsset/ExternalSftpPathAsset doesn't exist here anymore --
**_resources was missing from the sensor signature despite
required_resource_keys being used.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("es3_multi", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalS3Asset = _mod.ExternalS3Asset


def _specs_by_key(defs: dg.Definitions):
    return {s.key: s for s in defs.assets}


def test_single_source_mode_still_works_unchanged():
    comp = ExternalS3Asset(asset_key="lake/orders", bucket_name="my-bucket", prefix="orders/")
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 1
    assert dg.AssetKey(["lake", "orders"]) in specs


def test_sources_list_declares_several_assets_from_one_instance():
    comp = ExternalS3Asset(
        sources=[
            {"key": "lake/orders", "bucket_name": "my-bucket", "prefix": "orders/"},
            {"key": "lake/invoices", "bucket_name": "my-bucket", "prefix": "invoices/", "owners": ["team:finance"]},
        ],
    )
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 2
    assert specs[dg.AssetKey(["lake", "invoices"])].owners == ["team:finance"]


def test_both_shapes_set_is_rejected():
    comp = ExternalS3Asset(
        asset_key="lake/orders", bucket_name="my-bucket",
        sources=[{"key": "lake/invoices", "bucket_name": "my-bucket"}],
    )
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_multi_source_sensor_requires_explicit_sensor_name():
    comp = ExternalS3Asset(
        sources=[{"key": "lake/orders", "bucket_name": "my-bucket"}],
        create_observation_sensor=True,
    )
    with pytest.raises(ValueError, match="sensor_name is required"):
        comp.build_defs(None)


class _FakeS3Client:
    def __init__(self, data_versions):
        self.data_versions = data_versions
        self.calls = []

    def observe(self, source):
        self.calls.append(source)
        return {"data_version": self.data_versions[source]}


def test_shared_sensor_observes_every_source_over_one_resource_client():
    client = _FakeS3Client({"my-bucket/orders/": "1", "my-bucket/invoices/": "2"})
    comp = ExternalS3Asset(
        sources=[
            {"key": "lake/orders", "bucket_name": "my-bucket", "prefix": "orders/"},
            {"key": "lake/invoices", "bucket_name": "my-bucket", "prefix": "invoices/"},
        ],
        create_observation_sensor=True,
        sensor_name="s3_multi_sensor",
        resource_key="s3_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("s3_multi_sensor")
    context = dg.build_sensor_context(resources={"s3_client": client})
    result = sensor_def(context)

    assert client.calls == ["my-bucket/orders/", "my-bucket/invoices/"]
    assert len(result.asset_events) == 2


def test_single_source_resource_backed_sensor_does_not_crash_on_invocation():
    client = _FakeS3Client({"my-bucket/orders/": "1"})
    comp = ExternalS3Asset(
        asset_key="lake/orders", bucket_name="my-bucket", prefix="orders/",
        create_observation_sensor=True,
        resource_key="s3_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("my_bucket__observation_sensor")
    context = dg.build_sensor_context(resources={"s3_client": client})
    result = sensor_def(context)
    assert len(result.asset_events) == 1
