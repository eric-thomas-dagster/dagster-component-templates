"""Real, committed tests for the one-asset-per-instance gap reported for
ExternalSftpPathAsset: `paths:` lets one component instance declare several
remote directories sharing one host/credentials and (with
create_observation_sensor=true) one sensor/SSH connection instead of one per
path.

Also covers a real, separately-confirmed crash bug found while writing these
tests: the resource-backed sensor path required a resource (via
required_resource_keys), but the sensor function never declared **_resources
to receive it -- Dagster injects the resource as a kwarg matching its key, so
the ORIGINAL single-path sensor crashed with a TypeError the moment
create_observation_sensor=true + resource_key were used together (same class
of bug already fixed in ExternalKafkaAsset's sibling sensor this session).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("esftp_multi", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalSftpPathAsset = _mod.ExternalSftpPathAsset


def _specs_by_key(defs: dg.Definitions):
    return {s.key: s for s in defs.assets}


def test_single_path_mode_still_works_unchanged():
    comp = ExternalSftpPathAsset(asset_key="files/orders", host="sftp.example.com", remote_path="/orders")
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 1
    assert dg.AssetKey(["files", "orders"]) in specs


def test_paths_list_declares_several_assets_from_one_instance():
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[
            {"key": "files/orders", "remote_path": "/orders"},
            {"key": "files/invoices", "remote_path": "/invoices", "owners": ["team:finance"]},
        ],
    )
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 2
    assert specs[dg.AssetKey(["files", "invoices"])].owners == ["team:finance"]


def test_both_shapes_set_is_rejected():
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        asset_key="files/orders", remote_path="/orders",
        paths=[{"key": "files/invoices", "remote_path": "/invoices"}],
    )
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_multi_path_sensor_requires_explicit_sensor_name():
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[{"key": "files/orders", "remote_path": "/orders"}],
        create_observation_sensor=True,
    )
    with pytest.raises(ValueError, match="sensor_name is required"):
        comp.build_defs(None)


class _FakeSftpClient:
    def __init__(self, data_versions):
        self.data_versions = data_versions
        self.calls = []

    def observe(self, source):
        self.calls.append(source)
        return {"data_version": self.data_versions[source]}


def test_shared_sensor_observes_every_path_over_one_resource_client():
    client = _FakeSftpClient({
        "sftp.example.com:/orders": "1",
        "sftp.example.com:/invoices": "2",
    })
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[
            {"key": "files/orders", "remote_path": "/orders"},
            {"key": "files/invoices", "remote_path": "/invoices"},
        ],
        create_observation_sensor=True,
        sensor_name="sftp_multi_sensor",
        resource_key="sftp_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("sftp_multi_sensor")
    context = dg.build_sensor_context(resources={"sftp_client": client})
    result = sensor_def(context)

    assert client.calls == ["sftp.example.com:/orders", "sftp.example.com:/invoices"]
    assert len(result.asset_events) == 2


def test_single_path_resource_backed_sensor_does_not_crash_on_invocation():
    """This is the exact crash this test file's docstring describes -- the
    ORIGINAL code (before **_resources was added) raised a TypeError here."""
    client = _FakeSftpClient({"sftp.example.com:/orders": "1"})
    comp = ExternalSftpPathAsset(
        asset_key="files/orders", host="sftp.example.com", remote_path="/orders",
        create_observation_sensor=True,
        resource_key="sftp_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("files_orders__observation_sensor")
    context = dg.build_sensor_context(resources={"sftp_client": client})
    result = sensor_def(context)
    assert len(result.asset_events) == 1
