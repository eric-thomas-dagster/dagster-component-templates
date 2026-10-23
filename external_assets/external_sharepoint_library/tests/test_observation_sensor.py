"""Real, committed tests for a gap an external demo-builder hit: unlike its
sql/s3/sftp/kafka ExternalXAsset siblings, ExternalSharePointLibraryAsset had
no `create_observation_sensor`/`resource_key`/`emit_materialization` trio at
all -- it was declare-only with no way to keep the asset's health/data-version
current.

Added the same trio, with the resource-backed path (preferred) plus a native
Microsoft Graph path reusing the OAuth client-credentials flow already proven
in the standalone `sharepoint_monitor` sensor component (adapted to emit
AssetMaterialization/AssetObservation instead of RunRequest).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("esp_obs", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalSharePointLibraryAsset = _mod.ExternalSharePointLibraryAsset


def test_no_sensor_by_default():
    comp = ExternalSharePointLibraryAsset(
        asset_key="docs/reports", site_url="https://myorg.sharepoint.com/sites/Data", library_name="Reports",
    )
    defs = comp.build_defs(None)
    assert list(defs.assets) and not list(defs.sensors or [])


class _FakeSharePointClient:
    def __init__(self, data_version):
        self.data_version = data_version
        self.calls = []

    def observe(self, source):
        self.calls.append(source)
        return {"data_version": self.data_version, "file_count": 7}


def test_resource_backed_sensor_emits_materialization():
    client = _FakeSharePointClient("abc123")
    comp = ExternalSharePointLibraryAsset(
        asset_key="docs/reports", site_url="https://myorg.sharepoint.com/sites/Data", library_name="Reports",
        create_observation_sensor=True,
        resource_key="sharepoint_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("docs_reports__observation_sensor")
    context = dg.build_sensor_context(resources={"sharepoint_client": client})
    result = sensor_def(context)

    assert client.calls == ["https://myorg.sharepoint.com/sites/Data/Reports"]
    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert isinstance(event, dg.AssetMaterialization)
    assert event.asset_key == dg.AssetKey(["docs", "reports"])
    assert event.metadata["file_count"].value == 7


def test_emit_materialization_false_emits_observation_instead():
    client = _FakeSharePointClient("abc123")
    comp = ExternalSharePointLibraryAsset(
        asset_key="docs/reports", site_url="https://myorg.sharepoint.com/sites/Data", library_name="Reports",
        create_observation_sensor=True,
        resource_key="sharepoint_client",
        emit_materialization=False,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("docs_reports__observation_sensor")
    context = dg.build_sensor_context(resources={"sharepoint_client": client})
    result = sensor_def(context)
    assert isinstance(result.asset_events[0], dg.AssetObservation)


def test_native_path_without_credentials_skips_cleanly():
    comp = ExternalSharePointLibraryAsset(
        asset_key="docs/reports", site_url="https://myorg.sharepoint.com/sites/Data", library_name="Reports",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("docs_reports__observation_sensor")
    context = dg.build_sensor_context()
    result = sensor_def(context)
    assert result.skip_reason is not None
    assert "tenant_id_env_var" in str(result.skip_reason)
