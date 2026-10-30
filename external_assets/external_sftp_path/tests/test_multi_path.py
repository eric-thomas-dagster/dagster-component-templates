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


def test_sensor_skips_unchanged_paths_on_next_tick():
    """Real gap found via review feedback: the sensor used to re-materialize
    every path on every poll regardless of whether anything changed, so a
    downstream AutomationCondition.eager() (which reacts to "parent updated,"
    not to the data_version value) re-fired on every single poll. Fixed via a
    cursor of {asset_key: last_data_version} -- a path whose data_version is
    unchanged since the last tick is no longer re-emitted."""
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

    first = sensor_def(dg.build_sensor_context(resources={"sftp_client": client}))
    assert len(first.asset_events) == 2  # first tick: nothing known yet, both emit

    # Second tick, nothing changed on the source -- same data_versions.
    second = sensor_def(dg.build_sensor_context(resources={"sftp_client": client}, cursor=first.cursor))
    assert second.asset_events == []
    assert second.cursor == first.cursor

    # Third tick, only /orders changed -- only that path should re-emit.
    client.data_versions["sftp.example.com:/orders"] = "3"
    third = sensor_def(dg.build_sensor_context(resources={"sftp_client": client}, cursor=second.cursor))
    assert len(third.asset_events) == 1
    assert third.asset_events[0].asset_key == dg.AssetKey(["files", "orders"])


class _FakeSftpClientWithFileCount:
    def __init__(self, file_count):
        self.file_count = file_count

    def observe(self, source):
        return {"data_version": "1", "file_count": self.file_count}


def test_check_arrival_builds_one_asset_check_per_path_using_the_same_client():
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[{"key": "files/orders", "remote_path": "/orders"}],
        resource_key="sftp_client",
        check_arrival=True,
    )
    defs = comp.build_defs(None)
    check_defs = list(defs.asset_checks)
    assert len(check_defs) == 1
    assert check_defs[0].check_key.name == "arrival"

    client = _FakeSftpClientWithFileCount(file_count=3)
    context = dg.build_asset_check_context(resources={"sftp_client": client})
    result = check_defs[0](context)
    assert result.passed is True
    assert result.metadata["file_count"].value == 3

    client.file_count = 0
    context2 = dg.build_asset_check_context(resources={"sftp_client": client})
    result2 = check_defs[0](context2)
    assert result2.passed is False


class _FakeSftpClientWithRowCount:
    def __init__(self, row_count, expected_row_count, decrypt_ok=True):
        self.row_count = row_count
        self.expected_row_count = expected_row_count
        self.decrypt_ok = decrypt_ok

    def observe(self, source):
        return {
            "data_version": "1",
            "row_count": self.row_count,
            "expected_row_count": self.expected_row_count,
            "decrypt_ok": self.decrypt_ok,
        }


def test_row_count_tolerance_passes_within_tolerance_and_fails_outside_it():
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[{"key": "files/claims", "remote_path": "/claims"}],
        resource_key="sftp_client",
        check_arrival=True,
        row_count_tolerance_pct=2.0,
    )
    check_def = list(comp.build_defs(None).asset_checks)[0]

    # 1% off -- within a 2% tolerance.
    client = _FakeSftpClientWithRowCount(row_count=990, expected_row_count=1000)
    result = check_def(dg.build_asset_check_context(resources={"sftp_client": client}))
    assert result.passed is True
    assert result.metadata["row_count_diff_pct"].value == pytest.approx(1.0)

    # 10% off -- outside a 2% tolerance.
    client.row_count = 900
    result2 = check_def(dg.build_asset_check_context(resources={"sftp_client": client}))
    assert result2.passed is False


def test_decrypt_ok_false_fails_the_check_even_within_row_count_tolerance():
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[{"key": "files/claims", "remote_path": "/claims"}],
        resource_key="sftp_client",
        check_arrival=True,
        row_count_tolerance_pct=2.0,
    )
    check_def = list(comp.build_defs(None).asset_checks)[0]

    client = _FakeSftpClientWithRowCount(row_count=1000, expected_row_count=1000, decrypt_ok=False)
    result = check_def(dg.build_asset_check_context(resources={"sftp_client": client}))
    assert result.passed is False
    assert "decrypt" in result.description.lower()


def test_row_count_tolerance_has_no_effect_without_both_keys_present():
    """The paramiko fallback (and simpler resource clients) never set
    row_count/expected_row_count -- the check must fall back to the plain
    file_count presence check rather than silently passing/failing."""
    comp = ExternalSftpPathAsset(
        host="sftp.example.com",
        paths=[{"key": "files/claims", "remote_path": "/claims"}],
        resource_key="sftp_client",
        check_arrival=True,
        row_count_tolerance_pct=2.0,
    )
    check_def = list(comp.build_defs(None).asset_checks)[0]
    client = _FakeSftpClientWithFileCount(file_count=5)
    result = check_def(dg.build_asset_check_context(resources={"sftp_client": client}))
    assert result.passed is True
    assert result.metadata["file_count"].value == 5
