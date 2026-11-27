"""Real, committed tests for DataprocClusterAssetComponent.

Mocks only the external call: a fake ClusterControllerClient whose
get_cluster/create_cluster/delete_cluster responses match the real
google-cloud-dataproc v5 shapes (verified directly against the installed
`google.cloud.dataproc_v1` package -- see component.py's module docstring).
Everything else (idempotency logic, config-building, Dagster asset wiring)
is real.
"""
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


def _materialization_metadata(result):
    event = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION")
    return event.event_specific_data.materialization.metadata


class _FakeOperation:
    def __init__(self, result):
        self._result = result

    def result(self, timeout=None):
        return self._result


class FakeClusterClient:
    """`existing` is the Cluster-like object get_cluster returns, or None for NotFound."""

    def __init__(self, existing=None):
        self.existing = existing
        self.calls = []

    def get_cluster(self, project_id, region, cluster_name):
        self.calls.append(("get_cluster", {"project_id": project_id, "region": region, "cluster_name": cluster_name}))
        if self.existing is None:
            from google.api_core.exceptions import NotFound
            raise NotFound(f"cluster {cluster_name!r} not found")
        return self.existing

    def create_cluster(self, project_id, region, cluster):
        self.calls.append(("create_cluster", {"project_id": project_id, "region": region, "cluster": cluster}))
        created = SimpleNamespace(status=SimpleNamespace(state=SimpleNamespace(name="RUNNING")))
        self.existing = created
        return _FakeOperation(created)

    def delete_cluster(self, project_id, region, cluster_name):
        self.calls.append(("delete_cluster", {"project_id": project_id, "region": region, "cluster_name": cluster_name}))
        self.existing = None
        return _FakeOperation(None)


def _existing_cluster(state="RUNNING"):
    return SimpleNamespace(status=SimpleNamespace(state=SimpleNamespace(name=state)))


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_cluster_client", lambda region, gcp_credentials_env_var: client)


def _comp(**kw):
    args = dict(asset_name="out", project_id="my-proj", region="us-central1", cluster_name="my-cluster")
    args.update(kw)
    return mod.DataprocClusterAssetComponent(**args)


def test_absent_cluster_desired_present_creates(monkeypatch):
    client = FakeClusterClient(existing=None)
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="present")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    call_names = [c[0] for c in client.calls]
    assert call_names == ["get_cluster", "create_cluster"]
    meta = _materialization_metadata(result)
    assert meta["action"].value == "created"
    assert meta["cluster_status"].value == "RUNNING"


def test_present_cluster_desired_present_is_noop(monkeypatch):
    client = FakeClusterClient(existing=_existing_cluster())
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="present")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    call_names = [c[0] for c in client.calls]
    assert call_names == ["get_cluster"]
    meta = _materialization_metadata(result)
    assert meta["action"].value == "none"


def test_present_cluster_desired_absent_deletes(monkeypatch):
    client = FakeClusterClient(existing=_existing_cluster())
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="absent")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    call_names = [c[0] for c in client.calls]
    assert call_names == ["get_cluster", "delete_cluster"]
    meta = _materialization_metadata(result)
    assert meta["action"].value == "deleted"


def test_absent_cluster_desired_absent_is_noop(monkeypatch):
    client = FakeClusterClient(existing=None)
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="absent")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    call_names = [c[0] for c in client.calls]
    assert call_names == ["get_cluster"]
    meta = _materialization_metadata(result)
    assert meta["action"].value == "none"


def test_invalid_desired_state_raises(monkeypatch):
    client = FakeClusterClient(existing=None)
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="bogus")
    with pytest.raises(ValueError, match="desired_state"):
        comp.build_defs(None)


def test_create_cluster_config_shape(monkeypatch):
    """num_workers/machine types/idle_delete_ttl_seconds land in the right ClusterConfig sub-fields."""
    client = FakeClusterClient(existing=None)
    _patch_client(monkeypatch, client)
    comp = _comp(
        desired_state="present",
        master_machine_type="n2-standard-8",
        worker_machine_type="n2-standard-16",
        num_workers=5,
        zone="us-central1-a",
        service_account="sa@my-proj.iam.gserviceaccount.com",
        network_tags="allow-ssh,dataproc-worker",
        idle_delete_ttl_seconds=1800,
    )
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    create_call = next(c for c in client.calls if c[0] == "create_cluster")
    cluster = create_call[1]["cluster"]
    config = cluster["config"]
    assert config["master_config"]["machine_type_uri"] == "n2-standard-8"
    assert config["worker_config"]["machine_type_uri"] == "n2-standard-16"
    assert config["worker_config"]["num_instances"] == 5
    assert config["gce_cluster_config"]["zone_uri"] == "us-central1-a"
    assert config["gce_cluster_config"]["service_account"] == "sa@my-proj.iam.gserviceaccount.com"
    assert config["gce_cluster_config"]["tags"] == ["allow-ssh", "dataproc-worker"]
    assert config["lifecycle_config"]["idle_delete_ttl"].seconds == 1800
