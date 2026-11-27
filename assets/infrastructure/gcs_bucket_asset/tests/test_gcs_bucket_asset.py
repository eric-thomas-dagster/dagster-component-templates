"""Real, committed tests for GCSBucketAssetComponent.

Mocks only the external call (a fake google-cloud-storage Client matching the real
google.api_core.exceptions.Conflict/NotFound shapes) -- everything else (idempotency logic,
the Conflict-then-get_bucket ownership check, force-delete blob enumeration, Dagster asset
wiring) is real.
"""
import dagster as dg
import pytest
from google.api_core.exceptions import Conflict, NotFound

from .conftest import load_component_module

mod = load_component_module()


class FakeBucket:
    def __init__(self, name):
        self.name = name
        self.storage_class = None
        self.delete_calls = []

    def delete(self, force=False):
        self.delete_calls.append({"force": force})


class FakeGCSClient:
    def __init__(self, *, create_raises=None, get_bucket_raises=None, existing_blobs=None):
        self.calls = []
        self._create_raises = create_raises
        self._get_bucket_raises = get_bucket_raises
        self._existing_blobs = existing_blobs or []
        self._last_bucket = None

    def bucket(self, name):
        self.calls.append(("bucket", name))
        b = FakeBucket(name)
        self._last_bucket = b
        return b

    def create_bucket(self, bucket, location=None):
        self.calls.append(("create_bucket", bucket.name, location, bucket.storage_class))
        if self._create_raises:
            raise self._create_raises
        return bucket

    def get_bucket(self, name):
        self.calls.append(("get_bucket", name))
        if self._get_bucket_raises:
            raise self._get_bucket_raises
        return self._last_bucket or FakeBucket(name)

    def list_blobs(self, name):
        self.calls.append(("list_blobs", name))
        return list(self._existing_blobs)


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.deleted = False

    def delete(self):
        self.deleted = True


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_gcs_client", lambda credentials_env_var: client)


def _comp(**kw):
    args = dict(asset_name="out", bucket_name="my-bucket")
    args.update(kw)
    return mod.GCSBucketAssetComponent(**args)


def test_ensure_exists_creates_when_missing(monkeypatch):
    client = FakeGCSClient()
    _patch_client(monkeypatch, client)
    comp = _comp(action="ensure_exists", location="us-central1", storage_class="NEARLINE")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["already_existed"].value is False
    create_call = [c for c in client.calls if c[0] == "create_bucket"][0]
    assert create_call == ("create_bucket", "my-bucket", "us-central1", "NEARLINE")


def test_ensure_exists_is_noop_when_conflict_and_owned(monkeypatch):
    client = FakeGCSClient(create_raises=Conflict("already exists"))
    _patch_client(monkeypatch, client)
    comp = _comp(action="ensure_exists")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["already_existed"].value is True
    assert ("get_bucket", "my-bucket") in client.calls


def test_ensure_exists_reraises_when_conflict_and_not_owned(monkeypatch):
    client = FakeGCSClient(create_raises=Conflict("already exists"), get_bucket_raises=NotFound("forbidden"))
    _patch_client(monkeypatch, client)
    comp = _comp(action="ensure_exists")
    with pytest.raises(NotFound):
        dg.materialize(list(comp.build_defs(None).assets))


def test_create_action_does_not_swallow_conflict(monkeypatch):
    client = FakeGCSClient(create_raises=Conflict("already exists"))
    _patch_client(monkeypatch, client)
    comp = _comp(action="create")
    with pytest.raises(Conflict):
        dg.materialize(list(comp.build_defs(None).assets))


def test_delete_is_noop_when_bucket_already_absent(monkeypatch):
    client = FakeGCSClient(get_bucket_raises=NotFound("gone"))
    _patch_client(monkeypatch, client)
    comp = _comp(action="delete")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["existed_before"].value is False
    assert not any(c[0] == "list_blobs" for c in client.calls)


def test_delete_without_force_does_not_list_blobs(monkeypatch):
    client = FakeGCSClient()
    client._last_bucket = FakeBucket("my-bucket")
    _patch_client(monkeypatch, client)
    comp = _comp(action="delete")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["existed_before"].value is True
    assert md["objects_deleted"].value == 0
    assert not any(c[0] == "list_blobs" for c in client.calls)
    assert client._last_bucket.delete_calls == [{"force": False}]


def test_delete_with_force_empties_bucket_first(monkeypatch):
    blobs = [FakeBlob("a.txt"), FakeBlob("b.txt")]
    client = FakeGCSClient(existing_blobs=blobs)
    client._last_bucket = FakeBucket("my-bucket")
    _patch_client(monkeypatch, client)
    comp = _comp(action="delete", force_delete=True)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["objects_deleted"].value == 2
    assert all(b.deleted for b in blobs)
    # bucket.delete() must still be called with force=False -- blobs were already removed by hand
    assert client._last_bucket.delete_calls == [{"force": False}]
