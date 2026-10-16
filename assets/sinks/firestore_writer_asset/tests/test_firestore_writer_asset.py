"""resource_key tests for FirestoreWriterAssetComponent (only the google.* client libs are faked)."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import (FakeFirestoreClient, FakeFirestoreResource,
                       install_fake_google_firestore, load_component_module)

DF = pd.DataFrame({"id": [10, 20, 30], "name": ["a", "b", "c"]})
CREDS = {"project_id": "inline-proj", "client_email": "svc@inline", "private_key": "x"}
EXPECTED = [
    ("merge", "people/10", {"name": "a"}),
    ("merge", "people/20", {"name": "b"}),
    ("merge", "people/30", {"name": "c"}),
]


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _no_adc(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)


def _up():
    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return DF.copy()
    return src


def _comp(mod, **kw):
    return mod.FirestoreWriterAssetComponent(
        asset_name="to_fs", upstream_asset_key="src", collection="people", id_column="id", **kw)


def test_default_none_uses_inline_credentials(mod, monkeypatch):
    clients, creds_calls = install_fake_google_firestore(monkeypatch)
    comp = _comp(mod, credentials=CREDS)
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    r = dg.materialize([_up(), sink])
    assert r.success
    assert creds_calls == [CREDS]
    (c,) = clients
    assert c.kwargs["project"] == "inline-proj" and c.kwargs["credentials"] == ("creds", "svc@inline")
    assert c.committed == EXPECTED
    assert r.asset_materializations_for_node("to_fs")[0].metadata["rows_written"].value == 3


def test_resource_path_same_writes_and_no_inline_client(mod, monkeypatch):
    clients, creds_calls = install_fake_google_firestore(monkeypatch)
    shared = FakeFirestoreClient(project="resource-proj")
    # No credentials/credentials_path/ADC at all: build_defs must not raise.
    sink = _comp(mod, resource_key="fs_shared").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"fs_shared"}
    r = dg.materialize([_up(), sink], resources={"fs_shared": FakeFirestoreResource(shared)})
    assert r.success
    assert clients == [] and creds_calls == []  # inline firestore.Client / credentials never built
    assert shared.committed == EXPECTED
    assert r.asset_materializations_for_node("to_fs")[0].metadata["rows_written"].value == 3


def test_resource_path_batches_commit_by_batch_size(mod, monkeypatch):
    install_fake_google_firestore(monkeypatch)
    shared = FakeFirestoreClient()
    sink = _comp(mod, resource_key="fs_shared", batch_size=2, write_mode="set").build_defs(None).assets[0]
    assert dg.materialize([_up(), sink], resources={"fs_shared": FakeFirestoreResource(shared)}).success
    assert shared.commit_count == 2 and [op[0] for op in shared.committed] == ["set"] * 3


def test_unregistered_resource_key_fails_loudly(mod, monkeypatch):
    install_fake_google_firestore(monkeypatch)
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_up(), sink])


def test_no_resource_key_and_no_credentials_still_raises_with_hint(mod):
    with pytest.raises(ValueError, match="resource_key"):
        _comp(mod).build_defs(None)


def test_resource_key_ignores_dangling_adc_env(mod, monkeypatch, tmp_path):
    """GOOGLE_APPLICATION_CREDENTIALS is routinely exported in dev shells; a resource_key
    component must not try to open it at build time."""
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "nope.json"))
    _comp(mod, resource_key="fs_shared").build_defs(None)
