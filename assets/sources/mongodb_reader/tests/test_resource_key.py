"""Committed tests for MongodbReaderComponent's resource_key retrofit.

No tests/ directory existed for this component before this retrofit, so this
file also covers the basic inline connection_string_env_var path as a
baseline. Mirrors mongodb_writer's test_mongodb_writer.py resource_key tests:
real dg.materialize() end to end, with only the external pymongo client
faked.
"""
import dagster as dg
import pytest

from .conftest import (
    FakeMongoClient,
    FakeMongoResource,
    install_fake_pymongo,
    load_component_module,
)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    monkeypatch.setenv("MONGODB_URI", "mongodb://inline.example:27017")
    return install_fake_pymongo(monkeypatch)


def _component(mod, **kw):
    return mod.MongodbReaderComponent(
        asset_name="from_mongo", database="shop", collection="orders", **kw)


def test_resource_key_default_none_and_inline_path_unchanged(mod, fake):
    comp = _component(mod)
    assert comp.resource_key is None
    source = comp.build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == set()

    fake.preseed = {("shop", "orders"): [{"id": 1, "total": 1.5}, {"id": 2, "total": 2.5}]}
    result = dg.materialize([source])
    assert result.success
    (client,) = fake.instances
    assert client.args == ("mongodb://inline.example:27017",)  # inline MongoClient path ran


def test_resource_key_set_adds_required_resource_key(mod, fake):
    source = _component(mod, resource_key="mongo_shared").build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == {"mongo_shared"}


def test_resource_key_set_uses_shared_client_not_inline_mongoclient(mod, fake, monkeypatch):
    monkeypatch.delenv("MONGODB_URI")  # inline config absent: resource must be sufficient
    from .conftest import FakeCollection
    shared = FakeMongoClient()
    shared.colls[("shop", "orders")] = FakeCollection(docs=[{"id": 9, "total": 9.5}])
    fake.instances.clear()  # constructing `shared` itself registered an instance; discard it

    source = _component(mod, resource_key="mongo_shared").build_defs(None).assets[0]
    result = dg.materialize([source], resources={"mongo_shared": FakeMongoResource(shared)})
    assert result.success

    assert fake.instances == []  # the inline MongoClient was never constructed
    assert shared.colls[("shop", "orders")].find_calls  # find() was actually invoked
    mats = result.asset_materializations_for_node("from_mongo")
    assert mats[0].metadata["dagster/row_count"].value == 1


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake):
    source = _component(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([source])
