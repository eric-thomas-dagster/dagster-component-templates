"""Committed tests for MongoDBIngestionComponent's resource_key retrofit.

This component builds its own inline dlt source, opening the MongoDB
connection deep inside a `@dlt.resource` generator that only runs when
`pipeline.run(source)` actually iterates it -- which the existing
conftest's FakePipeline.run() deliberately never does (see its own
docstring). Exercising the real generator here would mean teaching the
fake pipeline to iterate dlt resources, which is out of scope for this
retrofit. Instead, `_build_mongo_client_factory` -- the one piece of new
code this retrofit adds -- is tested directly as the plain Python
function it is: given (connection_url, resource_key, context), it
returns the zero-arg callable the dlt resource calls to open its
connection. `required_resource_keys` wiring is checked by building real
Dagster AssetsDefinitions via build_defs(), for both asset_shape values.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        connection_url="mongodb://localhost:27017",
        database="mydb",
        collection_names=["users"],
    )
    kwargs.update(overrides)
    return mod.MongoDBIngestionComponent(**kwargs)


class _FakeMongoResource:
    def __init__(self, client):
        self._client = client
        self.get_client_calls = 0

    def get_client(self):
        self.get_client_calls += 1
        return self._client


class _FakeResources:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class _FakeContext:
    def __init__(self, resources):
        self.resources = resources


# ------------------------------------------------------- _build_mongo_client_factory

def test_resource_key_unset_uses_inline_connection_url(mod, monkeypatch):
    calls = []

    class _FakeMongoClient:
        def __init__(self, url):
            calls.append(url)

    import sys
    import types

    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.MongoClient = _FakeMongoClient
    monkeypatch.setitem(sys.modules, "pymongo", fake_pymongo)

    factory = mod._build_mongo_client_factory("mongodb://inline.example:27017", None, context=None)
    factory()
    assert calls == ["mongodb://inline.example:27017"]


def test_resource_key_set_calls_shared_resource_get_client(mod):
    sentinel_client = object()
    fake_resource = _FakeMongoResource(sentinel_client)
    context = _FakeContext(_FakeResources(mongo_shared=fake_resource))

    # Inline connection_url is also set here, to prove resource_key takes
    # precedence (same precedence rule as dataframe_to_snowflake) rather
    # than being combined with or validated against it.
    factory = mod._build_mongo_client_factory("mongodb://inline.example:27017", "mongo_shared", context)
    client = factory()

    assert client is sentinel_client
    assert fake_resource.get_client_calls == 1


def test_neither_connection_url_nor_resource_key_raises_loudly(mod):
    with pytest.raises(ValueError, match="connection_url.*resource_key"):
        mod._build_mongo_client_factory(None, None, context=None)


# ------------------------------------------------------------- required_resource_keys

def test_resource_key_default_none_no_required_resource_keys(mod):
    comp = _make_component(mod)
    assert comp.resource_key is None
    asset_def = comp.build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()


def test_resource_key_set_adds_required_resource_key_single_shape(mod):
    comp = _make_component(mod, resource_key="mongo_shared")
    asset_def = comp.build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"mongo_shared"}


def test_resource_key_set_adds_required_resource_key_multi_asset_shape(mod):
    comp = _make_component(
        mod, resource_key="mongo_shared", asset_shape="multi_asset",
        collection_names=["users", "orders"],
    )
    asset_def = comp.build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"mongo_shared"}


def test_connection_url_now_optional_when_resource_key_set(mod):
    # Previously connection_url was a required field; it must now be
    # omittable when resource_key is the connection source instead.
    comp = mod.MongoDBIngestionComponent(
        asset_name="test_asset", database="mydb", collection_names=["users"],
        resource_key="mongo_shared",
    )
    assert comp.connection_url is None
    # Building defs (schema/decorator wiring) must not require connection_url.
    comp.build_defs(None)
