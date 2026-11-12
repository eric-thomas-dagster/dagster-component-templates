"""Committed tests for RedisReaderComponent's resource_key retrofit.

Only the external service is faked; the component runs for real under dg.materialize().
This top-level resource_key is distinct from the per-entry `resource_key` inside `sinks`
(which names the resource a downstream warehouse sink writes through) -- both tests here
leave `sinks` unset.
"""
import dagster as dg
import pytest

from .conftest import FakeRedis, FakeRedisResource, install_fake_redis, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    monkeypatch.setenv("REDIS_HOST", "inline.example")
    return install_fake_redis(monkeypatch)


def _component(mod, **kw):
    return mod.RedisReaderComponent(asset_name="redis_sessions", key_pattern="session:*", **kw)


def test_resource_key_default_none_and_inline_path_unchanged(mod, fake):
    comp = _component(mod)
    assert comp.resource_key is None
    source = comp.build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == set()
    r = dg.materialize([source])
    assert r.success
    (client,) = fake.constructed
    assert client.cfg["host"] == "inline.example"
    mats = r.asset_materializations_for_node("redis_sessions")
    assert mats[0].metadata["dagster/row_count"].value == 1


def test_resource_path_uses_get_client_not_inline_config(mod, fake, monkeypatch):
    monkeypatch.delenv("REDIS_HOST")  # inline config absent: resource must be sufficient
    shared = FakeRedis()
    fake.constructed.clear()  # drop the construction FakeRedis() itself just did
    source = _component(mod, resource_key="redis_shared").build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == {"redis_shared"}
    r = dg.materialize([source], resources={"redis_shared": FakeRedisResource(shared)})
    assert r.success
    assert fake.constructed == []  # redis.Redis(...) (inline path) never called


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake):
    source = _component(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([source])
