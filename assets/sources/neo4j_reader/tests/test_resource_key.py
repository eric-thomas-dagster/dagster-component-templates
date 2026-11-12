"""Committed tests for Neo4jReaderComponent's resource_key retrofit.

Only the external service is faked; the component runs for real under dg.materialize().
"""
import dagster as dg
import pytest

from .conftest import FakeDriver, FakeNeo4jResource, install_fake_neo4j, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    monkeypatch.setenv("NEO4J_URI", "bolt://inline.example:7687")
    monkeypatch.setenv("NEO4J_USERNAME", "inline_user")
    monkeypatch.setenv("NEO4J_PASSWORD", "inline_pw")
    return install_fake_neo4j(monkeypatch)


def _component(mod, **kw):
    return mod.Neo4jReaderComponent(
        asset_name="neo4j_people", query="MATCH (n:Person) RETURN n.name AS name, n.age AS age", **kw)


def test_resource_key_default_none_and_inline_path_unchanged(mod, fake):
    comp = _component(mod, database="shopdb")
    assert comp.resource_key is None
    source = comp.build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == set()
    r = dg.materialize([source])
    assert r.success
    (driver,) = fake.instances
    assert driver.uri == "bolt://inline.example:7687"
    assert driver.auth == ("inline_user", "inline_pw")
    (query,) = driver.queries
    assert query["database"] == "shopdb"
    assert driver.closed


def test_resource_path_uses_get_driver_not_inline_config(mod, fake, monkeypatch):
    for k in ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"):
        monkeypatch.delenv(k)  # inline config absent: resource must be sufficient
    shared = FakeDriver("bolt://res.example:7687", ("u", "p"))
    fake.instances.clear()
    source = _component(mod, resource_key="neo4j_shared", database="shopdb").build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == {"neo4j_shared"}
    r = dg.materialize([source], resources={"neo4j_shared": FakeNeo4jResource(shared)})
    assert r.success
    assert fake.instances == []  # GraphDatabase.driver (inline path) never called
    (query,) = shared.queries
    assert query["database"] == "shopdb"  # reader's own `database`, not the resource's


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake):
    source = _component(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([source])
