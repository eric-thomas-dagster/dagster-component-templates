"""Committed tests for CassandraReaderComponent's resource_key retrofit.

No tests/ directory existed for this component before this retrofit (unlike
its sink counterpart, cassandra_writer), so this file also covers the basic
inline query path as a baseline, following the same conventions as
cassandra_writer's tests/conftest.py: real dg.materialize() end to end, only
the external cassandra-driver module faked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeSession, install_fake_cassandra, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    return install_fake_cassandra(monkeypatch)


def _component(mod, **kw):
    return mod.CassandraReaderComponent(
        asset_name="from_cassandra", keyspace="shop", query="SELECT * FROM events LIMIT 10", **kw)


class FakeCassandraResource(dg.ConfigurableResource):
    """Stand-in for resources.cassandra_resource.CassandraResourceComponent's
    registered CassandraResource: same get_session() shape."""

    def get_session(self):
        global _shared_session
        return _shared_session


_shared_session = None


def test_resource_key_default_none_and_inline_path_unchanged(mod, fake):
    comp = _component(mod)
    assert comp.resource_key is None
    source = comp.build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([source])
    assert result.success
    (cluster,) = fake.instances  # the inline Cluster path ran
    (session,) = cluster.sessions
    assert session.keyspace == "shop"
    assert session.queries == ["SELECT * FROM events LIMIT 10"]


def test_resource_key_set_adds_required_resource_key(mod, fake):
    source = _component(mod, resource_key="cassandra_shared").build_defs(None).assets[0]
    assert source.required_resource_keys - {"io_manager"} == {"cassandra_shared"}


def test_resource_key_set_uses_shared_session_not_inline_cluster(mod, fake):
    global _shared_session
    _shared_session = FakeSession(cluster=None, keyspace="shop", rows=[{"id": 7, "name": "z"}])

    source = _component(mod, resource_key="cassandra_shared").build_defs(None).assets[0]
    result = dg.materialize([source], resources={"cassandra_shared": FakeCassandraResource()})
    assert result.success

    assert fake.instances == []  # no inline Cluster was ever constructed
    assert _shared_session.queries == ["SELECT * FROM events LIMIT 10"]
    mats = result.asset_materializations_for_node("from_cassandra")
    assert mats[0].metadata["dagster/row_count"].value == 1


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake):
    source = _component(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([source])
