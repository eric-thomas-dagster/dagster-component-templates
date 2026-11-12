"""Committed tests for CassandraWriterComponent's resource_key retrofit.

Mirrors mongodb_writer's test_mongodb_writer.py resource_key tests: real
dg.materialize() end to end, with only the external service faked (same
install_fake_cassandra fake used by test_cassandra_writer.py). The shared
resource path uses a bare CassandraResource-shaped stand-in exposing
get_session() -> the fake session directly, so no Cluster is ever built
when resource_key is set.
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


def _unpartitioned_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "total": [1.5, 2.5, 3.5]})
    return orders_flat


def _component(mod, **kw):
    return mod.CassandraWriterComponent(
        asset_name="to_cassandra", upstream_asset_key=kw.pop("upstream_asset_key", "orders_flat"), keyspace="shop",
        table="orders", **kw)


class FakeCassandraResource(dg.ConfigurableResource):
    """Stand-in for resources.cassandra_resource.CassandraResourceComponent's
    registered CassandraResource: same get_session() shape, pre-wired to a
    fake session so no real driver call occurs."""

    def get_session(self):
        global _shared_session
        return _shared_session


_shared_session = None


def test_resource_key_default_none_and_inline_path_unchanged(mod, fake):
    comp = _component(mod)
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_unpartitioned_upstream(), sink]).success
    (cluster,) = fake.instances  # the inline Cluster path ran
    (session,) = cluster.sessions
    assert session.keyspace == "shop"


def test_resource_key_set_adds_required_resource_key(mod, fake):
    sink = _component(mod, resource_key="cassandra_shared").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"cassandra_shared"}


def test_resource_key_set_uses_shared_session_not_inline_cluster(mod, fake):
    global _shared_session
    _shared_session = FakeSession(cluster=None, keyspace="shop")

    sink = _component(mod, resource_key="cassandra_shared").build_defs(None).assets[0]
    assert dg.materialize(
        [_unpartitioned_upstream(), sink],
        resources={"cassandra_shared": FakeCassandraResource()},
    ).success

    assert fake.instances == []  # no inline Cluster was ever constructed
    assert _shared_session.prepared == ["INSERT INTO orders (id, total) VALUES (?, ?)"]
    written = [values for batch in _shared_session.executed for (_p, values) in batch.entries]
    assert sorted(v[0] for v in written) == [1, 2, 3]


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake):
    sink = _component(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_unpartitioned_upstream(), sink])
