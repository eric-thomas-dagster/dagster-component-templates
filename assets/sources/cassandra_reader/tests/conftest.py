"""Test helpers for CassandraReaderComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. Only the external service (cassandra-driver
Cluster/session) is faked; everything the component itself owns (query
execution call-through, metadata) runs for real under dg.materialize().
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("cassandra_reader_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeSession:
    def __init__(self, cluster, keyspace, rows=None):
        self.cluster, self.keyspace = cluster, keyspace
        self.queries = []
        self._rows = rows if rows is not None else [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def execute(self, query):
        self.queries.append(query)
        return self._rows


class FakeCluster:
    instances = []

    def __init__(self, hosts, port=9042, auth_provider=None):
        self.hosts, self.port, self.auth_provider = hosts, port, auth_provider
        self.sessions = []
        FakeCluster.instances.append(self)

    def connect(self, keyspace):
        s = FakeSession(self, keyspace)
        self.sessions.append(s)
        return s


class FakePlainTextAuthProvider:
    def __init__(self, username, password):
        self.username, self.password = username, password


def install_fake_cassandra(monkeypatch):
    """Inject fake cassandra.cluster / cassandra.auth (restored by monkeypatch)."""
    FakeCluster.instances = []
    pkg = types.ModuleType("cassandra")
    pkg.__path__ = []
    cluster = types.ModuleType("cassandra.cluster")
    cluster.Cluster = FakeCluster
    auth = types.ModuleType("cassandra.auth")
    auth.PlainTextAuthProvider = FakePlainTextAuthProvider
    pkg.cluster, pkg.auth = cluster, auth
    for name, m in (("cassandra", pkg), ("cassandra.cluster", cluster), ("cassandra.auth", auth)):
        monkeypatch.setitem(sys.modules, name, m)
    return FakeCluster
