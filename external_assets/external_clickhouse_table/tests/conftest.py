"""Test helpers for ExternalClickHouseTableComponent: loads component.py via importlib; only the
external clickhouse-connect boundary is faked."""
import importlib.util
import sys
import types
from pathlib import Path
from types import ModuleType


def load_component_module() -> ModuleType:
    here = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("external_clickhouse_table_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeClickHouseClient:
    """Same `.command()` surface as clickhouse-connect's Client, enough to serve the component's
    5 system-table queries regardless of call order."""

    def __init__(self, row_count=0, size_bytes=0, last_modified="", parts_count=0, engine=""):
        self.row_count = row_count
        self.size_bytes = size_bytes
        self.last_modified = last_modified
        self.parts_count = parts_count
        self.engine = engine
        self.commands = []
        self.kwargs = {}

    def command(self, sql):
        self.commands.append(sql)
        if "bytes_on_disk" in sql:
            return self.size_bytes
        if "modification_time" in sql:
            return self.last_modified
        if "system.parts" in sql:  # count() FROM system.parts (checked after the two above)
            return self.parts_count
        if "system.tables" in sql:
            return self.engine
        return self.row_count  # plain SELECT count() FROM db.table


def install_fake_clickhouse_connect(monkeypatch, client: FakeClickHouseClient):
    mod = types.ModuleType("clickhouse_connect")

    def get_client(**kwargs):
        client.kwargs = kwargs
        return client

    mod.get_client = get_client
    monkeypatch.setitem(sys.modules, "clickhouse_connect", mod)


class FakeObserveResource:
    """A custom resource exposing the legacy `.observe(source) -> dict` contract."""

    def __init__(self, result: dict):
        self._result = result
        self.calls = []

    def observe(self, source):
        self.calls.append(source)
        return dict(self._result)


class FakeGetClientResource:
    """Same surface as the shared ClickHouseResourceComponent's ClickHouseResource: get_client() only."""

    def __init__(self, client: FakeClickHouseClient):
        self._client = client

    def get_client(self):
        return self._client
