"""Test helpers for DataframeToClickHouseComponent: loads component.py via importlib; only the external service is faked."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataframe_to_clickhouse_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeClickHouseClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.inserts = []

    def insert_df(self, table=None, df=None, database=None, **kw):
        self.inserts.append({"table": table, "df": df.copy(), "database": database})

    def query(self, sql):
        return types.SimpleNamespace(result_rows=[[42]])


def install_fake_clickhouse_connect(monkeypatch):
    clients = []
    mod = types.ModuleType("clickhouse_connect")

    def get_client(**kwargs):
        c = FakeClickHouseClient(**kwargs)
        clients.append(c)
        return c

    mod.get_client = get_client
    monkeypatch.setitem(sys.modules, "clickhouse_connect", mod)
    return clients


class FakeClickHouseResource:
    """Same surface as ClickHouseResource: host/port/database/username/password/secure + get_client()."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client
