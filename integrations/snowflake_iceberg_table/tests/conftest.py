"""Shared test helpers for snowflake_iceberg_table (self-contained; loads component.py via importlib).

Only the external service boundary is faked (snowflake.connector.connect); the component runs for real.
"""
import contextlib
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("snowflake_iceberg_table_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = [("count",)]

    def execute(self, sql):
        self.conn.executed.append(sql)

    def fetchone(self):
        return (self.conn.row_count,)

    def close(self):
        pass


class FakeConnection:
    """Recording fake standing in for a real snowflake.connector connection."""

    def __init__(self, row_count=0, **kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.close_calls = 0
        self.row_count = row_count

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.close_calls += 1


class FakeConnectorModule:
    """Stands in for the `snowflake.connector` module."""

    def __init__(self):
        self.conns = []

    def connect(self, **kwargs):
        conn = FakeConnection(**kwargs)
        self.conns.append(conn)
        return conn


@pytest.fixture
def sf(monkeypatch):
    """Patch snowflake.connector.connect with a recording fake; returns the fake module."""
    fake = FakeConnectorModule()
    import sys
    import types

    fake_pkg = types.ModuleType("snowflake")
    fake_connector = types.ModuleType("snowflake.connector")
    fake_connector.connect = fake.connect
    fake_pkg.connector = fake_connector
    monkeypatch.setitem(sys.modules, "snowflake", fake_pkg)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_connector)
    return fake


class FakeSnowflakeResource:
    """Stands in for a real dagster_snowflake.SnowflakeResource: exposes a real
    get_connection() context-manager method, backed by a FakeConnection."""

    def __init__(self, row_count=0):
        self.row_count = row_count
        self.conn = None
        self.get_connection_calls = 0

    @contextlib.contextmanager
    def get_connection(self):
        self.get_connection_calls += 1
        self.conn = FakeConnection(row_count=self.row_count, account="resource_acct")
        try:
            yield self.conn
        finally:
            self.conn.close()


@pytest.fixture
def mod():
    return load_component_module()
