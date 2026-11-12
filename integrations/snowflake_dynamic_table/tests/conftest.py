"""Shared test helpers for snowflake_dynamic_table (self-contained; loads component.py via importlib).

Only the Snowflake wire boundary is faked; the component runs for real under dg.materialize().
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("snowflake_dynamic_table_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql):
        self.conn.executed.append(sql)

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.close_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.close_calls += 1


class FakeSnowflakeConnectorModule:
    """Recording fake for `snowflake.connector.connect` (the inline-field path)."""

    def __init__(self):
        self.conns = []
        self.kwargs_seen = []

    def connect(self, **kwargs):
        self.kwargs_seen.append(kwargs)
        conn = FakeConnection()
        self.conns.append(conn)
        return conn
