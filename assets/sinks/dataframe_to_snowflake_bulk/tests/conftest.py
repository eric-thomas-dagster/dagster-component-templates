"""Test helpers for x: loads component.py via importlib; only the external service is faked."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataframe_to_snowflake_bulk_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.closed = False

    def execute(self, sql, *a, **k):
        self.conn.statements.append(sql)
        return self

    def fetchall(self):
        # Only consumed by the merge path's `DESC TABLE <staging>`.
        return [("ID",), ("NAME",)]

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.statements = []
        self.close_count = 0
        self.cursors = []

    def cursor(self):
        c = FakeCursor(self)
        self.cursors.append(c)
        return c

    def commit(self):
        pass

    def close(self):
        self.close_count += 1


def install_fake_snowflake_connect(monkeypatch):
    """Patch snowflake.connector.connect (the external service); returns list of FakeConnection."""
    import snowflake.connector

    conns = []

    def connect(**kwargs):
        c = FakeConnection(kwargs)
        conns.append(c)
        return c

    monkeypatch.setattr(snowflake.connector, "connect", connect)
    return conns
