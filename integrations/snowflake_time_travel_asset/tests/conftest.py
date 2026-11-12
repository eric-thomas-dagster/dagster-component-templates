"""Shared test helpers for snowflake_time_travel_asset (self-contained; loads component.py via importlib).

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake DBAPI2 connection/cursor pair that `pandas.read_sql` can read rows from. The resource
path uses the REAL dagster_snowflake.SnowflakeResource.get_connection(), which itself calls
the (faked) snowflake.connector.connect -- so faking just that one function covers both the
inline path and the resource path, and the component runs for real either way.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("snowflake_time_travel_asset_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = [("ID",), ("NAME",), ("AMOUNT",)]

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def fetchall(self):
        return [(1, "a", 10.0), (2, "b", 20.0)]

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.close_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def close(self):
        self.close_calls += 1
