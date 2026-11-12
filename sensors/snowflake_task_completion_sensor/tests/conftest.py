"""Shared test helpers for snowflake_task_completion_sensor (self-contained; loads
component.py via importlib). Only the Snowflake wire boundary is faked."""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("snowflake_task_completion_sensor_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []
        self.closed = False

    def execute(self, sql):
        self.executed.append(sql)

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, rows, kwargs=None):
        self.kwargs = kwargs or {}
        self._rows = rows
        self.closed = False
        self._cursor = None

    def cursor(self):
        self._cursor = FakeCursor(self._rows)
        return self._cursor

    def close(self):
        self.closed = True
