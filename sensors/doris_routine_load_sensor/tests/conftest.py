"""Test helpers for DorisRoutineLoadSensorComponent: loads component.py via importlib; only the SQL
engine's result rows are faked (no real Doris/MySQL connection involved)."""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("doris_routine_load_sensor_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *a, **kw):
        return FakeResult(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeEngine:
    """Same method the real DorisResource exposes to this sensor: get_engine() -> a SQLAlchemy-engine-shaped
    object. `.begin()` is the only method actually used (`with engine.begin() as conn: conn.execute(...)`)."""

    def __init__(self, rows):
        self._rows = rows

    def begin(self):
        return FakeConn(self._rows)


class FakeDorisResource:
    def __init__(self, rows):
        self._engine = FakeEngine(rows)
        self.get_engine_calls = 0

    def get_engine(self):
        self.get_engine_calls += 1
        return self._engine
