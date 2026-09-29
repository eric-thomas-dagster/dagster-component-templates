"""Shared test helpers for WarehouseJoinComponent."""
import importlib.util
import pathlib
import time
from types import ModuleType

import pytest


def duckdb_reconnect_readonly(db_path: str):
    """A read-only connect right after materialize() can lose a race
    against the just-finished write connection's pool teardown -- DuckDB
    raises ConnectionException outright rather than blocking/waiting.
    Same transient-race workaround as this repo's other DuckDB-touching
    tests/preview code (short retry with backoff)."""
    import duckdb

    last_error = None
    for attempt in range(6):
        try:
            return duckdb.connect(db_path, read_only=True)
        except Exception as e:
            last_error = e
            time.sleep(0.3 * (attempt + 1))
    raise last_error


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "warehouse_join_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import dagster_duckdb  # noqa: F401
        return True
    except ImportError:
        return False


requires_duckdb = pytest.mark.skipif(
    not _duckdb_available(),
    reason="requires duckdb + dagster-duckdb (pip install duckdb dagster-duckdb)",
)
