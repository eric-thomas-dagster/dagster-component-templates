"""Shared test helpers for OrbIngestionComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed.

This component's architecture (dlt's generic REST API source) buries the
real HTTP boundary deep inside dlt's own client/paginator machinery, so --
unlike components that expose a hand-rolled `_list_x_page` function per
resource -- the "paid network call" boundary here is dlt's own pipeline
execution: `dlt.pipeline(...)` and `rest_api_source(...)`. Those two are
monkeypatched wholesale in the end-to-end tests (no real HTTP request, no
real DuckDB file is ever created), while resource-list building, destination
resolution, partition building, and DataFrame/metadata assembly are all
exercised for real.

`_build_resources_config` (the pure logic that turns the `resources` field
into dlt's REST resource list -- base paths, data_selector, cursor
paginator) needs no mocking at all and is tested directly.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "orb_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """Stands in for the DBAPI cursor dlt's `sql_client().execute_query()`
    yields -- only `.df()` is ever called on it by the component."""

    def __init__(self, df):
        self._df = df

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def df(self):
        return self._df


class FakeSqlClient:
    """Stands in for `pipeline.sql_client()`. `tables` maps table_name ->
    DataFrame; the first `execute_query` call (information_schema lookup)
    returns the table list, subsequent calls return that table's rows."""

    def __init__(self, tables: dict):
        self.tables = tables
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_query(self, sql: str):
        self.queries.append(sql)
        import pandas as pd

        if "information_schema.tables" in sql:
            return FakeCursor(pd.DataFrame({"table_name": list(self.tables.keys())}))
        for table_name, df in self.tables.items():
            if sql.strip().endswith(f".{table_name}"):
                return FakeCursor(df)
        return FakeCursor(pd.DataFrame())


class FakePipeline:
    """Stands in for the object `dlt.pipeline(...)` returns. Captures the
    kwargs it was constructed with and the source passed to `.run()` so
    tests can assert on them without ever touching a real DuckDB file or
    making a real HTTP request."""

    def __init__(self, tables: dict | None = None, **kwargs):
        self.kwargs = kwargs
        self.run_calls = []
        self._tables = tables or {}

    def run(self, source):
        self.run_calls.append(source)
        return f"load_info(runs={len(self.run_calls)})"

    def sql_client(self):
        return FakeSqlClient(self._tables)


def metadata_for(result, asset_name: str) -> dict:
    """Reads an asset's materialization metadata for a single-asset job."""
    mats = result.asset_materializations_for_node(asset_name)[0]
    return {k: (v.value if hasattr(v, "value") else v) for k, v in mats.metadata.items()}


def output_value(result, node_name: str):
    """Reads the Output's value (the DataFrame) for a single-asset job."""
    return result.output_for_node(node_name, "result")
