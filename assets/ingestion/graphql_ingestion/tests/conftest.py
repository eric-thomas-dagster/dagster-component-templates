"""Shared test helpers for GraphQLIngestionComponent.

Mocks ONLY requests.post (keyed by query name, read from the request body)
-- everything this component actually owns (partitions_def construction,
pagination, JSON flattening, real SQL write, partition filtering) is
exercised for real through dg.materialize() + a real temp-file SQLite
database.
"""
import importlib.util
import pathlib
from types import ModuleType
from unittest.mock import MagicMock


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "graphql_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeGraphQLServer:
    """Maps a query's `name` variable embedded in the request body (we
    match on the literal query string here, since this fixture only ever
    runs one query per test) to a canned list of record dicts; records
    every request body."""

    def __init__(self, records: list):
        self.records = records
        self.calls: list = []

    def handle_post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(json)
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        resp.json.return_value = {"data": {"items": self.records}}
        return resp
