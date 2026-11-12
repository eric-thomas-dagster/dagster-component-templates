"""Test helpers for Neo4jReaderComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (neo4j driver) is faked; everything the component
itself owns -- resource_key dispatch, metadata -- runs for real under dg.materialize().
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("neo4j_reader_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeSession:
    def __init__(self, driver, database):
        self.driver, self.database = driver, database

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, cypher):
        self.driver.queries.append({"database": self.database, "cypher": cypher})
        return self.driver.rows


class FakeDriver:
    instances = []
    rows = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 40}]

    def __init__(self, uri=None, auth=None):
        self.uri, self.auth = uri, auth
        self.queries = []
        self.closed = False
        FakeDriver.instances.append(self)

    def session(self, database=None):
        return FakeSession(self, database)

    def close(self):
        self.closed = True


def install_fake_neo4j(monkeypatch):
    """Inject a fake `neo4j` with GraphDatabase.driver (restored by monkeypatch)."""
    FakeDriver.instances = []
    mod = types.ModuleType("neo4j")
    mod.GraphDatabase = types.SimpleNamespace(driver=lambda uri, auth=None: FakeDriver(uri, auth))
    monkeypatch.setitem(sys.modules, "neo4j", mod)
    return FakeDriver


class FakeNeo4jResource:
    """Stands in for the registered Neo4jResource (exposes .get_driver())."""

    def __init__(self, driver):
        self._driver = driver

    def get_driver(self):
        return self._driver
