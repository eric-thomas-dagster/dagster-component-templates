"""Shared test helpers for KnowledgeGraphBuilderComponent."""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("knowledge_graph_builder_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except ImportError:
        return False


def _sqlglot_available() -> bool:
    try:
        import sqlglot  # noqa: F401
        return True
    except ImportError:
        return False


requires_duckdb = pytest.mark.skipif(not _duckdb_available(), reason="requires duckdb (pip install duckdb)")
requires_sqlglot = pytest.mark.skipif(not _sqlglot_available(), reason="requires sqlglot (pip install sqlglot)")
