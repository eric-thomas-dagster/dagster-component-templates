"""Shared test helpers for HybridKnowledgeSearchServerComponent."""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("hybrid_knowledge_search_server_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sqlglot_available() -> bool:
    try:
        import sqlglot  # noqa: F401
        return True
    except ImportError:
        return False


requires_sqlglot = pytest.mark.skipif(not _sqlglot_available(), reason="requires sqlglot (pip install sqlglot)")
