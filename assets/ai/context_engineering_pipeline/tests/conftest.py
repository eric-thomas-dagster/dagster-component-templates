"""Shared test helpers for ContextEngineeringPipelineComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed.
"""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "context_engineering_pipeline_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sentence_transformers_available() -> bool:
    try:
        import sentence_transformers  # noqa: F401
        return True
    except ImportError:
        return False


def _transformers_available() -> bool:
    try:
        import transformers  # noqa: F401
        return True
    except ImportError:
        return False


def _chromadb_available() -> bool:
    try:
        import chromadb  # noqa: F401
        return True
    except ImportError:
        return False


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import dagster_duckdb  # noqa: F401
        return True
    except ImportError:
        return False


requires_sentence_transformers = pytest.mark.skipif(
    not _sentence_transformers_available(),
    reason="requires sentence-transformers (pip install sentence-transformers)",
)
requires_transformers = pytest.mark.skipif(
    not _transformers_available(),
    reason="requires transformers+torch (pip install transformers torch)",
)
requires_chromadb = pytest.mark.skipif(
    not _chromadb_available(), reason="requires chromadb (pip install chromadb)",
)
requires_duckdb = pytest.mark.skipif(
    not _duckdb_available(),
    reason="requires duckdb + dagster-duckdb (pip install duckdb dagster-duckdb)",
)
