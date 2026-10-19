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


def _duckdb_engine_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import duckdb_engine  # noqa: F401
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
def _hf_models_reachable() -> bool:
    """These tests load real HuggingFace models: fine when the hub is reachable or the models are already cached, otherwise
    they would hang ~1 minute and fail with a network error, which says nothing about the code."""
    import socket
    from pathlib import Path

    cache = Path.home() / ".cache" / "huggingface" / "hub"
    if cache.is_dir() and any(cache.iterdir()):
        return True
    try:
        socket.create_connection(("huggingface.co", 443), timeout=2).close()
        return True
    except OSError:
        return False


requires_hf_models = pytest.mark.skipif(
    not _hf_models_reachable(), reason="needs the HuggingFace hub (network) or a populated HF cache for the real models",
)


requires_chromadb = pytest.mark.skipif(
    not _chromadb_available(), reason="requires chromadb (pip install chromadb)",
)
requires_duckdb = pytest.mark.skipif(
    not _duckdb_available(),
    reason="requires duckdb + dagster-duckdb (pip install duckdb dagster-duckdb)",
)
requires_duckdb_engine = pytest.mark.skipif(
    not _duckdb_engine_available(),
    reason="requires duckdb + duckdb-engine (pip install duckdb duckdb-engine) for the bare-SQLAlchemy database_url_env_var path",
)
