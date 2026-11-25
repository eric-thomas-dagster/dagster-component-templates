"""Shared test helpers for FieldEncryptionComponent."""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "field_encryption_test", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _esdk_available() -> bool:
    try:
        import aws_encryption_sdk  # noqa: F401
        return True
    except ImportError:
        return False


requires_esdk = pytest.mark.skipif(
    not _esdk_available(),
    reason="requires aws-encryption-sdk (pip install aws-encryption-sdk)",
)
