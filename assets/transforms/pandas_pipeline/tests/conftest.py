import importlib.util
import os

import pytest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def mod():
    spec = importlib.util.spec_from_file_location(
        "pandas_pipeline_under_test", os.path.join(_HERE, "component.py")
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m
