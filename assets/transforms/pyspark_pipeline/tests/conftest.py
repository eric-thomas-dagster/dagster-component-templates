import importlib.util
import os

import pytest

pytest.importorskip("pyspark")

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def mod():
    spec = importlib.util.spec_from_file_location(
        "pyspark_pipeline_under_test", os.path.join(_HERE, "component.py")
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="session")
def spark():
    """A real local SparkSession, shared across the whole test session (starting the JVM
    per-test would be far too slow). Skips cleanly (not a hard failure) when no JVM is
    reachable -- this repo's tests run on machines/CI that may not have a JDK installed,
    and `pyspark.importorskip` above only guards the Python package, not the JVM underneath
    it."""
    try:
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("pyspark_pipeline-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
    except Exception as e:  # noqa: BLE001 -- any JVM/py4j startup failure should skip, not error
        pytest.skip(f"no usable Spark/JVM in this environment: {e}")
    yield session
    session.stop()
