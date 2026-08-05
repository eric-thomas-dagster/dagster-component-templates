"""Shared test helpers for observation sensor components.

Loads each component module directly via importlib so tests don't depend on
an installed dagster_community_components package.
"""
import importlib.util
import pathlib
from types import ModuleType


def _load(rel_dir: str) -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent  # observations/
    component_py = here / rel_dir / "component.py"
    spec = importlib.util.spec_from_file_location(
        f"{rel_dir}_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_kafka():
    return _load("kafka_observation_sensor")


def load_sql():
    return _load("sql_observation_sensor")


def load_s3():
    return _load("s3_observation_sensor")


def run_sensor(defs, resources: dict | None = None):
    """Materialize the single sensor from a Definitions object, invoke it once,
    and return its SensorResult."""
    from dagster import build_sensor_context

    sensor_defs = list(defs.sensors)
    assert len(sensor_defs) == 1
    sensor_def = sensor_defs[0]
    ctx = build_sensor_context(resources=resources or {})
    return sensor_def(ctx)
