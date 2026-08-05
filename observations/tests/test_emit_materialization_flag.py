"""Verify the emit_materialization flag toggles between AssetMaterialization
and AssetObservation on the emitted event, for a representative sensor
from each family (object-store, warehouse, messaging, monitor)."""
import importlib.util
import pathlib

import pytest
from dagster import AssetMaterialization, AssetObservation
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from .conftest import run_sensor


HERE = pathlib.Path(__file__).resolve().parent.parent  # observations/


def _load_obs(subdir: str):
    spec = importlib.util.spec_from_file_location(subdir, HERE / subdir / "component.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Observer:
    def __init__(self, data_version="v1"):
        self._dv = data_version

    def observe(self, source):
        return {"data_version": self._dv, "count": 1}


# Representative sensor per family — same shape across all 20.
CASES = [
    ("kafka_observation_sensor", "KafkaObservationSensorComponent",
     {"asset_key": "k", "bootstrap_servers": "x", "topic": "t"}),
    ("s3_observation_sensor", "S3ObservationSensorComponent",
     {"asset_key": "k", "bucket_name": "b"}),
    ("bigquery_table_observation_sensor", "BigQueryTableObservationSensorComponent",
     {"asset_key": "k", "project_id": "p", "dataset_id": "d", "table_id": "t"}),
    ("sqs_observation_sensor", "SqsObservationSensorComponent",
     {"asset_key": "k", "queue_url": "https://x/q"}),
]


@pytest.mark.parametrize("subdir,cls_name,extra_kwargs", CASES, ids=[c[0] for c in CASES])
def test_emit_materialization_default_true(subdir, cls_name, extra_kwargs):
    """Default (emit_materialization=True) → AssetMaterialization."""
    mod = _load_obs(subdir)
    Cls = getattr(mod, cls_name)
    c = Cls(sensor_name="s", resource_key="observer", **extra_kwargs)
    defs = c.build_defs(None)
    result = run_sensor(defs, resources={"observer": Observer()})
    ev = result.asset_events[0]
    assert isinstance(ev, AssetMaterialization), (
        f"{subdir}: expected AssetMaterialization by default, got {type(ev).__name__}"
    )
    # DV tag still attached
    assert ev.tags[DATA_VERSION_TAG] == "v1"


@pytest.mark.parametrize("subdir,cls_name,extra_kwargs", CASES, ids=[c[0] for c in CASES])
def test_emit_materialization_false_gives_observation(subdir, cls_name, extra_kwargs):
    """emit_materialization=False → AssetObservation."""
    mod = _load_obs(subdir)
    Cls = getattr(mod, cls_name)
    c = Cls(sensor_name="s", resource_key="observer",
            emit_materialization=False, **extra_kwargs)
    defs = c.build_defs(None)
    result = run_sensor(defs, resources={"observer": Observer()})
    ev = result.asset_events[0]
    assert isinstance(ev, AssetObservation), (
        f"{subdir}: expected AssetObservation with flag=False, got {type(ev).__name__}"
    )
    # DV tag still attached — the swap only changes the class, not the payload
    assert ev.tags[DATA_VERSION_TAG] == "v1"
