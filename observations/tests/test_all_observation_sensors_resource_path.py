"""Parametrized coverage — every observation sensor's `.observe(source)` path.

Verifies for all 20 observation sensor components that:
  a) Setting resource_key routes to `.observe(source)` on the resource
  b) The returned dict's `data_version` key becomes the DATA_VERSION_TAG on
     the emitted AssetObservation
  c) Remaining dict entries pass through as metadata
  d) `**_resources` kwargs injection works (Dagster injects the resource by
     name; sensor body must accept it)

The 3 core sensors (kafka, sql, s3) have detailed tests in their own files;
this file catches regressions across the other 17 uniformly.
"""
import importlib.util
import pathlib

import pytest
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from .conftest import run_sensor


HERE = pathlib.Path(__file__).resolve().parent.parent  # observations/


def _load(subdir: str):
    spec = importlib.util.spec_from_file_location(
        subdir, HERE / subdir / "component.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Observer:
    """Universal `.observe(source)` test double."""
    def __init__(self, data_version: str = "v1", **extra):
        self._dv = data_version
        self._extra = extra
        self.calls: list = []

    def observe(self, source):
        self.calls.append(source)
        return {"data_version": self._dv, **self._extra}


# (subdir, ComponentClass attr, kwargs to instantiate, resource_key kwarg name)
# Each tuple: component subdir, class name, constructor kwargs (minus asset_key /
# sensor_name / resource_key which we set uniformly).
SENSORS = [
    # Object-store (already upgraded)
    ("adls_observation_sensor", "AdlsObservationSensorComponent",
     {"account_name": "acct", "container_name": "c", "path_prefix": "p/"}),
    ("gcs_observation_sensor", "GcsObservationSensorComponent",
     {"bucket_name": "b", "prefix": "p/"}),
    ("sftp_path_observation_sensor", "SftpPathObservationSensorComponent",
     {"host": "h.example.com", "remote_path": "/data"}),
    # Warehouse (already upgraded)
    ("bigquery_table_observation_sensor", "BigQueryTableObservationSensorComponent",
     {"project_id": "p", "dataset_id": "d", "table_id": "t"}),
    ("snowflake_table_observation_sensor", "SnowflakeTableObservationSensorComponent",
     {"account": "a", "database": "D", "schema_name": "S", "table_name": "T",
      "username_env_var": "USER"}),
    ("databricks_table_observation_sensor", "DatabricksTableObservationSensorComponent",
     {"workspace_url": "https://x", "schema_name": "s", "table_name": "t"}),
    ("clickhouse_table_observation_sensor", "ClickHouseTableObservationSensorComponent",
     {"database": "d", "table": "t"}),
    # Messaging (upgraded in v0.10.46)
    ("eventhubs_observation_sensor", "EventHubsObservationSensorComponent",
     {"namespace": "ns", "eventhub_name": "eh"}),
    ("kinesis_observation_sensor", "KinesisObservationSensorComponent",
     {"stream_name": "stream1"}),
    ("mqtt_observation_sensor", "MqttObservationSensorComponent",
     {"broker_host": "localhost", "topic": "t"}),
    ("nats_observation_sensor", "NatsObservationSensorComponent",
     {"servers": "nats://x", "stream_name": "s"}),
    ("pubsub_observation_sensor", "PubsubObservationSensorComponent",
     {"project_id": "p", "topic_id": "t"}),
    ("pulsar_observation_sensor", "PulsarObservationSensorComponent",
     {"service_url": "pulsar://x", "topic": "t"}),
    ("rabbitmq_observation_sensor", "RabbitmqObservationSensorComponent",
     {"host": "h", "queue_name": "q"}),
    ("redis_stream_observation_sensor", "RedisStreamObservationSensorComponent",
     {"stream_name": "s", "host": "h"}),
    ("servicebus_observation_sensor", "ServiceBusObservationSensorComponent",
     {"namespace": "ns", "queue_name": "q"}),
    ("sqs_observation_sensor", "SqsObservationSensorComponent",
     {"queue_url": "https://sqs/x/q"}),
]


@pytest.mark.parametrize("subdir,cls_name,ctor_kwargs", SENSORS, ids=[s[0] for s in SENSORS])
def test_resource_path_attaches_data_version(subdir, cls_name, ctor_kwargs):
    """Setting resource_key routes to .observe(source) and attaches DATA_VERSION_TAG."""
    mod = _load(subdir)
    Cls = getattr(mod, cls_name)
    c = Cls(
        sensor_name="s1",
        asset_key="ns/ent",
        resource_key="observer",
        **ctor_kwargs,
    )
    defs = c.build_defs(None)
    obs_client = Observer(data_version="ver-42", healthy=True, foo="bar")
    result = run_sensor(defs, resources={"observer": obs_client})

    assert result.asset_events, (
        f"{subdir}: no asset events emitted "
        f"(skip_reason={result.skip_reason!r})"
    )
    obs = result.asset_events[0]
    assert obs.tags[DATA_VERSION_TAG] == "ver-42", f"{subdir}: DV mismatch"
    # data_version popped from metadata; extras passed through
    assert "data_version" not in obs.metadata, f"{subdir}: data_version leaked into metadata"
    assert obs.metadata["healthy"].value is True, f"{subdir}: metadata pass-through broke"
    assert obs.metadata["foo"].value == "bar"
    # .observe was called exactly once with some non-empty source
    assert len(obs_client.calls) == 1, f"{subdir}: expected 1 .observe() call, got {len(obs_client.calls)}"
    assert obs_client.calls[0], f"{subdir}: source arg was empty"


@pytest.mark.parametrize("subdir,cls_name,ctor_kwargs", SENSORS, ids=[s[0] for s in SENSORS])
def test_resource_path_data_version_stable_across_ticks(subdir, cls_name, ctor_kwargs):
    """Same underlying state → same DV → downstream automation stays idle."""
    mod = _load(subdir)
    Cls = getattr(mod, cls_name)
    c = Cls(sensor_name="s1", asset_key="ns/ent", resource_key="observer", **ctor_kwargs)
    defs = c.build_defs(None)

    obs1 = run_sensor(defs, resources={"observer": Observer("v1")}).asset_events[0]
    obs2 = run_sensor(defs, resources={"observer": Observer("v1")}).asset_events[0]
    assert obs1.tags[DATA_VERSION_TAG] == obs2.tags[DATA_VERSION_TAG]


@pytest.mark.parametrize("subdir,cls_name,ctor_kwargs", SENSORS, ids=[s[0] for s in SENSORS])
def test_resource_path_data_version_changes_on_state_change(subdir, cls_name, ctor_kwargs):
    """New underlying state → new DV → downstream automation fires."""
    mod = _load(subdir)
    Cls = getattr(mod, cls_name)
    c = Cls(sensor_name="s1", asset_key="ns/ent", resource_key="observer", **ctor_kwargs)
    defs = c.build_defs(None)

    obs1 = run_sensor(defs, resources={"observer": Observer("v1")}).asset_events[0]
    obs2 = run_sensor(defs, resources={"observer": Observer("v2")}).asset_events[0]
    assert obs1.tags[DATA_VERSION_TAG] != obs2.tags[DATA_VERSION_TAG]
