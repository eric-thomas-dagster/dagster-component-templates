"""S3 observation sensor — DataVersion + resource_key contract."""
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from .conftest import load_s3, run_sensor


s3 = load_s3()
S3ObservationSensorComponent = s3.S3ObservationSensorComponent


class FakeS3Observer:
    def __init__(self, data_version: str, **extra):
        self._dv = data_version
        self._extra = extra

    def observe(self, source: str):
        return {"data_version": self._dv, "source": source, **self._extra}


def _first_observation(result):
    assert result.asset_events, f"no asset events; skip_reason={result.skip_reason!r}"
    return result.asset_events[0]


def test_data_version_via_resource():
    c = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="s3/data-lake/landing",
        bucket_name="data-lake", prefix="landing/",
        resource_key="s3_obs",
    )
    defs = c.build_defs(None)
    result = run_sensor(defs, resources={
        "s3_obs": FakeS3Observer("42-2026-08-05T10:00:00Z", object_count=42, total_size_bytes=1024000),
    })
    obs = _first_observation(result)
    assert obs.tags[DATA_VERSION_TAG] == "42-2026-08-05T10:00:00Z"
    assert obs.metadata["object_count"].value == 42
    assert obs.metadata["total_size_bytes"].value == 1024000
    assert "data_version" not in obs.metadata


def test_data_version_stable_when_no_new_objects():
    c = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="s3/lake", bucket_name="lake",
        resource_key="s3_obs",
    )
    defs = c.build_defs(None)
    obs1 = _first_observation(run_sensor(defs, resources={"s3_obs": FakeS3Observer("5-t1")}))
    obs2 = _first_observation(run_sensor(defs, resources={"s3_obs": FakeS3Observer("5-t1")}))
    assert obs1.tags[DATA_VERSION_TAG] == obs2.tags[DATA_VERSION_TAG]


def test_data_version_changes_on_new_objects():
    c = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="s3/lake", bucket_name="lake",
        resource_key="s3_obs",
    )
    defs = c.build_defs(None)
    obs1 = _first_observation(run_sensor(defs, resources={"s3_obs": FakeS3Observer("5-t1")}))
    obs2 = _first_observation(run_sensor(defs, resources={"s3_obs": FakeS3Observer("6-t2")}))
    assert obs1.tags[DATA_VERSION_TAG] != obs2.tags[DATA_VERSION_TAG]


def test_source_argument_includes_prefix():
    """Verify the observed source string is 'bucket/prefix' when prefix set."""
    captured = {}
    class Capturer:
        def observe(self, source):
            captured["src"] = source
            return {"data_version": "v"}
    c = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="s3/lake", bucket_name="my-bucket",
        prefix="incoming/", resource_key="s3_obs",
    )
    defs = c.build_defs(None)
    run_sensor(defs, resources={"s3_obs": Capturer()})
    assert captured["src"] == "my-bucket/incoming/"


def test_source_argument_bare_bucket_when_no_prefix():
    captured = {}
    class Capturer:
        def observe(self, source):
            captured["src"] = source
            return {"data_version": "v"}
    c = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="s3/lake", bucket_name="just-bucket",
        resource_key="s3_obs",
    )
    defs = c.build_defs(None)
    run_sensor(defs, resources={"s3_obs": Capturer()})
    assert captured["src"] == "just-bucket"
