"""SQL observation sensor — DataVersion + resource_key contract."""
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from .conftest import load_sql, run_sensor


sql = load_sql()
SqlObservationSensorComponent = sql.SqlObservationSensorComponent


class FakeSqlObserver:
    def __init__(self, data_version: str, **extra):
        self._dv = data_version
        self._extra = extra

    def observe(self, table_name: str):
        return {"data_version": self._dv, "table_name": table_name, **self._extra}


def _first_observation(result):
    assert result.asset_events, f"no asset events; skip_reason={result.skip_reason!r}"
    return result.asset_events[0]


def test_data_version_via_resource():
    c = SqlObservationSensorComponent(
        sensor_name="s1", asset_key="postgres/public/orders", table_name="orders",
        resource_key="sql_obs",
    )
    defs = c.build_defs(None)
    result = run_sensor(defs, resources={
        "sql_obs": FakeSqlObserver("1234", row_count=1234, latest_watermark="2026-08-05T10:00:00"),
    })
    obs = _first_observation(result)
    assert obs.tags[DATA_VERSION_TAG] == "1234"
    assert obs.metadata["row_count"].value == 1234
    assert obs.metadata["latest_watermark"].value == "2026-08-05T10:00:00"
    assert "data_version" not in obs.metadata


def test_data_version_stable_when_no_new_rows():
    c = SqlObservationSensorComponent(
        sensor_name="s1", asset_key="postgres/public/orders", table_name="orders",
        resource_key="sql_obs",
    )
    defs = c.build_defs(None)
    obs1 = _first_observation(run_sensor(defs, resources={"sql_obs": FakeSqlObserver("100")}))
    obs2 = _first_observation(run_sensor(defs, resources={"sql_obs": FakeSqlObserver("100")}))
    assert obs1.tags[DATA_VERSION_TAG] == obs2.tags[DATA_VERSION_TAG]


def test_data_version_changes_on_new_rows():
    c = SqlObservationSensorComponent(
        sensor_name="s1", asset_key="postgres/public/orders", table_name="orders",
        resource_key="sql_obs",
    )
    defs = c.build_defs(None)
    obs1 = _first_observation(run_sensor(defs, resources={"sql_obs": FakeSqlObserver("100")}))
    obs2 = _first_observation(run_sensor(defs, resources={"sql_obs": FakeSqlObserver("101")}))
    assert obs1.tags[DATA_VERSION_TAG] != obs2.tags[DATA_VERSION_TAG]


def test_asset_key_slash_separated():
    from dagster import AssetKey
    c = SqlObservationSensorComponent(
        sensor_name="s1", asset_key="postgres/public/orders", table_name="orders",
        resource_key="sql_obs",
    )
    defs = c.build_defs(None)
    obs = _first_observation(run_sensor(defs, resources={"sql_obs": FakeSqlObserver("v1")}))
    assert obs.asset_key == AssetKey.from_user_string("postgres/public/orders")
    assert obs.asset_key.path == ["postgres", "public", "orders"]
