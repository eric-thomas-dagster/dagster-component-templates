"""resource_key tests for DorisRoutineLoadSensorComponent (the SQLAlchemy engine's result rows are
faked; the component runs via real direct sensor invocation)."""
import dagster as dg
import pytest

from .conftest import FakeDorisResource, load_component_module

ROWS = [{"Name": "orders_rl", "State": "RUNNING", "TableName": "orders"}]


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, **kw):
    return mod.DorisRoutineLoadSensorComponent(
        sensor_name="rl_health", database="analytics", job_name_target="alert_job", **kw
    )


def test_default_none_builds_inline_sqlalchemy_engine(mod, monkeypatch):
    monkeypatch.setenv("DORIS_FE_HOST", "inline-fe")
    monkeypatch.setenv("DORIS_USER", "inline-user")
    monkeypatch.setenv("DORIS_PASSWORD", "inline-pw")

    captured = {}
    import sqlalchemy
    real_create_engine = sqlalchemy.create_engine

    def fake_create_engine(conn_str, *a, **kw):
        captured["conn_str"] = conn_str
        from .conftest import FakeEngine
        return FakeEngine(ROWS)

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)

    comp = _comp(mod)
    assert comp.resource_key is None
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == set()

    ctx = dg.build_sensor_context()
    result = sensor_def(ctx)
    assert captured["conn_str"] == "mysql+pymysql://inline-user:inline-pw@inline-fe:9030/analytics"
    assert isinstance(result, dg.SensorResult)


def test_resource_path_uses_resource_and_ignores_inline(mod, monkeypatch):
    for k in ("DORIS_FE_HOST", "DORIS_USER", "DORIS_PASSWORD"):
        monkeypatch.delenv(k, raising=False)

    fake_resource = FakeDorisResource(ROWS)
    comp = _comp(mod, resource_key="doris_shared")
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == {"doris_shared"}

    ctx = dg.build_sensor_context(resources={"doris_shared": fake_resource})
    result = sensor_def(ctx)
    assert fake_resource.get_engine_calls == 1
    assert isinstance(result, dg.SensorResult)
    # First tick with no prior cursor: state changes are reported (no previous state to diff against).
    assert result.asset_events or result.run_requests or result.cursor
