"""resource_key tests for DorisQueryAssetComponent (real in-memory SQLite engine stands in for Doris)."""
import dagster as dg
import pytest
import sqlalchemy

from .conftest import FakeDorisResource, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, **kw):
    return mod.DorisQueryAssetComponent(asset_name="doris_q", query="SELECT 1 AS id", **kw)


def test_default_none_uses_inline_fields(mod, monkeypatch):
    monkeypatch.setenv("DORIS_FE_HOST", "inline-fe")
    monkeypatch.setenv("DORIS_USER", "inline-user")
    monkeypatch.setenv("DORIS_PASSWORD", "inline-pw")

    captured = {}
    real_create_engine = sqlalchemy.create_engine

    def fake_create_engine(conn_str, *a, **kw):
        captured["conn_str"] = conn_str
        return real_create_engine("sqlite:///:memory:")

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)

    comp = _comp(mod, database="analytics")
    assert comp.resource_key is None
    asset = comp.build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == set()

    result = dg.materialize([asset])
    assert result.success
    assert captured["conn_str"] == "mysql+pymysql://inline-user:inline-pw@inline-fe:9030/analytics"
    df = result.output_for_node("doris_q")
    assert df["id"].tolist() == [1]


def test_resource_path_uses_resource_and_ignores_inline(mod, monkeypatch):
    for k in ("DORIS_FE_HOST", "DORIS_USER", "DORIS_PASSWORD"):
        monkeypatch.delenv(k, raising=False)

    shared_engine = sqlalchemy.create_engine("sqlite:///:memory:")
    fake_resource = FakeDorisResource(shared_engine)

    comp = _comp(mod, resource_key="doris_shared")
    asset = comp.build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == {"doris_shared"}

    result = dg.materialize([asset], resources={"doris_shared": fake_resource})
    assert result.success
    assert fake_resource.get_engine_calls == 1
    df = result.output_for_node("doris_q")
    assert df["id"].tolist() == [1]


def test_unregistered_resource_key_fails_loudly(mod):
    comp = _comp(mod, resource_key="missing_key")
    asset = comp.build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset])


def test_inline_missing_creds_still_raises(mod, monkeypatch):
    for k in ("DORIS_FE_HOST", "DORIS_USER", "DORIS_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    comp = _comp(mod)
    asset = comp.build_defs(None).assets[0]
    with pytest.raises(Exception, match="Doris connection missing"):
        dg.materialize([asset])
