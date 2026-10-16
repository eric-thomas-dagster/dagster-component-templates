"""resource_key tests for DataframeToStarRocksComponent (only requests.put is faked; component runs under real dg.materialize)."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeFEResource, install_fake_requests_put, load_component_module

DF = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
EXPECTED_CSV = "1,a\n2,b\n3,c\n"
ENV = ("STARROCKS_FE_HOST", "STARROCKS_USER", "STARROCKS_PASSWORD")


@pytest.fixture()
def mod():
    return load_component_module()


def _up():
    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return DF.copy()
    return src


def _comp(mod, **kw):
    return mod.DataframeToStarRocksComponent(asset_name="load", upstream_asset_key="src", table="orders",
                       database="analytics", **kw)


def test_default_none_uses_inline_env_and_fields(mod, monkeypatch):
    monkeypatch.setenv(ENV[0], "inline-fe")
    monkeypatch.setenv(ENV[1], "inline-user")
    monkeypatch.setenv(ENV[2], "inline-pw")
    calls = install_fake_requests_put(monkeypatch)
    comp = _comp(mod, ssl=True, http_port=8443)
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_up(), sink]).success
    (c,) = calls
    assert c["url"] == "https://inline-fe:8443/api/analytics/orders/_stream_load"
    assert c["auth"] == ("inline-user", "inline-pw")
    assert c["data"].decode() == EXPECTED_CSV


def test_resource_path_uses_resource_and_ignores_inline(mod, monkeypatch):
    for k in ENV:
        monkeypatch.delenv(k, raising=False)
    calls = install_fake_requests_put(monkeypatch)
    # Inline ssl/http_port fields deliberately differ from the resource's: resource must win.
    comp = _comp(mod, resource_key="fe_shared", ssl=False, http_port=1111)
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"fe_shared"}
    res = FakeFEResource("res-fe.example", "res-user", "res-pw", ssl=True, http_port=8443)
    r = dg.materialize([_up(), sink], resources={"fe_shared": res})
    assert r.success
    (c,) = calls
    assert c["url"] == "https://res-fe.example:8443/api/analytics/orders/_stream_load"
    assert c["auth"] == ("res-user", "res-pw")
    assert c["data"].decode() == EXPECTED_CSV
    assert c["headers"]["columns"] == "id,name"
    md = r.asset_materializations_for_node("load")[0].metadata
    assert md["starrocks/loaded_rows"].value == 3
    assert md["starrocks_table"].value == "analytics.orders"


def test_resource_and_inline_send_same_payload(mod, monkeypatch):
    monkeypatch.setenv(ENV[0], "h")
    monkeypatch.setenv(ENV[1], "u")
    monkeypatch.setenv(ENV[2], "p")
    calls = install_fake_requests_put(monkeypatch)
    assert dg.materialize([_up(), _comp(mod).build_defs(None).assets[0]]).success
    sink_r = _comp(mod, resource_key="k").build_defs(None).assets[0]
    assert dg.materialize([_up(), sink_r], resources={"k": FakeFEResource("h", "u", "p")}).success
    a, b = calls
    assert a["url"] == b["url"] == "http://h:8030/api/analytics/orders/_stream_load"
    assert a["data"] == b["data"]
    assert a["auth"] == b["auth"]
    assert {k: v for k, v in a["headers"].items() if k != "label"} == \
           {k: v for k, v in b["headers"].items() if k != "label"}


def test_unregistered_resource_key_fails_loudly(mod, monkeypatch):
    install_fake_requests_put(monkeypatch)
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_up(), sink])


def test_inline_missing_creds_still_raises(mod, monkeypatch):
    for k in ENV:
        monkeypatch.delenv(k, raising=False)
    install_fake_requests_put(monkeypatch)
    with pytest.raises(Exception, match="creds missing"):
        dg.materialize([_up(), _comp(mod).build_defs(None).assets[0]])


def test_partitions_def_is_none_confirmed(mod):
    assert _comp(mod).build_defs(None).assets[0].partitions_def is None
