"""resource_key tests for DataframeToClickHouseComponent (only clickhouse_connect is faked)."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import (FakeClickHouseClient, FakeClickHouseResource,
                       install_fake_clickhouse_connect, load_component_module)

DF = pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})


@pytest.fixture()
def mod():
    return load_component_module()


def _up():
    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return DF.copy()
    return src


def _comp(mod, **kw):
    return mod.DataframeToClickHouseComponent(
        asset_name="to_ch", upstream_asset_key="src", table="events", database="analytics", **kw)


def test_default_none_uses_inline_env(mod, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "inline-host")
    monkeypatch.setenv("CLICKHOUSE_USER", "inline-user")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "inline-pw")
    clients = install_fake_clickhouse_connect(monkeypatch)
    comp = _comp(mod)
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_up(), sink]).success
    (c,) = clients
    assert c.kwargs["host"] == "inline-host" and c.kwargs["username"] == "inline-user"
    assert c.kwargs["password"] == "inline-pw" and c.kwargs["database"] == "analytics"
    (ins,) = c.inserts
    assert ins["table"] == "events" and ins["database"] == "analytics"
    pd.testing.assert_frame_equal(ins["df"], DF)


def test_resource_path_same_insert_and_no_inline_client(mod, monkeypatch):
    for k in ("CLICKHOUSE_HOST", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    inline_clients = install_fake_clickhouse_connect(monkeypatch)
    shared = FakeClickHouseClient(host="resource-host")
    sink = _comp(mod, resource_key="ch_shared").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"ch_shared"}
    r = dg.materialize([_up(), sink], resources={"ch_shared": FakeClickHouseResource(shared)})
    assert r.success
    assert inline_clients == []  # inline get_client never called
    (ins,) = shared.inserts
    assert ins["table"] == "events" and ins["database"] == "analytics"
    pd.testing.assert_frame_equal(ins["df"], DF)
    md = r.asset_materializations_for_node("to_ch")[0].metadata
    assert md["row_count"].value == 3 and md["clickhouse_table"].value == "analytics.events"


def test_unregistered_resource_key_fails_loudly(mod, monkeypatch):
    install_fake_clickhouse_connect(monkeypatch)
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_up(), sink])


def test_partitions_def_is_none_confirmed(mod):
    sink = _comp(mod).build_defs(None).assets[0]
    assert sink.partitions_def is None
