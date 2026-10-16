"""resource_key tests for DataframeToSnowflakeBulkComponent.

Only snowflake.connector.connect is faked. The component runs under real dg.materialize(),
real parquet chunks are written, and the resource path uses the real
dagster_snowflake.SnowflakeResource.get_connection().
"""
import re

import dagster as dg
import pandas as pd
import pytest
from dagster_snowflake import SnowflakeResource

from .conftest import install_fake_snowflake_connect, load_component_module

DF = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")


@pytest.fixture()
def mod():
    return load_component_module()


def _up():
    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return DF.copy()
    return src


def _comp(mod, **kw):
    return mod.DataframeToSnowflakeBulkComponent(
        asset_name="load", upstream_asset_key="src", table="ORDERS", database="DB",
        schema="PUBLIC", **kw)


def _norm(statements):
    """Stage names carry a random uuid and PUT paths a temp dir; mask them for comparison."""
    out = []
    for s in statements:
        s = re.sub(r"@~/orders_[0-9a-f]{12}", "@~/orders_<id>", s)
        s = re.sub(r"file://\S+", "file://<tmp>", s)
        s = re.sub(r"ORDERS_STG_[0-9A-F]{8}", "ORDERS_STG_<id>", s)
        out.append(s)
    return out


def _resource():
    return SnowflakeResource(account="res-acct", user="res-user", password="res-pw")


def test_default_none_uses_inline_env(mod, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "inline-acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "inline-user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "inline-pw")
    conns = install_fake_snowflake_connect(monkeypatch)
    comp = _comp(mod)
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    r = dg.materialize([_up(), sink])
    assert r.success
    (c,) = conns
    assert c.kwargs["account"] == "inline-acct" and c.kwargs["user"] == "inline-user"
    assert c.kwargs["password"] == "inline-pw" and c.kwargs["database"] == "DB"
    assert c.close_count >= 1 and all(cur.closed for cur in c.cursors)
    s = c.statements
    # default mode is "replace": PUT, DROP, CREATE ... USING TEMPLATE, COPY INTO
    assert s[0].startswith("PUT file://") and s[1] == "DROP TABLE IF EXISTS DB.PUBLIC.ORDERS"
    assert "CREATE TABLE IF NOT EXISTS DB.PUBLIC.ORDERS" in s[2]
    assert s[3].startswith("COPY INTO DB.PUBLIC.ORDERS")
    # purge_stage defaults True -> COPY ... PURGE = TRUE is last, no separate REMOVE
    assert s[-1] == s[3] and "PURGE = TRUE" in s[3]
    assert r.asset_materializations_for_node("load")[0].metadata["row_count"].value == 3


def test_resource_path_same_statements_and_closes_connection(mod, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "a")
    monkeypatch.setenv("SNOWFLAKE_USER", "u")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
    conns = install_fake_snowflake_connect(monkeypatch)
    assert dg.materialize([_up(), _comp(mod).build_defs(None).assets[0]]).success
    inline = conns[0]

    for k in ENV:
        monkeypatch.delenv(k)
    sink = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"snow"}
    r = dg.materialize([_up(), sink], resources={"snow": _resource()})
    assert r.success
    assert len(conns) == 2
    via_res = conns[1]
    # connection came from the registered SnowflakeResource, not the env vars
    assert via_res.kwargs["account"] == "res-acct" and via_res.kwargs["user"] == "res-user"
    assert via_res.kwargs["password"] == "res-pw"
    assert _norm(via_res.statements) == _norm(inline.statements)
    assert via_res.close_count >= 1 and all(cur.closed for cur in via_res.cursors)
    assert r.asset_materializations_for_node("load")[0].metadata["row_count"].value == 3


def test_resource_path_merge_mode(mod, monkeypatch):
    for k in ENV:
        monkeypatch.delenv(k, raising=False)
    conns = install_fake_snowflake_connect(monkeypatch)
    sink = _comp(mod, resource_key="snow", mode="merge", primary_key=["id"]).build_defs(None).assets[0]
    assert dg.materialize([_up(), sink], resources={"snow": _resource()}).success
    (c,) = conns
    assert any(s.startswith("MERGE INTO DB.PUBLIC.ORDERS t USING") for s in c.statements)
    assert any("ON t.ID = s.ID" in s and "t.NAME = s.NAME" in s for s in c.statements)


def test_resource_path_closes_connection_on_failure(mod, monkeypatch):
    for k in ENV:
        monkeypatch.delenv(k, raising=False)
    conns = install_fake_snowflake_connect(monkeypatch)
    sink = _comp(mod, resource_key="snow").build_defs(None).assets[0]

    import snowflake.connector
    orig = snowflake.connector.connect

    def connect(**kw):
        c = orig(**kw)
        real_cursor = c.cursor

        def cursor():
            cur = real_cursor()
            real_exec = cur.execute

            def boom(sql, *a, **k):
                if sql.startswith("COPY INTO"):
                    raise RuntimeError("copy exploded")
                return real_exec(sql, *a, **k)
            cur.execute = boom
            return cur
        c.cursor = cursor
        return c

    monkeypatch.setattr(snowflake.connector, "connect", connect)
    with pytest.raises(Exception, match="copy exploded"):
        dg.materialize([_up(), sink], resources={"snow": _resource()})
    assert conns[0].close_count >= 1


def test_unregistered_resource_key_fails_loudly(mod, monkeypatch):
    install_fake_snowflake_connect(monkeypatch)
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_up(), sink])


def test_inline_without_env_still_fails(mod, monkeypatch):
    for k in ENV:
        monkeypatch.delenv(k, raising=False)
    install_fake_snowflake_connect(monkeypatch)
    with pytest.raises(Exception, match="SNOWFLAKE_ACCOUNT"):
        dg.materialize([_up(), _comp(mod).build_defs(None).assets[0]])


def test_partitions_def_is_none_confirmed(mod):
    assert _comp(mod).build_defs(None).assets[0].partitions_def is None
