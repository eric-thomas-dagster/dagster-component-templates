"""Committed tests for SnowflakeTimeTravelAssetComponent's resource_key retrofit.

Only the Snowflake wire boundary is faked (`snowflake.connector.connect`); the component
(and, on the resource path, the real dagster_snowflake.SnowflakeResource) runs for real
under dg.materialize().
"""
import warnings

import dagster as dg
import pytest
import snowflake.connector
from dagster_snowflake import SnowflakeResource

from .conftest import FakeConn, load_component_module

# pandas warns that a plain DBAPI2 connection (our FakeConn) "is not tested"; expected and benign.
pytestmark = pytest.mark.filterwarnings(
    "ignore:pandas only supports SQLAlchemy connectable.*:UserWarning"
)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    conns = []

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        conns.append(c)
        return c

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    return conns


def _comp(mod, **kw):
    kw.setdefault("asset_name", "tt_asset")
    kw.setdefault("account", "acct")
    kw.setdefault("user", "user")
    kw.setdefault("database", "DB")
    kw.setdefault("schema_name", "SC")
    kw.setdefault("source_table", "ORDERS")
    kw.setdefault("time_travel_offset_seconds", -3600)
    return mod.SnowflakeTimeTravelAssetComponent(**kw)


# ------------------------------------------------------------------ inline path (existing behavior)

def test_resource_key_default_none_uses_inline_password_and_closes_connection(mod, sf):
    comp = _comp(mod, password="pw")
    tt_asset = comp.build_defs(None).assets[0]
    assert tt_asset.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([tt_asset])
    assert result.success
    (conn,) = sf
    assert conn.kwargs["account"] == "acct" and conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "pw"
    assert conn.close_calls == 1
    assert "AT(OFFSET => -3600)" in conn.executed[0]


def test_inline_path_missing_auth_raises(mod, sf):
    comp = _comp(mod)  # no password, no authenticator
    tt_asset = comp.build_defs(None).assets[0]
    with pytest.raises(Exception, match="must set password OR authenticator"):
        dg.materialize([tt_asset])


# ------------------------------------------------------------------ resource_key path

def test_resource_key_wires_required_resource_keys(mod, sf):
    comp = _comp(mod, resource_key="snow")
    tt_asset = comp.build_defs(None).assets[0]
    assert tt_asset.required_resource_keys - {"io_manager"} == {"snow"}


def test_resource_path_uses_real_snowflake_resource_and_ignores_inline_fields(mod, sf):
    # Deliberately leave password unset -- if the inline path were used by mistake,
    # _connect() would raise "must set password OR authenticator+key/token".
    comp = _comp(mod, resource_key="snow", password=None)
    tt_asset = comp.build_defs(None).assets[0]
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    result = dg.materialize([tt_asset], resources={"snow": res})
    assert result.success
    (conn,) = sf
    assert conn.kwargs["account"] == "res_acct" and conn.kwargs["user"] == "res_user"
    assert conn.close_calls >= 1  # closed by the component (and again by the resource's own exit)


def test_resource_key_not_registered_fails_loudly(mod, sf):
    comp = _comp(mod, resource_key="missing_key")
    tt_asset = comp.build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([tt_asset])
