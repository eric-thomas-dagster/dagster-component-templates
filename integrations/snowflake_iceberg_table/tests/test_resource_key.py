"""Committed tests for SnowflakeIcebergTableComponent's resource_key field.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection (see conftest.sf). The resource path uses a fake standing in for
dagster_snowflake.SnowflakeResource that exposes a real get_connection() context manager
(see conftest.FakeSnowflakeResource); the component runs for real under dg.materialize().
"""
import dagster as dg

from .conftest import FakeSnowflakeResource


def _comp(mod, **overrides):
    kwargs = dict(
        asset_name="iceberg_tbl",
        account="acct",
        user="user",
        database="DB",
        schema_name="SC",
        table_name="TBL",
        external_volume="vol",
        base_location="iceberg/tbl/",
        password="pw",
    )
    kwargs.update(overrides)
    return mod.SnowflakeIcebergTableComponent(**kwargs)


def test_resource_key_default_none_inline_path_and_closes_connection(mod, sf):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([asset_def])
    assert result.success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "acct"
    assert conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "pw"
    assert conn.close_calls == 1
    assert any("CREATE OR REPLACE ICEBERG TABLE DB.SC.TBL" in e for e in conn.executed)


def test_resource_key_set_required_and_uses_get_connection(mod, sf):
    asset_def = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"snow"}
    res = FakeSnowflakeResource(row_count=5)
    result = dg.materialize([asset_def], resources={"snow": res})
    assert result.success
    assert res.get_connection_calls == 1
    assert sf.conns == []  # inline snowflake.connector.connect never used
    assert res.conn.close_calls >= 1
    mats = result.get_asset_materialization_events()
    assert len(mats) == 1
    md = mats[0].materialization.metadata
    assert md["snowflake/row_count"].value == 5


def test_resource_key_not_registered_fails_loudly(mod, sf):
    asset_def = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    import pytest
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset_def])
