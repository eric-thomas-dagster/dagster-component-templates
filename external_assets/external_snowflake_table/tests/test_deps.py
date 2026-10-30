"""Real gap reported against this registry (dagster-demo-factory component
feedback, 2026-10-30): ExternalSnowflakeTableAsset had no `deps` field, so an
external table couldn't be declared downstream of whatever feeds it (e.g. a
BigQuery/GCS export job) without a `post_processing` `deps:` workaround.
"""
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()
ExternalSnowflakeTableAsset = mod.ExternalSnowflakeTableAsset


def _component(**overrides):
    attrs = dict(
        asset_key="snowflake/raw/orders",
        account="myorg-us-east-1",
        database="RAW",
        schema_name="PUBLIC",
        table_name="ORDERS",
    )
    attrs.update(overrides)
    return ExternalSnowflakeTableAsset(**attrs)


def test_deps_accepts_a_list_and_reaches_the_asset_spec():
    comp = _component(deps=["exports/orders_parquet"])
    defs = comp.build_defs(None)
    spec = next(iter(defs.assets))
    assert dg.AssetKey.from_user_string("exports/orders_parquet") in {d.asset_key for d in spec.deps}


def test_deps_accepts_a_comma_separated_string():
    comp = _component(deps="exports/orders_parquet, exports/customers_parquet")
    defs = comp.build_defs(None)
    spec = next(iter(defs.assets))
    dep_keys = {d.asset_key for d in spec.deps}
    assert dg.AssetKey.from_user_string("exports/orders_parquet") in dep_keys
    assert dg.AssetKey.from_user_string("exports/customers_parquet") in dep_keys


def test_no_deps_configured_means_no_deps_on_the_spec():
    comp = _component()
    defs = comp.build_defs(None)
    spec = next(iter(defs.assets))
    assert list(spec.deps) == []
