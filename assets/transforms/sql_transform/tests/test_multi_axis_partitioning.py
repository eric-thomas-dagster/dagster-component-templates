"""Real, committed tests for a gap reported building a demo: `partition_type:
'multi'` always named its static axis 'static_dim', completely ignoring the
`partition_static_dim` field that exists specifically to let a user name it
(e.g. 'customer') -- the field was declared and documented but silently
discarded, since `_build_partitions_def` was called with the static axis name
hardcoded. There was also no way to use the general `partition_dimensions`
(multi-axis, named-per-dimension) shape at all -- the call site passed `None`
for it unconditionally.

Fixed: `_build_partitions_def` now takes an (optional) `static_dim_name`
keyword, threaded from `self.partition_static_dim`; a new `partition_dimensions`
field is threaded through too, for full control over axis names beyond what
partition_type='multi' can express (it always calls its two axes 'date' and
'static_dim'/partition_static_dim).
"""
import dagster as dg

from .conftest import load_component_module


def _comp(mod, **kw):
    args = dict(
        asset_name="out_asset",
        connection_url_env_var="TEST_MULTI_AXIS_URL",
        return_dataframe=True,
        sql="SELECT 1 AS x",
    )
    args.update(kw)
    return mod.SqlTransformComponent(**args)


def test_connection_url_env_var_has_a_default_for_demo_seams():
    """No kwargs needed beyond asset_name/sql -- this used to raise a pydantic
    validation error for a missing required field."""
    mod = load_component_module()
    comp = mod.SqlTransformComponent(asset_name="out_asset", sql="SELECT 1 AS x")
    assert comp.connection_url_env_var == "DATABASE_URL"


def test_partition_static_dim_names_the_axis_instead_of_the_hardcoded_default():
    mod = load_component_module()
    comp = _comp(
        mod,
        partition_type="multi",
        partition_start="2024-01-01",
        partition_values="acme,globex",
        partition_static_dim="customer",
    )
    assets = comp.build_defs(None).assets
    pdef = assets[0].partitions_def
    assert isinstance(pdef, dg.MultiPartitionsDefinition)
    dim_names = {d.name for d in pdef.partitions_defs}
    assert dim_names == {"date", "customer"}
    assert "static_dim" not in dim_names


def test_without_partition_static_dim_falls_back_to_the_historical_default():
    """Backward compatible: omitting partition_static_dim keeps the old name."""
    mod = load_component_module()
    comp = _comp(
        mod,
        partition_type="multi",
        partition_start="2024-01-01",
        partition_values="acme,globex",
    )
    assets = comp.build_defs(None).assets
    pdef = assets[0].partitions_def
    dim_names = {d.name for d in pdef.partitions_defs}
    assert dim_names == {"date", "static_dim"}


def test_partition_dimensions_multi_axis_shape_is_now_usable():
    mod = load_component_module()
    comp = _comp(
        mod,
        partition_dimensions=[
            {"name": "date", "type": "daily", "start": "2024-01-01"},
            {"name": "region", "type": "static", "values": "us,eu"},
        ],
    )
    assets = comp.build_defs(None).assets
    pdef = assets[0].partitions_def
    assert isinstance(pdef, dg.MultiPartitionsDefinition)
    dim_names = {d.name for d in pdef.partitions_defs}
    assert dim_names == {"date", "region"}
