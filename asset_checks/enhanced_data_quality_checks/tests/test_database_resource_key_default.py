"""Real gap reported against this registry (dagster-demo-factory component
feedback, 2026-10-30): the top-level `database_resource_key` field on
`EnhancedDataQualityChecks` was purely declarative -- `_create_asset_component`
only ever read `asset_config.get('database_resource_key')` (the PER-ASSET
value), so a project with many assets had to repeat `database_resource_key`
on every single asset block even though the component-level field looked like
it should cover exactly that case.

Worse than just repetition: an asset block that omitted its own
`database_resource_key` (relying on the top-level default) silently fell back
to `data_source_type: "dataframe"` instead of "database", since the
data-source-type inference only checked the per-asset value too.

Fixed by resolving `asset_config.get('database_resource_key') or
self.database_resource_key` once, and using that resolved value both for the
data-source-type inference and for populating the per-asset component data.
"""
import importlib.util
import pathlib

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc_db_resource_key_default", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


def test_asset_block_without_database_resource_key_inherits_the_component_default():
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        row_count_check=[{"min_rows": 1}],
    )
    temp = c._create_asset_component("orders", {"row_count_check": [{"min_rows": 1}]})
    assert temp.database_resource_key == "duckdb"
    assert temp.data_source_type == "database"


def test_asset_block_can_still_override_the_component_default():
    c = EnhancedDataQualityChecks(
        database_resource_key="duckdb",
        row_count_check=[{"min_rows": 1}],
    )
    temp = c._create_asset_component(
        "special_table",
        {"database_resource_key": "snowflake", "row_count_check": [{"min_rows": 1}]},
    )
    assert temp.database_resource_key == "snowflake"
    assert temp.data_source_type == "database"


def test_no_component_default_and_no_per_asset_key_leaves_database_resource_key_unset():
    """Guards against the fallback accidentally inventing a resource key out
    of nowhere when neither the component nor the asset block configured one."""
    c = EnhancedDataQualityChecks(row_count_check=[{"min_rows": 1}])
    temp = c._create_asset_component("in_memory_df", {"row_count_check": [{"min_rows": 1}]})
    assert temp.database_resource_key is None
