"""Real, committed test for a gap found against the ADF-converter feedback: SQLToDatabaseAssetComponent
had no dry_run flag, unlike the SSIS/Synapse/SAP job-trigger components -- "so we wrap it" per the
original ask. Confirms dry_run skips the real database connections entirely (both source_url_env_var/
destination_url_env_var are deliberately left unset here -- a real run would raise immediately)."""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_dry_run_skips_the_real_connection_and_query(mod):
    component = mod.SQLToDatabaseAssetComponent(
        asset_name="orders_sync",
        destination_table="raw_orders",
        source_table="orders",
        dry_run=True,
        # deliberately no source_url_env_var/destination_url_env_var/source_resource_key/
        # destination_resource_key set -- a real (non-dry_run) run would raise at
        # _resolve_db_url before ever reaching a connection attempt.
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    assert events[0].materialization.metadata["status"].value == "dry_run — not executed"
