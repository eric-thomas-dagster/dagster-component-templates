"""Committed regression tests for OrbIngestionComponent.

The real HTTP calls to api.withorb.com are never made here. For the
pure-logic pieces (`_build_resources_config`, `_cursor_paginator`,
`_build_partitions_def`, `_resolve_destination*`) nothing is mocked at
all -- they're deterministic, I/O-free functions exercised directly.

For the end-to-end asset flow, the only things monkeypatched are the two
calls that would otherwise hit the real network / write a real local
DuckDB file: `dlt.pipeline(...)` and `dlt.sources.rest_api.rest_api_source`.
Everything else -- resource-list building, the Authorization/base_url
config shape, destination/non-SQL routing, DataFrame assembly, and
metadata emission -- runs for real.
"""
import dagster as dg
import dlt.sources.rest_api as rest_api_pkg
import pandas as pd
import pytest

from .conftest import FakePipeline, load_component_module, metadata_for, output_value


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def])


def _patch_pipeline(monkeypatch, mod, tables=None):
    """Monkeypatches `dlt.pipeline` to return a FakePipeline (no real
    DuckDB file, no real network). Returns the FakePipeline instances
    created, in call order, so tests can assert on construction kwargs."""
    created = []

    def _factory(**kwargs):
        p = FakePipeline(tables=tables, **kwargs)
        created.append(p)
        return p

    monkeypatch.setattr(mod.dlt, "pipeline", _factory)
    return created


def _patch_rest_api_source(monkeypatch):
    """Monkeypatches the real `dlt.sources.rest_api.rest_api_source`
    (imported by the asset body at call-time via `from dlt.sources.rest_api
    import rest_api_source`, so patching the module attribute takes
    effect). Returns the list of configs the asset passed in, in call
    order."""
    captured = []

    def _fake_source(config):
        captured.append(config)
        return object()

    monkeypatch.setattr(rest_api_pkg, "rest_api_source", _fake_source)
    return captured


# --- Pure logic: _build_resources_config ------------------------------------


def test_build_resources_config_default_pair(mod):
    built = mod._build_resources_config("customers,subscriptions")
    names = [r["name"] for r in built]
    assert names == ["customers", "subscriptions"]
    for r in built:
        assert r["endpoint"]["data_selector"] == "data"
        assert r["endpoint"]["paginator"] == {
            "type": "cursor",
            "cursor_path": "pagination_metadata.next_cursor",
            "cursor_param": "cursor",
        }
    assert built[0]["endpoint"]["path"] == "customers"
    assert built[1]["endpoint"]["path"] == "subscriptions"


def test_build_resources_config_all_three_in_order(mod):
    built = mod._build_resources_config("invoices,customers,subscriptions")
    # order follows the input string, not a fixed internal order
    assert [r["name"] for r in built] == ["invoices", "customers", "subscriptions"]
    assert built[0]["endpoint"]["path"] == "invoices"


def test_build_resources_config_unknown_resource_ignored(mod):
    built = mod._build_resources_config("customers,usage_events,bogus")
    assert [r["name"] for r in built] == ["customers"]


def test_build_resources_config_empty_string_yields_nothing(mod):
    assert mod._build_resources_config("") == []


def test_build_resources_config_whitespace_tolerant(mod):
    built = mod._build_resources_config(" customers , invoices ")
    assert [r["name"] for r in built] == ["customers", "invoices"]


def test_cursor_paginator_returns_fresh_dict_each_call(mod):
    a = mod._cursor_paginator()
    b = mod._cursor_paginator()
    assert a == b
    assert a is not b  # no shared mutable state across resource entries


# --- Pure logic: destination resolution -------------------------------------


def test_resolve_destination_defaults_to_duckdb(mod):
    component = mod.OrbIngestionComponent(asset_name="orb_ingestion", api_key="k")
    assert component._resolve_destination() == "duckdb"


def test_resolve_destination_passthrough_string_without_creds(mod):
    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="k", destination="snowflake"
    )
    assert component._resolve_destination() == "snowflake"


def test_resolve_staging_none_for_non_staged_destinations(mod):
    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="k", destination="snowflake"
    )
    assert component._resolve_staging() is None


def test_resolve_staging_filesystem_fallback_for_athena(mod):
    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="k", destination="athena"
    )
    assert component._resolve_staging() == "filesystem"


# --- Pure logic: partitions_def ----------------------------------------------


def test_partitions_def_none_when_unset(mod):
    assert mod._build_partitions_def(None, None, None, None, None) is None


def test_partitions_def_daily_requires_start(mod):
    with pytest.raises(ValueError, match="requires partition_start"):
        mod._build_partitions_def("daily", None, None, None, None)


def test_partitions_def_static_requires_values(mod):
    with pytest.raises(ValueError, match="requires partition_values"):
        mod._build_partitions_def("static", None, None, None, None)


def test_partitions_def_dynamic_requires_name(mod):
    with pytest.raises(ValueError, match="requires dynamic_partition_name"):
        mod._build_partitions_def("dynamic", None, None, None, None)


def test_partitions_def_dimensions_and_flat_conflict(mod):
    with pytest.raises(ValueError, match="not both"):
        mod._build_partitions_def("daily", "2024-01-01", None, None, [{"name": "x", "type": "static", "values": "a"}])


def test_partitions_def_daily_builds(mod):
    pd_def = mod._build_partitions_def("daily", "2024-01-01", None, None, None)
    assert isinstance(pd_def, dg.DailyPartitionsDefinition)


# --- End-to-end asset flow (dlt.pipeline + rest_api_source monkeypatched) ---


def test_full_materialize_builds_bearer_auth_config(mod, monkeypatch):
    tables = {
        "customers": pd.DataFrame({"id": ["cust_1", "cust_2"], "name": ["Acme", "Globex"]}),
        "subscriptions": pd.DataFrame({"id": ["sub_1"], "customer_id": ["cust_1"]}),
    }
    _patch_pipeline(monkeypatch, mod, tables=tables)
    captured_configs = _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="sk_test_123", resources="customers,subscriptions"
    )
    result = _materialize(component)
    assert result.success

    assert len(captured_configs) == 1
    config = captured_configs[0]
    assert config["client"]["base_url"] == "https://api.withorb.com/v1"
    assert config["client"]["auth"] == {"type": "bearer", "token": "sk_test_123"}
    assert [r["name"] for r in config["resources"]] == ["customers", "subscriptions"]


def test_full_materialize_duckdb_default_combines_dataframes(mod, monkeypatch):
    tables = {
        "customers": pd.DataFrame({"id": ["cust_1", "cust_2"]}),
        "subscriptions": pd.DataFrame({"id": ["sub_1"]}),
    }
    _patch_pipeline(monkeypatch, mod, tables=tables)
    _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(asset_name="orb_ingestion", api_key="k")
    result = _materialize(component)
    assert result.success

    df = output_value(result, "orb_ingestion")
    assert len(df) == 3  # 2 customers + 1 subscription
    assert set(df["_resource_type"]) == {"customers", "subscriptions"}

    meta = metadata_for(result, "orb_ingestion")
    assert meta["row_count"] == 3
    assert meta["destination"] == "duckdb (in-memory)"
    assert set(meta["resources_loaded"]) == {"customers", "subscriptions"}
    assert meta["rows_customers"] == 2
    assert meta["rows_subscriptions"] == 1


def test_persist_only_emits_materialize_result_no_dataframe(mod, monkeypatch):
    _patch_pipeline(monkeypatch, mod, tables={})
    _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="k", destination="snowflake", persist_only=True
    )
    result = _materialize(component)
    assert result.success

    meta = metadata_for(result, "orb_ingestion")
    assert meta["destination"] == "snowflake"
    # MaterializeResult path returns before ever calling sql_client -- no
    # row_count/preview keys should be present.
    assert "row_count" not in meta


def test_non_sql_destination_without_persist_only_emits_materialize_result(mod, monkeypatch):
    _patch_pipeline(monkeypatch, mod, tables={})
    _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="k", destination="filesystem", bucket_url="file:///tmp/orb"
    )
    result = _materialize(component)
    assert result.success

    meta = metadata_for(result, "orb_ingestion")
    assert meta["destination"] == "filesystem"
    assert "row_count" not in meta


def test_no_tables_found_returns_empty_dataframe(mod, monkeypatch):
    _patch_pipeline(monkeypatch, mod, tables={})
    _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(asset_name="orb_ingestion", api_key="k")
    result = _materialize(component)
    assert result.success

    df = output_value(result, "orb_ingestion")
    assert len(df) == 0

    meta = metadata_for(result, "orb_ingestion")
    assert "row_count" not in meta  # base_metadata only, same as chargify's empty-data path


def test_pipeline_dataset_name_defaults_to_asset_name(mod, monkeypatch):
    created = _patch_pipeline(monkeypatch, mod, tables={"customers": pd.DataFrame({"id": ["c1"]})})
    _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(asset_name="orb_ingestion", api_key="k")
    result = _materialize(component)
    assert result.success
    assert created[0].kwargs["dataset_name"] == "orb_ingestion"
    assert created[0].kwargs["pipeline_name"] == "orb_ingestion_pipeline"


def test_explicit_dataset_name_overrides_default(mod, monkeypatch):
    created = _patch_pipeline(monkeypatch, mod, tables={"customers": pd.DataFrame({"id": ["c1"]})})
    _patch_rest_api_source(monkeypatch)

    component = mod.OrbIngestionComponent(
        asset_name="orb_ingestion", api_key="k", dataset_name="orb_raw"
    )
    result = _materialize(component)
    assert result.success
    assert created[0].kwargs["dataset_name"] == "orb_raw"
