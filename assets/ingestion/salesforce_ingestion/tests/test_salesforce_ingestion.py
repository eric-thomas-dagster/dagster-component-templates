"""Committed tests for SalesforceIngestionComponent's cosmetic-partitioning fix +
backfill_policy support, and for the where_clause/incremental_field + real-bug-fix work below.

Real pre-existing bug (shared by 23 other ingestion components, see
airtable_ingestion's tests for the full rationale): partition_type could be
configured and partition_date_column/partition_static_column were declared
and documented ("Column used to filter the upstream DataFrame to the
current date/static partition value"), but nothing in the asset body ever
actually filtered anything -- every partition silently returned the
identical full dataset. Fixed via the shared _filter_df_to_partition()
helper, applied right before the final Output(value=...).

A second, separate real pre-existing bug (found while adding where_clause/
incremental_field): the component called dlt's verified `salesforce_source()`
as `salesforce_source(user_name=..., password=..., security_token=...)`, but
that function's real signature (confirmed against the actual vendored
source) only accepts `credentials=`/`session=` -- every real run would have
raised TypeError immediately. Never caught because this component's own
tests faked the whole dlt.sources.salesforce boundary away. Fixed by
building Salesforce dlt resources directly against simple_salesforce
(see component.py's "inline Salesforce dlt source" block) instead of
calling the vendored verified source at all -- tests below exercise the
real SOQL-building/incremental/quoting logic this enables, mocking only
simple_salesforce's Salesforce client (the paid/external-API boundary).

Fakes the dlt pipeline run + sql_client() query-back wholesale (this
repo's "mock only the paid call" convention, see conftest.py) -- resource
dispatch, DataFrame concatenation, partition filtering, and preview
metadata are all exercised for real through dg.materialize().
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    DEFAULT_DESCRIBE_FIELDS,
    FakeSalesforce,
    install_fake_dlt,
    install_fake_simple_salesforce,
    load_component_module,
)


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        username="u",
        password="p",
        security_token="t",
        sf_objects=["Account"],
    )
    kwargs.update(overrides)
    return mod.SalesforceIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_filters_to_one_days_rows(mod, monkeypatch):
    table = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "created_at": [
            "2024-01-02T10:00:00", "2024-01-02T18:00:00",
            "2024-01-03T10:00:00",
            "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00",
        ],
    })
    install_fake_dlt(monkeypatch, mod, table_rows={"account": table})
    component = _make_component(
        mod,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("test_asset")
    assert sorted(df["id"].tolist()) == [1, 2]


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_filters_to_the_requested_range_only(mod, monkeypatch):
    """Real bug before the fix: partition_date_column was declared and
    documented but never used -- every partition (and every backfill
    range) silently returned the IDENTICAL full table. Confirmed fixed: a
    2-day single_run backfill (2024-01-02..2024-01-03) now excludes the
    2024-01-04 rows that fall outside the requested range."""
    table = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "created_at": [
            "2024-01-02T10:00:00", "2024-01-02T18:00:00",
            "2024-01-03T10:00:00",
            "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00",
        ],
    })
    install_fake_dlt(monkeypatch, mod, table_rows={"account": table})
    component = _make_component(
        mod,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    df = result.output_for_node("test_asset")
    assert sorted(df["id"].tolist()) == [1, 2, 3]


def test_partitioned_without_filter_column_configured_is_unfiltered(mod, monkeypatch):
    """Backward-compat: partition_date_column/partition_static_column are
    optional. Without one configured, a partitioned asset still returns
    the full table (pre-existing documented behavior for users who only
    wanted scheduling cadence, not filtering)."""
    table = pd.DataFrame({"id": [1, 2, 3], "created_at": ["2024-01-02", "2024-01-03", "2024-01-04"]})
    install_fake_dlt(monkeypatch, mod, table_rows={"account": table})
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


# ── where_clause / incremental_field (real SOQL-generation tests) ──────────────────
#
# These call mod._build_salesforce_resource(...) directly and iterate the returned
# DltResource (list(resource())) to exercise the real generator body -- pipeline.run()
# is faked everywhere else in this file and never actually iterates a resource, so
# materializing the asset alone cannot exercise this logic.

def test_default_full_reload_no_where_no_incremental(mod, monkeypatch):
    fake_client = FakeSalesforce(
        describe_by_object={"Account": DEFAULT_DESCRIBE_FIELDS},
        bulk_pages_by_object={"Account": [[{"Id": "1", "attributes": {}}]]},
    )
    install_fake_simple_salesforce(monkeypatch, fake_client)
    resource = mod._build_salesforce_resource(fake_client, "Account", "account", None, None)
    assert resource.write_disposition == "replace"
    rows = list(resource())
    assert rows == [{"Id": "1"}]  # "attributes" stripped
    assert fake_client.queries == ["SELECT Id, Name, SystemModstamp, BillingStreet FROM Account"]


def test_where_clause_only_no_incremental(mod, monkeypatch):
    fake_client = FakeSalesforce(
        describe_by_object={"Account": DEFAULT_DESCRIBE_FIELDS},
        bulk_pages_by_object={"Account": []},
    )
    install_fake_simple_salesforce(monkeypatch, fake_client)
    resource = mod._build_salesforce_resource(
        fake_client, "Account", "account", None, "Type = 'Customer'",
    )
    list(resource())
    assert fake_client.queries == [
        "SELECT Id, Name, SystemModstamp, BillingStreet FROM Account WHERE Type = 'Customer'"
    ]


def test_incremental_field_datetime_type_first_run_unquoted(mod, monkeypatch):
    """SystemModstamp is a datetime-typed field -- first run (no stored last_value) emits
    no WHERE clause at all (nothing to compare against yet), just the ORDER BY, and the
    resource is wired for merge with Id as the primary key."""
    fake_client = FakeSalesforce(
        describe_by_object={"Account": DEFAULT_DESCRIBE_FIELDS},
        bulk_pages_by_object={"Account": []},
    )
    install_fake_simple_salesforce(monkeypatch, fake_client)
    resource = mod._build_salesforce_resource(
        fake_client, "Account", "account", "SystemModstamp", None,
    )
    assert resource.write_disposition == "merge"
    assert resource.incremental is not None
    list(resource())
    assert fake_client.queries == [
        "SELECT Id, Name, SystemModstamp, BillingStreet FROM Account ORDER BY SystemModstamp ASC"
    ]


def test_incremental_field_combined_with_where_clause_first_run(mod, monkeypatch):
    """First run has no stored last_value (nothing to compare the incremental field
    against yet), so only where_clause appears in the predicate, parenthesized (it's
    written to be ANDed with whatever the incremental comparison would otherwise add)."""
    fake_client = FakeSalesforce(
        describe_by_object={"Account": DEFAULT_DESCRIBE_FIELDS},
        bulk_pages_by_object={"Account": []},
    )
    install_fake_simple_salesforce(monkeypatch, fake_client)
    resource = mod._build_salesforce_resource(
        fake_client, "Account", "account", "Id", "Type = 'Customer'",
    )
    list(resource())
    assert fake_client.queries == [
        "SELECT Id, Name, SystemModstamp, BillingStreet FROM Account "
        "WHERE (Type = 'Customer') ORDER BY Id ASC"
    ]


def test_quote_soql_value_quotes_string_like_types_not_numeric_date_types(mod):
    """Direct unit test of the quoting rule used for an incremental field's comparison
    value: id/string-like Salesforce field types get single-quoted (SOQL string literal
    syntax, escaping embedded quotes) -- the StreamSets-migration case this component
    documents (`WHERE Id > '${OFFSET}'`) -- while date/datetime/numeric/boolean types are
    left bare, matching SOQL's own literal syntax for those types."""
    assert mod._quote_soql_value("005xx0000012345AAA", "id") == "'005xx0000012345AAA'"
    assert mod._quote_soql_value("O'Brien", "string") == "'O\\'Brien'"
    assert mod._quote_soql_value("2024-01-01T00:00:00.000Z", "datetime") == "2024-01-01T00:00:00.000Z"
    assert mod._quote_soql_value("2024-01-01", "date") == "2024-01-01"
    assert mod._quote_soql_value(42, "int") == "42"


def test_custom_object_name_supported(mod, monkeypatch):
    """No fixed resource-function list anymore -- any object name Salesforce's describe()
    recognizes works, standard or custom (__c)."""
    fake_client = FakeSalesforce(
        describe_by_object={"MyCustomObject__c": [
            {"name": "Id", "type": "id", "compoundFieldName": None},
            {"name": "Custom_Field__c", "type": "string", "compoundFieldName": None},
        ]},
        bulk_pages_by_object={"MyCustomObject__c": [[{"Id": "a1", "Custom_Field__c": "x", "attributes": {}}]]},
    )
    install_fake_simple_salesforce(monkeypatch, fake_client)
    resource = mod._build_salesforce_resource(
        fake_client, "MyCustomObject__c", "mycustomobject__c", None, None,
    )
    rows = list(resource())
    assert rows == [{"Id": "a1", "Custom_Field__c": "x"}]
    assert fake_client.queries == ["SELECT Id, Custom_Field__c FROM MyCustomObject__c"]


def test_bulk_api_falls_back_to_standard_soql_when_async_disabled(mod, monkeypatch):
    fake_client = FakeSalesforce(
        describe_by_object={"Account": DEFAULT_DESCRIBE_FIELDS},
        standard_records_by_object={"Account": [{"Id": "1", "attributes": {}}]},
        raise_async_disabled=True,
    )
    install_fake_simple_salesforce(monkeypatch, fake_client)
    resource = mod._build_salesforce_resource(fake_client, "Account", "account", None, None)
    rows = list(resource())
    assert rows == [{"Id": "1"}]
    # The bulk attempt (which raised) and the standard-API fallback both record the same SOQL.
    assert "SELECT Id, Name, SystemModstamp, BillingStreet FROM Account" in fake_client.queries


def test_single_asset_shape_materializes_with_where_clause_and_incremental_field_configured(mod, monkeypatch):
    """End-to-end sanity check through build_defs()/materialize() with the new fields set --
    confirms the asset still wires up correctly (schema_contract, table-name overrides, etc.)
    around the new resource-construction path, not just the resource factory in isolation."""
    table = pd.DataFrame({"id": [1, 2], "created_at": ["2024-01-02", "2024-01-03"]})
    fake_sf = FakeSalesforce(describe_by_object={"Account": DEFAULT_DESCRIBE_FIELDS})
    install_fake_dlt(monkeypatch, mod, table_rows={"account": table}, fake_sf_client=fake_sf)
    component = _make_component(
        mod, where_clause="Type = 'Customer'", incremental_field="SystemModstamp",
    )
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 2
