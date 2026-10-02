"""Committed regression tests for FreshserviceTicketUpsertComponent.

The real Freshservice REST API is never called here -- FakeFreshserviceResource
(conftest.py) stands in for the one external-call boundary
(create_ticket / update_ticket / filter_tickets), while everything this
component actually owns -- dual source resolution, key_field validation,
fields_map top-level/custom_fields splitting, the custom_fields. prefix
stripping for filter queries, in-run cache, per-row error handling, and
metadata -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeFreshserviceResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource, upstream_name="upstream_tickets"):
    upstream_asset = make_upstream_asset(upstream_name, upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"freshservice_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata rather than output_for_node()."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- dual-source validation ------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.FreshserviceTicketUpsertComponent(
            asset_name="x",
            key_field="subject",
            fields_map={"title": "subject"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.FreshserviceTicketUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            key_field="subject",
            fields_map={"title": "subject"},
        ).build_defs(context=None)


# --- key_field validation ---------------------------------------------------

def test_key_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not present in fields_map values"):
        mod.FreshserviceTicketUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            key_field="custom_fields.unique_external_id",
            fields_map={"title": "subject", "details": "description"},
        ).build_defs(context=None)


# --- missing-key rows skipped ------------------------------------------------

def test_missing_key_rows_skipped_and_counted(mod):
    df = pd.DataFrame(
        {
            "alert_id": ["A-1", None, ""],
            "title": ["Payment gateway down", "No key here", "Blank key"],
        }
    )
    resource = FakeFreshserviceResource()
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="custom_fields.unique_external_id",
        fields_map={"alert_id": "custom_fields.unique_external_id", "title": "subject"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "freshservice_tickets_out")
    assert out["rows_skipped_no_key"] == 2
    assert out["rows_created"] == 1


# --- create: top-level + nested custom_fields split -------------------------

def test_new_key_value_creates_ticket_with_correct_split(mod):
    df = pd.DataFrame(
        {
            "alert_id": ["A-1"],
            "title": ["Payment gateway down"],
            "details": ["Gateway returning 500s"],
            "requester_email": ["ops@corp.com"],
        }
    )
    resource = FakeFreshserviceResource()
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="custom_fields.unique_external_id",
        fields_map={
            "alert_id": "custom_fields.unique_external_id",
            "title": "subject",
            "details": "description",
            "requester_email": "email",
        },
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 1
    assert len(resource.update_calls) == 0
    body = resource.create_calls[0]
    assert body["subject"] == "Payment gateway down"
    assert body["description"] == "Gateway returning 500s"
    assert body["email"] == "ops@corp.com"
    assert body["custom_fields"] == {"unique_external_id": "A-1"}

    out = _metadata_for(result, "freshservice_tickets_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0


# --- update path: existing key value calls update_ticket, not create -------

def test_existing_key_value_calls_update_not_create(mod):
    resource = FakeFreshserviceResource()
    resource.seed_ticket(42, subject="Old subject", custom_fields={"unique_external_id": "A-1"})

    df = pd.DataFrame({"alert_id": ["A-1"], "title": ["Updated subject"]})
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="custom_fields.unique_external_id",
        fields_map={"alert_id": "custom_fields.unique_external_id", "title": "subject"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 0
    assert len(resource.update_calls) == 1
    ticket_id, body = resource.update_calls[0]
    assert ticket_id == 42
    assert body["subject"] == "Updated subject"

    out = _metadata_for(result, "freshservice_tickets_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


# --- custom_fields. prefix stripped for the filter query --------------------

def test_custom_fields_prefix_stripped_in_filter_query(mod):
    resource = FakeFreshserviceResource()
    df = pd.DataFrame({"alert_id": ["A-1"], "title": ["Some ticket"]})
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="custom_fields.unique_external_id",
        fields_map={"alert_id": "custom_fields.unique_external_id", "title": "subject"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.filter_calls) == 1
    # Bare field name -- no "custom_fields." prefix -- per Freshservice's
    # Filter Tickets API, which addresses custom fields by raw name.
    assert resource.filter_calls[0] == "unique_external_id:'A-1'"


def test_unprefixed_key_field_used_as_is_in_filter_query(mod):
    resource = FakeFreshserviceResource()
    df = pd.DataFrame({"email": ["a@b.com"], "title": ["Some ticket"]})
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="email",
        fields_map={"email": "email", "title": "subject"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.filter_calls[0] == "email:'a@b.com'"


# --- batch_size caps rows -----------------------------------------------------

def test_batch_size_caps_rows(mod):
    df = pd.DataFrame(
        {
            "alert_id": [f"A-{i}" for i in range(10)],
            "title": [f"ticket {i}" for i in range(10)],
        }
    )
    resource = FakeFreshserviceResource()
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="custom_fields.unique_external_id",
        fields_map={"alert_id": "custom_fields.unique_external_id", "title": "subject"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "freshservice_tickets_out")
    assert out["rows_created"] == 3
    assert len(resource.create_calls) == 3


# --- inline source end-to-end -------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeFreshserviceResource()
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        source={
            "kind": "inline",
            "rows": [
                {"alert_id": "A-1", "title": "Ticket one"},
                {"alert_id": "A-2", "title": "Ticket two"},
            ],
        },
        key_field="custom_fields.unique_external_id",
        fields_map={"alert_id": "custom_fields.unique_external_id", "title": "subject"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"freshservice_resource": resource})
    assert result.success
    out = _metadata_for(result, "freshservice_tickets_out")
    assert out["rows_created"] == 2
    assert len(resource.create_calls) == 2


# --- resource-call exception caught, counted, run doesn't fail ---------------

def test_resource_exception_caught_and_counted(mod):
    resource = FakeFreshserviceResource(fail_on_create_for={"A-2"})
    df = pd.DataFrame(
        {
            "alert_id": ["A-1", "A-2", "A-3"],
            "title": ["ok ticket", "boom ticket", "ok ticket 2"],
        }
    )
    component = mod.FreshserviceTicketUpsertComponent(
        asset_name="freshservice_tickets_out",
        upstream_asset_key="upstream_tickets",
        key_field="custom_fields.unique_external_id",
        fields_map={"alert_id": "custom_fields.unique_external_id", "title": "subject"},
    )
    result = _materialize(component, df, resource)
    # The run as a whole still succeeds -- per-row errors are caught and
    # surfaced in metadata rather than failing the asset.
    assert result.success
    out = _metadata_for(result, "freshservice_tickets_out")
    assert out["rows_created"] == 2
    assert out["rows_errored"] == 1
    assert "first_errors" in out
    assert "A-2" in out["first_errors"][0]
