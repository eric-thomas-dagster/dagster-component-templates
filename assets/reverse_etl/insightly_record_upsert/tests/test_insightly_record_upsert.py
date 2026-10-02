"""Committed regression tests for InsightlyRecordUpsertComponent.

The real InsightlyResource is never instantiated here -- a minimal
FakeInsightlyResource (conftest.py) stands in for the one external,
paid-API boundary, while everything this component actually owns --
dual source resolution, validation, CONTACTINFOS shaping, dedupe-column
resolution, and metadata counting -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    FailingInsightlyResource,
    FakeInsightlyResource,
    load_component_module,
    make_upstream_asset,
)


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"insightly": resource})


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- CONTACTINFOS shaping (pure, no resource involved) -----------------

def test_build_body_nests_email_and_phone_for_contacts(mod):
    row = {"work_email": "jane@example.com", "work_phone": "+14155551234", "fn": "Jane"}
    fields_map = {"work_email": "email", "work_phone": "phone", "fn": "FIRST_NAME"}
    body = mod._build_body(row, fields_map, "Contacts")
    assert body["FIRST_NAME"] == "Jane"
    assert {"TYPE": "EMAIL", "LABEL": "Work", "DETAIL": "jane@example.com"} in body["CONTACTINFOS"]
    assert {"TYPE": "PHONE", "LABEL": "Work", "DETAIL": "+14155551234"} in body["CONTACTINFOS"]


def test_build_body_flat_fields_for_leads_no_nesting(mod):
    row = {"contact_email": "a@b.com"}
    fields_map = {"contact_email": "EMAIL"}
    body = mod._build_body(row, fields_map, "Leads")
    assert body == {"EMAIL": "a@b.com"}
    assert "CONTACTINFOS" not in body


def test_build_body_email_sentinel_ignored_for_non_contacts(mod):
    # The "email"/"phone" sentinel only nests for object_type == "Contacts" --
    # for Leads/Organisations it would land as a literal flat key "email",
    # which is almost certainly not what the user wants, but that's a config
    # mistake the component doesn't try to silently paper over.
    row = {"col": "a@b.com"}
    fields_map = {"col": "email"}
    body = mod._build_body(row, fields_map, "Leads")
    assert body == {"email": "a@b.com"}


def test_build_body_skips_none_and_nan_values(mod):
    row = {"fn": None, "ln": float("nan"), "email_col": "a@b.com"}
    fields_map = {"fn": "FIRST_NAME", "ln": "LAST_NAME", "email_col": "email"}
    body = mod._build_body(row, fields_map, "Contacts")
    assert "FIRST_NAME" not in body
    assert "LAST_NAME" not in body
    assert body["CONTACTINFOS"] == [{"TYPE": "EMAIL", "LABEL": "Work", "DETAIL": "a@b.com"}]


# --- validation ----------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.InsightlyRecordUpsertComponent(
            asset_name="x",
            object_type="Contacts",
            dedupe_field="email",
            fields_map={"e": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.InsightlyRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            object_type="Contacts",
            dedupe_field="email",
            fields_map={"e": "email"},
        ).build_defs(context=None)


def test_invalid_object_type_raises(mod):
    with pytest.raises(ValueError, match="object_type"):
        mod.InsightlyRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            object_type="Deals",
            dedupe_field="email",
            fields_map={"e": "email"},
        ).build_defs(context=None)


def test_dedupe_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="dedupe_field"):
        mod.InsightlyRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            object_type="Contacts",
            dedupe_field="phone",
            fields_map={"e": "email"},
        ).build_defs(context=None)


# --- full asset body, against the fake resource --------------------------

def test_create_and_update_paths_end_to_end(mod):
    df = pd.DataFrame(
        {
            "work_email": ["new@example.com", "existing@example.com"],
            "first_name": ["New", "Existing"],
        }
    )
    resource = FakeInsightlyResource(existing_by_value={"existing@example.com": 77})
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        upstream_asset_key="upstream_customers",
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email", "first_name": "FIRST_NAME"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.upsert_calls) == 2
    first, second = resource.upsert_calls
    assert first["object_type"] == "Contacts"
    assert first["search_field_name"] == "EMAIL_ADDRESS"  # default sentinel mapping
    assert first["search_field_value"] == "new@example.com"
    assert first["body"]["CONTACTINFOS"] == [
        {"TYPE": "EMAIL", "LABEL": "Work", "DETAIL": "new@example.com"}
    ]
    assert second["search_field_value"] == "existing@example.com"

    out = _metadata_for(result, "insightly_contacts_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_key"] == 0
    assert out["dedupe_search_field_name"] == "EMAIL_ADDRESS"


def test_dedupe_search_field_name_override(mod):
    df = pd.DataFrame({"contact_email": ["a@b.com"]})
    resource = FakeInsightlyResource()
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_leads_out",
        upstream_asset_key="upstream_customers",
        object_type="Leads",
        dedupe_field="EMAIL",
        dedupe_search_field_name="EMAIL",
        fields_map={"contact_email": "EMAIL"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls[0]["search_field_name"] == "EMAIL"
    assert resource.upsert_calls[0]["object_type"] == "Leads"


def test_rows_with_no_dedupe_value_are_skipped_and_counted(mod):
    df = pd.DataFrame({"work_email": ["a@b.com", None, ""]})
    resource = FakeInsightlyResource()
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        upstream_asset_key="upstream_customers",
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "insightly_contacts_out")
    assert out["rows_upserted"] == 1
    assert out["rows_skipped_no_key"] == 2
    assert len(resource.upsert_calls) == 1


def test_errors_are_isolated_per_row_and_counted(mod):
    df = pd.DataFrame({"work_email": ["good@example.com", "bad@example.com"]})
    resource = FailingInsightlyResource(fail_values={"bad@example.com"})
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        upstream_asset_key="upstream_customers",
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success  # per-row errors don't fail the whole asset
    out = _metadata_for(result, "insightly_contacts_out")
    assert out["rows_upserted"] == 1
    assert out["rows_errored"] == 1
    assert "bad@example.com" in out["first_errors"][0]


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"work_email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeInsightlyResource()
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        upstream_asset_key="upstream_customers",
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "insightly_contacts_out")
    assert out["rows_upserted"] == 3
    assert len(resource.upsert_calls) == 3


def test_empty_upstream_short_circuits(mod):
    df = pd.DataFrame({"work_email": []})
    resource = FakeInsightlyResource()
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        upstream_asset_key="upstream_customers",
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "insightly_contacts_out")
    assert out["rows_upserted"] == 0
    assert resource.upsert_calls == []


def test_source_inline_mode(mod):
    resource = FakeInsightlyResource()
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        source={"kind": "inline", "rows": [{"work_email": "a@b.com"}, {"work_email": "c@d.com"}]},
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"insightly": resource})
    assert result.success
    out = _metadata_for(result, "insightly_contacts_out")
    assert out["rows_upserted"] == 2


def test_missing_upstream_columns_raises_failure(mod):
    df = pd.DataFrame({"unrelated_col": ["x"]})
    resource = FakeInsightlyResource()
    component = mod.InsightlyRecordUpsertComponent(
        asset_name="insightly_contacts_out",
        upstream_asset_key="upstream_customers",
        object_type="Contacts",
        dedupe_field="email",
        fields_map={"work_email": "email"},
    )
    with pytest.raises(Exception, match="Columns not in upstream"):
        _materialize(component, df, resource)
