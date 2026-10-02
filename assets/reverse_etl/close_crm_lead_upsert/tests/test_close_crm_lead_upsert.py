"""Committed regression tests for CloseCrmLeadUpsertComponent.

The real CloseCrmResource (HTTP-backed) is never constructed here -- a
minimal FakeCloseCrmResource (conftest.py) stands in for it, recording
upsert_lead() calls. Everything this component actually owns -- dual
source resolution, validation (dedupe_field / fields_map shape checks),
Lead-body construction, dedupe-value extraction, and metadata counting --
is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeCloseCrmResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_leads", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"close_crm": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata rather than output_for_node()."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation --------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.CloseCrmLeadUpsertComponent(
            asset_name="x",
            dedupe_field="email",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.CloseCrmLeadUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            dedupe_field="email",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_unrecognized_fields_map_value_raises(mod):
    with pytest.raises(ValueError, match="not a recognized Close field"):
        mod.CloseCrmLeadUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            dedupe_field="email",
            fields_map={"email": "email", "title": "job_title"},
        ).build_defs(context=None)


def test_dedupe_field_not_in_fields_map_values_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.CloseCrmLeadUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            dedupe_field="phone",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_dedupe_field_contact_name_is_rejected_as_unsearchable(mod):
    # contact_name IS a valid fields_map value, but Close can't search on it.
    with pytest.raises(ValueError, match="not searchable"):
        mod.CloseCrmLeadUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            dedupe_field="contact_name",
            fields_map={"full_name": "contact_name"},
        ).build_defs(context=None)


def test_dedupe_field_custom_field_is_accepted(mod):
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        dedupe_field="custom.cf_abc",
        fields_map={"warehouse_id": "custom.cf_abc"},
    )
    defs = component.build_defs(context=None)  # should not raise
    assert len(list(defs.assets)) == 1


# --- full asset body, against the fake resource ------------------------------

def test_upsert_builds_full_lead_body_and_counts_created(mod):
    df = pd.DataFrame(
        {
            "company": ["Acme Inc"],
            "contact": ["Jane Doe"],
            "email": ["jane@example.com"],
            "phone": ["+14155552671"],
            "ext_id": ["wh-123"],
        }
    )
    resource = FakeCloseCrmResource()
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        dedupe_field="email",
        fields_map={
            "company": "name",
            "contact": "contact_name",
            "email": "email",
            "phone": "phone",
            "ext_id": "custom.cf_xyz",
        },
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.upsert_calls) == 1
    call = resource.upsert_calls[0]
    assert call["dedupe_field"] == "email"
    assert call["dedupe_value"] == "jane@example.com"
    body = call["create_body"]
    assert body["name"] == "Acme Inc"
    assert body["contacts"] == [
        {
            "name": "Jane Doe",
            "emails": [{"email": "jane@example.com", "type": "office"}],
            "phones": [{"phone": "+14155552671", "type": "office"}],
        }
    ]
    assert body["custom.cf_xyz"] == "wh-123"

    out = _metadata_for(result, "close_crm_leads_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["rows_upserted"] == 1
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_key"] == 0
    assert out["close_lead_dedupe_field"] == "email"


def test_upsert_counts_updated_when_resource_reports_updated(mod):
    df = pd.DataFrame({"email": ["existing@example.com"]})
    resource = FakeCloseCrmResource(
        scripted_results={"existing@example.com": {"action": "updated", "id": "lead_1"}}
    )
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        dedupe_field="email",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "close_crm_leads_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_rows_missing_dedupe_value_are_skipped_and_counted(mod):
    df = pd.DataFrame({"email": ["a@b.com", None, ""]})
    resource = FakeCloseCrmResource()
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        dedupe_field="email",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "close_crm_leads_out")
    # Note: "" is not None/NaN so it IS passed through as the dedupe value
    # (an empty-string email) -- only the true-null row is skipped here.
    assert len(resource.upsert_calls) == 2
    assert out["rows_skipped_no_key"] == 1


def test_errors_from_resource_are_caught_and_counted(mod):
    df = pd.DataFrame({"email": ["ok@example.com", "boom@example.com"]})
    resource = FakeCloseCrmResource(
        scripted_results={"boom@example.com": RuntimeError("HTTP 500")}
    )
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        dedupe_field="email",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "close_crm_leads_out")
    assert out["rows_created"] == 1
    assert out["rows_errored"] == 1
    assert "boom@example.com" in out["first_errors"][0]


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeCloseCrmResource()
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        dedupe_field="email",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 3
    out = _metadata_for(result, "close_crm_leads_out")
    assert out["rows_upserted"] == 3


def test_missing_upstream_column_raises_failure(mod):
    df = pd.DataFrame({"other_col": ["x"]})
    resource = FakeCloseCrmResource()
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        dedupe_field="email",
        fields_map={"email": "email"},
    )
    upstream_asset = make_upstream_asset("upstream_leads", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def, upstream_asset],
        resources={"close_crm": resource},
        raise_on_error=False,
    )
    assert not result.success


def test_source_inline_mode(mod):
    resource = FakeCloseCrmResource()
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        dedupe_field="email",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"close_crm": resource})
    assert result.success
    out = _metadata_for(result, "close_crm_leads_out")
    assert out["rows_upserted"] == 2


def test_custom_resource_key_is_used(mod):
    resource = FakeCloseCrmResource()
    component = mod.CloseCrmLeadUpsertComponent(
        asset_name="close_crm_leads_out",
        upstream_asset_key="upstream_leads",
        resource_key="close_crm_eu",
        dedupe_field="email",
        fields_map={"email": "email"},
    )
    upstream_asset = make_upstream_asset("upstream_leads", pd.DataFrame({"email": ["a@b.com"]}))
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"close_crm_eu": resource})
    assert result.success
    assert len(resource.upsert_calls) == 1
