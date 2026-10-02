"""Committed regression tests for MarketoLeadUpsertComponent.

The real Marketo API is never called here -- `_call_marketo_leads_api` (the
one external, paid-API boundary) is monkeypatched wholesale, while dual
source resolution, fields_map application, lookup_field validation,
chunking, and create/update/skipped accounting are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeMarketoResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, body):
        calls.append(body)
        results = []
        for rec in body["input"]:
            results.append({"id": len(results) + 1, "status": "created"})
        return {"requestId": "abc", "success": True, "result": results}

    monkeypatch.setattr(mod, "_call_marketo_leads_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeMarketoResource()
    upstream_asset = make_upstream_asset("upstream_contacts", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"marketo": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.MarketoLeadUpsertComponent(
            asset_name="x",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.MarketoLeadUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.MarketoLeadUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="email",
            fields_map={"first_name": "firstName"},
        ).build_defs(context=None)


def test_default_lookup_field_is_email(mod):
    component = mod.MarketoLeadUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"email": "email"},
    )
    assert component.lookup_field == "email"
    # Should not raise -- email is present in fields_map.
    component.build_defs(context=None)


# --- missing columns ---------------------------------------------------

def test_missing_upstream_columns_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email", "first_name": "firstName"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- full asset body, against the monkeypatched API call ----------------

def test_create_update_skipped_counted_correctly(mod, monkeypatch):
    calls = []

    def _fake_call(resource, body):
        calls.append(body)
        statuses = ["created", "updated", "skipped"]
        results = []
        for i, rec in enumerate(body["input"]):
            if statuses[i] == "skipped":
                results.append({
                    "status": "skipped",
                    "reasons": [{"code": "1004", "message": "Lead not found"}],
                })
            else:
                results.append({"id": i, "status": statuses[i]})
        return {"requestId": "abc", "success": True, "result": results}

    monkeypatch.setattr(mod, "_call_marketo_leads_api", _fake_call)

    df = pd.DataFrame({
        "email": ["a@b.com", "c@d.com", "e@f.com"],
        "first_name": ["A", "C", "E"],
    })
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email", "first_name": "firstName"},
    )
    result = _materialize(component, df)
    assert result.success

    assert len(calls) == 1
    assert calls[0]["action"] == "createOrUpdate"
    assert calls[0]["lookupField"] == "email"
    assert len(calls[0]["input"]) == 3

    out = metadata_for(result, "marketo_lead_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_skipped"] == 1
    assert out["rows_upserted"] == 2
    assert out["rows_errored"] == 1
    assert "1004" in out["first_errors"][0]


def test_rows_missing_lookup_value_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"email": ["a@b.com", None, ""], "first_name": ["A", "B", "C"]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email", "first_name": "firstName"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "marketo_lead_upsert_out")
    # "" is falsy for a lookup value check only via None/NaN -- empty string
    # survives as a value today (component only treats None/NaN as missing);
    # verify exactly 1 is skipped (the None row).
    assert out["rows_skipped_no_key"] == 1
    assert len(recorded_calls) == 1
    assert len(recorded_calls[0]["input"]) == 2


def test_empty_upstream_short_circuits_without_api_call(mod, recorded_calls):
    df = pd.DataFrame({"email": [], "first_name": []})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email", "first_name": "firstName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "marketo_lead_upsert_out")
    assert out["rows_upserted"] == 0


def test_all_rows_missing_lookup_value_makes_no_api_call(mod, recorded_calls):
    df = pd.DataFrame({"email": [None, None], "first_name": ["A", "B"]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email", "first_name": "firstName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "marketo_lead_upsert_out")
    # Mirrors SalesforceRecordUpsertComponent's early-return shape: when
    # *every* row lacks the lookup value, metadata is just rows_upserted=0
    # (no per-reason breakdown, since the whole batch short-circuits).
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "marketo_lead_upsert_out")
    assert out["rows_upserted"] == 3


def test_requests_chunked_at_300_per_request(mod, monkeypatch):
    monkeypatch.setattr(mod, "_LEADS_PER_REQUEST", 2)
    calls = []

    def _fake_call(resource, body):
        calls.append(body)
        return {
            "success": True,
            "result": [{"id": i, "status": "created"} for i in range(len(body["input"]))],
        }

    monkeypatch.setattr(mod, "_call_marketo_leads_api", _fake_call)

    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success
    assert len(calls) == 3
    assert [len(c["input"]) for c in calls] == [2, 2, 1]
    out = metadata_for(result, "marketo_lead_upsert_out")
    assert out["api_requests"] == 3


def test_source_inline_mode(mod, recorded_calls):
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"marketo": FakeMarketoResource()})
    assert result.success
    out = metadata_for(result, "marketo_lead_upsert_out")
    assert out["rows_upserted"] == 2
    assert len(recorded_calls) == 1


def test_custom_field_passes_through(mod, recorded_calls):
    df = pd.DataFrame({"email": ["a@b.com"], "score": [42]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email", "score": "leadScore__c"},
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls[0]["input"][0] == {"email": "a@b.com", "leadScore__c": 42}


def test_api_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_call(resource, body):
        raise RuntimeError("Marketo 500")

    monkeypatch.setattr(mod, "_call_marketo_leads_api", _fake_call)

    df = pd.DataFrame({"email": ["a@b.com"]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df)
    assert result.success  # component swallows per-chunk errors into metadata
    out = metadata_for(result, "marketo_lead_upsert_out")
    assert out["rows_errored"] == 1
    assert "Marketo 500" in out["first_errors"][0]


def test_custom_lookup_field_other_than_email(mod, recorded_calls):
    df = pd.DataFrame({"lead_id": [123], "first_name": ["A"]})
    component = mod.MarketoLeadUpsertComponent(
        asset_name="marketo_lead_upsert_out",
        upstream_asset_key="upstream_contacts",
        lookup_field="id",
        fields_map={"lead_id": "id", "first_name": "firstName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert recorded_calls[0]["lookupField"] == "id"
