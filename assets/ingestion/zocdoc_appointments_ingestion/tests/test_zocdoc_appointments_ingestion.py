"""Committed regression tests for ZocdocAppointmentsIngestionComponent.

`_zocdoc_list_appointments` (the one external GET /v1/appointments call) is
monkeypatched wholesale -- everything this component actually owns
(pagination/next_url following, limit enforcement, query-param
construction from filters, the date-window-required validation, and
metadata shape) is exercised for real.
"""
import dagster as dg
import pytest

from .conftest import FakeZocdocResource, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource=None, raise_on_error=True):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={component.resource_key: resource or FakeZocdocResource()},
        raise_on_error=raise_on_error,
    )


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- date window validation ---------------------------------------------------

def test_missing_date_window_and_partition_fails(mod):
    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
    )
    result = _materialize(component, raise_on_error=False)
    assert not result.success


# --- pagination -----------------------------------------------------------------

def test_single_page_no_next_url(mod, monkeypatch):
    calls = []

    def fake_list(session, base_url, params):
        calls.append(dict(params))
        return {
            "request_id": "req_1",
            "page": 0,
            "page_size": 100,
            "total_count": 2,
            "next_url": None,
            "data": [{"appointment_id": "a1"}, {"appointment_id": "a2"}],
        }

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component)
    assert result.success
    assert len(calls) == 1
    assert calls[0]["start_time_utc_min"] == "2026-06-01T00:00:00Z"
    assert calls[0]["start_time_utc_max"] == "2026-07-01T00:00:00Z"
    assert calls[0]["page"] == 0

    out = _metadata_for(result, "zocdoc_out")
    assert out["rows_fetched"] == 2
    assert out["pages_fetched"] == 1
    assert out["total_count_reported"] == 2


def test_pagination_follows_next_url(mod, monkeypatch):
    pages = [
        {
            "total_count": 4,
            "next_url": "https://api.example/next?page=1",
            "data": [{"appointment_id": "a1"}, {"appointment_id": "a2"}],
        },
        {
            "total_count": 4,
            "next_url": None,
            "data": [{"appointment_id": "a3"}, {"appointment_id": "a4"}],
        },
    ]
    call_count = {"n": 0}

    def fake_list(session, base_url, params):
        page = pages[call_count["n"]]
        call_count["n"] += 1
        return page

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
        limit=1000,
    )
    result = _materialize(component)
    assert result.success
    assert call_count["n"] == 2
    out = _metadata_for(result, "zocdoc_out")
    assert out["rows_fetched"] == 4
    assert out["pages_fetched"] == 2


def test_limit_caps_rows_and_stops_pagination(mod, monkeypatch):
    call_count = {"n": 0}

    def fake_list(session, base_url, params):
        call_count["n"] += 1
        return {
            "total_count": 1000,
            "next_url": "https://api.example/next",
            "data": [{"appointment_id": f"a{call_count['n']}-{i}"} for i in range(100)],
        }

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
        limit=150,
    )
    result = _materialize(component)
    assert result.success
    out = _metadata_for(result, "zocdoc_out")
    assert out["rows_fetched"] == 150
    # Stops as soon as >= limit rows collected -- shouldn't walk all 1000.
    assert call_count["n"] == 2


def test_empty_response_returns_empty_dataframe(mod, monkeypatch):
    def fake_list(session, base_url, params):
        return {"total_count": 0, "next_url": None, "data": []}

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component)
    assert result.success
    out = _metadata_for(result, "zocdoc_out")
    assert out["rows_fetched"] == 0


# --- filters become query params -------------------------------------------------

def test_filters_passed_as_query_params(mod, monkeypatch):
    captured = {}

    def fake_list(session, base_url, params):
        captured.update(params)
        return {"total_count": 0, "next_url": None, "data": []}

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
        practice_ids="prac_1,prac_2",
        provider_ids="prov_1",
        location_ids="loc_1",
        statuses="booked,confirmed",
        sort_by="start_time_utc",
        sort_direction="ascending",
    )
    result = _materialize(component)
    assert result.success
    assert captured["practice_ids"] == "prac_1,prac_2"
    assert captured["provider_ids"] == "prov_1"
    assert captured["location_ids"] == "loc_1"
    assert captured["statuses"] == "booked,confirmed"
    assert captured["sort_by"] == "start_time_utc"
    assert captured["sort_direction"] == "ascending"

    out = _metadata_for(result, "zocdoc_out")
    assert out["practice_ids"] == "prac_1,prac_2"
    assert out["statuses"] == "booked,confirmed"


# --- error sanitization -----------------------------------------------------------

def test_api_error_surfaces_only_status_code(mod, monkeypatch):
    def fake_list(session, base_url, params):
        raise mod.ZocdocAPIError(500, "Zocdoc appointments request failed")

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component, raise_on_error=False)
    assert not result.success
    # Walk the failure data for the actual error message.
    failure_events = [
        e for e in result.all_events
        if e.event_type_value == "STEP_FAILURE"
    ]
    assert failure_events
    message = failure_events[0].event_specific_data.error.message
    assert "500" in message
    assert "Zocdoc appointments request failed" in message


# --- structural: no preview capability exists --------------------------------------

def test_no_preview_field_exists_on_component(mod):
    """There is no include_preview_metadata/preview_rows field anywhere on
    this component -- the capability doesn't exist, so it can't be
    misconfigured on."""
    field_names = set(mod.ZocdocAppointmentsIngestionComponent.model_fields.keys())
    assert "include_preview_metadata" not in field_names
    assert "preview_rows" not in field_names


def test_metadata_never_contains_preview_key(mod, monkeypatch):
    def fake_list(session, base_url, params):
        return {"total_count": 1, "next_url": None, "data": [{"appointment_id": "a1", "notes": "some note"}]}

    monkeypatch.setattr(mod, "_zocdoc_list_appointments", fake_list)

    component = mod.ZocdocAppointmentsIngestionComponent(
        asset_name="zocdoc_out",
        resource_key="zocdoc_resource",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component)
    out = _metadata_for(result, "zocdoc_out")
    assert "preview" not in out
    assert all("preview" not in k.lower() for k in out.keys())
