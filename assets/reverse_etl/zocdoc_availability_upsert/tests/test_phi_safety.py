"""Dedicated PHI-safety regression tests for ZocdocAvailabilityUpsertComponent.

This component writes operational availability (timeslots), not patient
data -- but it still must never surface slot content (visit reason ids,
patient-type restrictions, or any sensitive free-text an upstream column
might carry) in materialization metadata, logs, or error messages. These
tests prove that guarantee against a realistic upstream DataFrame,
including a free-text column carrying provider-schedule detail that must
never be echoed back.
"""
import pandas as pd
import pytest
import dagster as dg

from .conftest import FakeZocdocResource, SENSITIVE_NOTE_VALUE, load_component_module, make_upstream_asset, metadata_for

FIELDS_MAP = {
    "prov_col": "provider_id",
    "date_col": "date",
    "loc_col": "location_id",
    "start_col": "start_time",
    "tz_col": "time_zone",
    "reasons_col": "allowed_visit_reason_ids",
    "patient_type_col": "patient_type",
}

_FORBIDDEN_SUBSTRINGS = [
    SENSITIVE_NOTE_VALUE,
    "Dr. Grant",
    "clinic staffing",
    "vr_super_secret_reason_42",
]


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None, raise_on_error=True):
    upstream_asset = make_upstream_asset("upstream_slots", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def, upstream_asset],
        resources={"zocdoc_resource": resource or FakeZocdocResource()},
        raise_on_error=raise_on_error,
    )


def _flatten_to_text(value) -> str:
    return repr(value)


def test_successful_upsert_metadata_contains_no_row_content(mod):
    def fake_put(session, base_url, provider_id, date_str, timeslots):
        return {}

    mod._zocdoc_put_timeslots = fake_put

    df = pd.DataFrame({
        "prov_col": ["prov_1", "prov_1"],
        "date_col": ["2026-06-15", "2026-06-15"],
        "loc_col": ["loc_1", "loc_1"],
        "start_col": ["2026-06-15T09:00:00Z", "2026-06-15T09:30:00Z"],
        "tz_col": ["America/New_York", "America/New_York"],
        "reasons_col": ["vr_super_secret_reason_42", "vr_super_secret_reason_42"],
        "patient_type_col": ["all", "all"],
        # An extra upstream column NOT in fields_map, carrying sensitive
        # free-text that must never leak even though it rides along in
        # the DataFrame.
        "internal_scheduling_note": [SENSITIVE_NOTE_VALUE, SENSITIVE_NOTE_VALUE],
    })

    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
    )
    result = _materialize(component, df)
    assert result.success

    out = metadata_for(result, "zocdoc_sync")
    full_text = _flatten_to_text(out)
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in full_text, (
            f"PHI/sensitive-data leak: {forbidden!r} found in metadata: {full_text}"
        )

    # Metadata keys are a known, bounded set -- no surprise key derived
    # from row content.
    allowed_keys = {
        "rows_processed", "groups_processed", "rows_upserted",
        "rows_skipped_no_key", "rows_errored", "groups_oversized",
        "first_errors",
    }
    assert set(out.keys()) <= allowed_keys


def test_error_path_metadata_contains_no_row_content(mod):
    """Even when a group fails, metadata's `first_errors` must carry only
    the (provider_id, date) identifiers and a status code -- never the
    timeslot payload that failed to write."""

    def fake_put(session, base_url, provider_id, date_str, timeslots):
        raise mod.ZocdocAPIError(422, "Zocdoc timeslots request failed")

    mod._zocdoc_put_timeslots = fake_put

    df = pd.DataFrame({
        "prov_col": ["prov_1"],
        "date_col": ["2026-06-15"],
        "loc_col": ["loc_1"],
        "start_col": ["2026-06-15T09:00:00Z"],
        "tz_col": ["America/New_York"],
        "reasons_col": ["vr_super_secret_reason_42"],
        "patient_type_col": ["all"],
    })

    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
    )
    result = _materialize(component, df)
    assert result.success

    out = metadata_for(result, "zocdoc_sync")
    full_text = _flatten_to_text(out)
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in full_text
    # The status code IS allowed to appear (it's not PHI).
    assert "422" in full_text
    # And the bare, non-PHI group identifiers are allowed.
    assert "prov_1" in full_text
    assert "2026-06-15" in full_text


def test_oversized_group_error_contains_no_row_content(mod):
    n = 1501
    df = pd.DataFrame({
        "prov_col": ["prov_1"] * n,
        "date_col": ["2026-06-15"] * n,
        "loc_col": ["loc_1"] * n,
        "start_col": ["2026-06-15T09:00:00Z"] * n,
        "tz_col": ["America/New_York"] * n,
        "reasons_col": ["vr_super_secret_reason_42"] * n,
        "patient_type_col": ["all"] * n,
    })

    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
        batch_size=2000,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zocdoc_sync")
    full_text = _flatten_to_text(out)
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert forbidden not in full_text


def test_put_timeslots_function_never_includes_body_in_error():
    """The isolated `_zocdoc_put_timeslots` function must never pass the
    request body (the timeslot list itself) or response body into a
    raised exception."""
    import pathlib

    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    start = source.index("def _zocdoc_put_timeslots")
    end = source.index("\n_REQUIRED_TARGETS", start + 1)
    func_src = source[start:end]
    assert "resp.text" not in func_src
    assert "{e}" not in func_src
    # The ZocdocAPIError raised on failure must never be constructed with
    # the timeslot list itself as its message argument.
    assert "ZocdocAPIError(resp.status_code, timeslots)" not in func_src
    assert "ZocdocAPIError(None, timeslots)" not in func_src


def test_zocdoc_api_error_class_never_stores_body(mod):
    import inspect

    sig = inspect.signature(mod.ZocdocAPIError.__init__)
    param_names = list(sig.parameters.keys())
    assert param_names == ["self", "status_code", "message"]


def test_no_preview_field_exists_on_component(mod):
    field_names = set(mod.ZocdocAvailabilityUpsertComponent.model_fields.keys())
    assert "include_preview_metadata" not in field_names
    assert "preview_rows" not in field_names
