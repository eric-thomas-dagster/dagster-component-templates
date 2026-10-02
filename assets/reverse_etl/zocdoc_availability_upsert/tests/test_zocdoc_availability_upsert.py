"""Committed regression tests for ZocdocAvailabilityUpsertComponent.

`_zocdoc_put_timeslots` (the one external PUT .../calendar/timeslots call)
is monkeypatched wholesale -- everything this component actually owns
(dual source resolution, fields_map validation, grouping rows by mapped
(provider_id, date), the one-PUT-per-group invariant, the oversized-group
refusal, missing-required-field row skipping, and metadata shape) is
exercised for real.
"""
import pandas as pd
import pytest
import dagster as dg

from .conftest import FakeZocdocResource, load_component_module, make_upstream_asset, metadata_for

FIELDS_MAP = {
    "prov_col": "provider_id",
    "date_col": "date",
    "loc_col": "location_id",
    "start_col": "start_time",
    "tz_col": "time_zone",
    "reasons_col": "allowed_visit_reason_ids",
    "patient_type_col": "patient_type",
}


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


def _basic_df(n=2):
    return pd.DataFrame({
        "prov_col": ["prov_1"] * n,
        "date_col": ["2026-06-15"] * n,
        "loc_col": ["loc_1"] * n,
        "start_col": [f"2026-06-15T0{9+i}:00:00Z" for i in range(n)],
        "tz_col": ["America/New_York"] * n,
        "reasons_col": ["vr_1,vr_2"] * n,
        "patient_type_col": ["all"] * n,
    })


# --- validation (build_defs time) -------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ZocdocAvailabilityUpsertComponent(
            asset_name="x",
            fields_map=FIELDS_MAP,
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ZocdocAvailabilityUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map=FIELDS_MAP,
        ).build_defs(context=None)


def test_fields_map_missing_required_target_raises(mod):
    incomplete = {k: v for k, v in FIELDS_MAP.items() if v != "time_zone"}
    with pytest.raises(ValueError, match="missing required target"):
        mod.ZocdocAvailabilityUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map=incomplete,
        ).build_defs(context=None)


def test_fields_map_unknown_target_raises(mod):
    bad_map = dict(FIELDS_MAP)
    bad_map["mystery_col"] = "blocked_field"
    with pytest.raises(ValueError, match="unknown Zocdoc timeslot field"):
        mod.ZocdocAvailabilityUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map=bad_map,
        ).build_defs(context=None)


# --- grouping + one-PUT-per-group -------------------------------------------

def test_single_group_single_put_call(mod):
    calls = []

    def fake_put(session, base_url, provider_id, date_str, timeslots):
        calls.append((provider_id, date_str, list(timeslots)))
        return {}

    mod._zocdoc_put_timeslots = fake_put

    df = _basic_df(n=3)
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
    )
    result = _materialize(component, df)
    assert result.success
    assert len(calls) == 1
    provider_id, date_str, slots = calls[0]
    assert provider_id == "prov_1"
    assert date_str == "2026-06-15"
    assert len(slots) == 3
    assert slots[0]["location_id"] == "loc_1"
    assert slots[0]["time_zone"] == "America/New_York"
    assert slots[0]["allowed_visit_reason_ids"] == ["vr_1", "vr_2"]
    assert slots[0]["patient_type"] == "all"

    out = metadata_for(result, "zocdoc_sync")
    assert out["groups_processed"] == 1
    assert out["rows_upserted"] == 3
    assert out["rows_errored"] == 0
    assert out["groups_oversized"] == 0


def test_multiple_groups_multiple_puts(mod):
    calls = []

    def fake_put(session, base_url, provider_id, date_str, timeslots):
        calls.append((provider_id, date_str))
        return {}

    mod._zocdoc_put_timeslots = fake_put

    df = pd.DataFrame({
        "prov_col": ["prov_1", "prov_1", "prov_2"],
        "date_col": ["2026-06-15", "2026-06-16", "2026-06-15"],
        "loc_col": ["loc_1"] * 3,
        "start_col": ["2026-06-15T09:00:00Z", "2026-06-16T09:00:00Z", "2026-06-15T10:00:00Z"],
        "tz_col": ["America/New_York"] * 3,
    })
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map={k: v for k, v in FIELDS_MAP.items() if v not in ("allowed_visit_reason_ids", "patient_type")},
    )
    result = _materialize(component, df)
    assert result.success
    assert len(calls) == 3  # 3 distinct (provider_id, date) groups
    assert set(calls) == {("prov_1", "2026-06-15"), ("prov_1", "2026-06-16"), ("prov_2", "2026-06-15")}

    out = metadata_for(result, "zocdoc_sync")
    assert out["groups_processed"] == 3
    assert out["rows_upserted"] == 3


# --- oversized groups: refuse, never partially write -------------------------

def test_oversized_group_refused_not_partially_written(mod):
    calls = []

    def fake_put(session, base_url, provider_id, date_str, timeslots):
        calls.append((provider_id, date_str, len(timeslots)))
        return {}

    mod._zocdoc_put_timeslots = fake_put

    n = 1501
    df = pd.DataFrame({
        "prov_col": ["prov_1"] * n,
        "date_col": ["2026-06-15"] * n,
        "loc_col": ["loc_1"] * n,
        "start_col": [f"2026-06-15T09:00:00Z"] * n,
        "tz_col": ["America/New_York"] * n,
    })
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map={k: v for k, v in FIELDS_MAP.items() if v not in ("allowed_visit_reason_ids", "patient_type")},
        batch_size=2000,
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == []  # never called the API for the oversized group
    out = metadata_for(result, "zocdoc_sync")
    assert out["groups_oversized"] == 1
    assert out["groups_processed"] == 0
    assert out["rows_upserted"] == 0
    assert "first_errors" in out


def test_exactly_max_slots_is_allowed(mod):
    calls = []

    def fake_put(session, base_url, provider_id, date_str, timeslots):
        calls.append(len(timeslots))
        return {}

    mod._zocdoc_put_timeslots = fake_put

    n = 1500
    df = pd.DataFrame({
        "prov_col": ["prov_1"] * n,
        "date_col": ["2026-06-15"] * n,
        "loc_col": ["loc_1"] * n,
        "start_col": ["2026-06-15T09:00:00Z"] * n,
        "tz_col": ["America/New_York"] * n,
    })
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map={k: v for k, v in FIELDS_MAP.items() if v not in ("allowed_visit_reason_ids", "patient_type")},
        batch_size=2000,
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == [1500]
    out = metadata_for(result, "zocdoc_sync")
    assert out["groups_oversized"] == 0
    assert out["groups_processed"] == 1


# --- missing required fields are skipped, never sent with placeholders ------

def test_rows_missing_required_field_are_skipped(mod):
    def fake_put(session, base_url, provider_id, date_str, timeslots):
        return {}

    mod._zocdoc_put_timeslots = fake_put

    df = pd.DataFrame({
        "prov_col": ["prov_1", None, "prov_1"],
        "date_col": ["2026-06-15", "2026-06-15", "2026-06-15"],
        "loc_col": ["loc_1", "loc_1", "loc_1"],
        "start_col": ["2026-06-15T09:00:00Z", "2026-06-15T10:00:00Z", "2026-06-15T11:00:00Z"],
        "tz_col": ["America/New_York"] * 3,
    })
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map={k: v for k, v in FIELDS_MAP.items() if v not in ("allowed_visit_reason_ids", "patient_type")},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zocdoc_sync")
    assert out["rows_skipped_no_key"] == 1
    assert out["rows_processed"] == 3
    assert out["rows_upserted"] == 2


# --- API error handling: sanitized, counted ----------------------------------

def test_api_error_counts_as_errored_group(mod):
    def fake_put(session, base_url, provider_id, date_str, timeslots):
        raise mod.ZocdocAPIError(503, "Zocdoc timeslots request failed")

    mod._zocdoc_put_timeslots = fake_put

    df = _basic_df(n=2)
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
    )
    result = _materialize(component, df)
    assert result.success  # component reports the error in metadata, doesn't hard-fail the run
    out = metadata_for(result, "zocdoc_sync")
    assert out["rows_errored"] == 2
    assert out["groups_processed"] == 0
    assert "first_errors" in out


# --- empty / batch_size cap ---------------------------------------------------

def test_empty_upstream_short_circuits(mod):
    df = pd.DataFrame(columns=list(FIELDS_MAP.keys()))
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zocdoc_sync")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod):
    def fake_put(session, base_url, provider_id, date_str, timeslots):
        return {}

    mod._zocdoc_put_timeslots = fake_put

    df = _basic_df(n=10)
    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        upstream_asset_key="upstream_slots",
        fields_map=FIELDS_MAP,
        batch_size=4,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "zocdoc_sync")
    assert out["rows_processed"] == 4
    assert out["rows_upserted"] == 4


# --- source=inline mode -------------------------------------------------------

def test_source_inline_mode(mod):
    calls = []

    def fake_put(session, base_url, provider_id, date_str, timeslots):
        calls.append((provider_id, date_str))
        return {}

    mod._zocdoc_put_timeslots = fake_put

    component = mod.ZocdocAvailabilityUpsertComponent(
        asset_name="zocdoc_sync",
        source={
            "kind": "inline",
            "rows": [
                {
                    "prov_col": "prov_1", "date_col": "2026-06-15", "loc_col": "loc_1",
                    "start_col": "2026-06-15T09:00:00Z", "tz_col": "America/New_York",
                },
            ],
        },
        fields_map={k: v for k, v in FIELDS_MAP.items() if v not in ("allowed_visit_reason_ids", "patient_type")},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"zocdoc_resource": FakeZocdocResource()})
    assert result.success
    assert calls == [("prov_1", "2026-06-15")]
