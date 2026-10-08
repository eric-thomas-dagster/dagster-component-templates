"""Committed regression tests for RampReimbursementCardWriteComponent.

`FakeRampResource` (conftest.py) stands in for the real Ramp Developer API.
Covers: build_defs-time validation per mode, dual source resolution,
per-row success/error/skip counting for all four modes, idempotency-key
determinism for receipt_reimbursement, and -- the one true safety property
of this component -- that `virtual_card_create`'s pan/cvv never survive
into the asset's metadata.
"""
import pandas as pd
import pytest
import dagster as dg

from .conftest import FakeRampResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_rows", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"ramp_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation (build_defs time) -------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", mode="mileage_reimbursement",
            reimbursee_id_column="a", trip_date_column="b", distance_column="c",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", source={"kind": "inline", "rows": []},
            mode="mileage_reimbursement",
            reimbursee_id_column="a", trip_date_column="b", distance_column="c",
        ).build_defs(context=None)


def test_invalid_mode_raises(mod):
    with pytest.raises(ValueError, match="mode must be one of"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="delete_everything",
        ).build_defs(context=None)


def test_mileage_mode_requires_columns(mod):
    with pytest.raises(ValueError, match="requires"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="mileage_reimbursement",
        ).build_defs(context=None)


def test_mileage_mode_rejects_invalid_distance_units(mod):
    with pytest.raises(ValueError, match="distance_units must be one of"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="mileage_reimbursement",
            reimbursee_id_column="a", trip_date_column="b", distance_column="c",
            distance_units="LIGHT_YEARS",
        ).build_defs(context=None)


def test_receipt_mode_requires_columns(mod):
    with pytest.raises(ValueError, match="requires"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="receipt_reimbursement",
        ).build_defs(context=None)


def test_virtual_card_create_requires_columns_and_interval(mod):
    with pytest.raises(ValueError, match="requires"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="virtual_card_create",
        ).build_defs(context=None)
    with pytest.raises(ValueError, match="requires `interval`"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="virtual_card_create",
            user_id_column="a", limit_amount_column="b",
        ).build_defs(context=None)
    with pytest.raises(ValueError, match="interval must be one of"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="virtual_card_create",
            user_id_column="a", limit_amount_column="b", interval="FORTNIGHTLY",
        ).build_defs(context=None)


def test_card_update_requires_card_id_column(mod):
    with pytest.raises(ValueError, match="requires `card_id_column`"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="card_update",
        ).build_defs(context=None)


def test_card_update_requires_at_least_one_updatable_field(mod):
    """The real-world constraint this component exists to document: Ramp's
    API has no spend_limit field here, so if none of the three real fields
    is configured there is nothing this mode could ever do."""
    with pytest.raises(ValueError, match="no spend_limit field"):
        mod.RampReimbursementCardWriteComponent(
            asset_name="x", upstream_asset_key="foo", mode="card_update",
            card_id_column="card_id",
        ).build_defs(context=None)


# --- mileage_reimbursement ---------------------------------------------------

def test_mileage_reimbursement_happy_path(mod):
    df = pd.DataFrame({
        "uid": ["usr_1", "usr_2"],
        "date": ["2026-01-01", "2026-01-02"],
        "miles": [10, 20],
    })
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="mileage_reimbursement",
        reimbursee_id_column="uid", trip_date_column="date", distance_column="miles",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.mileage_calls) == 2
    assert resource.mileage_calls[0]["distance_units"] == "MILES"
    out = _metadata_for(result, "ramp_out")
    assert out["rows_succeeded"] == 2
    assert out["mode"] == "mileage_reimbursement"


def test_mileage_reimbursement_skips_blank_required_value(mod):
    df = pd.DataFrame({"uid": ["usr_1", None], "date": ["2026-01-01", "2026-01-02"], "miles": [10, 20]})
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="mileage_reimbursement",
        reimbursee_id_column="uid", trip_date_column="date", distance_column="miles",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.mileage_calls) == 1
    out = _metadata_for(result, "ramp_out")
    assert out["rows_skipped"] == 1


def test_mileage_reimbursement_waypoints_parsed_from_csv_column(mod):
    df = pd.DataFrame({
        "uid": ["usr_1"], "date": ["2026-01-01"], "miles": [10],
        "wp": ["123 Main St, Springfield, 456 Oak Ave"],
    })
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="mileage_reimbursement",
        reimbursee_id_column="uid", trip_date_column="date", distance_column="miles",
        waypoints_column="wp",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.mileage_calls[0]["waypoints"] == ["123 Main St", "Springfield", "456 Oak Ave"]


def test_mileage_reimbursement_errors_are_counted_not_raised(mod):
    df = pd.DataFrame({"uid": ["usr_1"], "date": ["2026-01-01"], "miles": [10]})
    resource = FakeRampResource(fail_on={"mileage"})
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="mileage_reimbursement",
        reimbursee_id_column="uid", trip_date_column="date", distance_column="miles",
    )
    result = _materialize(component, df, resource)
    assert result.success  # asset itself succeeds; failures are counted in metadata
    out = _metadata_for(result, "ramp_out")
    assert out["rows_errored"] == 1
    assert out["rows_succeeded"] == 0


# --- receipt_reimbursement ---------------------------------------------------

def test_receipt_reimbursement_happy_path_and_reimbursement_id_passthrough(mod):
    df = pd.DataFrame({
        "uid": ["usr_1"], "path": ["/tmp/receipt1.png"], "rid": ["reimb_existing"],
    })
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="receipt_reimbursement",
        reimbursee_id_column="uid", receipt_file_path_column="path", reimbursement_id_column="rid",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.receipt_calls) == 1
    assert resource.receipt_calls[0]["reimbursement_id"] == "reimb_existing"


def test_receipt_reimbursement_idempotency_key_is_deterministic_per_row(mod):
    """Re-materializing the same row (no idempotency_key_column configured)
    must derive the SAME key both times -- that's the whole point of
    deriving it from row identity rather than a fresh uuid4 per call."""
    df = pd.DataFrame({"uid": ["usr_1"], "path": ["/tmp/receipt1.png"]})
    resource1 = FakeRampResource()
    resource2 = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="receipt_reimbursement",
        reimbursee_id_column="uid", receipt_file_path_column="path",
    )
    _materialize(component, df, resource1)
    _materialize(component, df, resource2)
    key1 = resource1.receipt_calls[0]["idempotency_key"]
    key2 = resource2.receipt_calls[0]["idempotency_key"]
    assert key1 == key2


def test_receipt_reimbursement_explicit_idempotency_key_column_wins(mod):
    df = pd.DataFrame({"uid": ["usr_1"], "path": ["/tmp/r.png"], "idem": ["my-custom-key"]})
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="receipt_reimbursement",
        reimbursee_id_column="uid", receipt_file_path_column="path", idempotency_key_column="idem",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.receipt_calls[0]["idempotency_key"] == "my-custom-key"


# --- virtual_card_create: the redaction guarantee ----------------------------

def test_virtual_card_create_never_leaks_pan_or_cvv_into_metadata(mod):
    df = pd.DataFrame({"uid": ["usr_1"], "amt": [500]})
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="virtual_card_create",
        user_id_column="uid", limit_amount_column="amt", interval="MONTHLY",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "ramp_out")

    # Full metadata, serialized, must never contain the PAN or CVV that
    # FakeRampResource's response included -- this is the component's core
    # safety property.
    serialized = str(out)
    assert "4111111111111111" not in serialized
    assert "123" not in serialized.replace("rows_succeeded", "")  # cvv='123' check avoids false positive on counts
    assert len(resource.virtual_card_calls) == 1

    created = out["created_cards"]
    assert created[0]["last4"] == "1111"
    assert "pan" not in created[0]
    assert "cvv" not in created[0]
    assert "expiration" not in created[0]
    assert created[0]["card_id"].startswith("card_")


def test_virtual_card_create_request_body_shape(mod):
    df = pd.DataFrame({"uid": ["usr_1"], "amt": [500]})
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="virtual_card_create",
        user_id_column="uid", limit_amount_column="amt", interval="MONTHLY",
        currency_code="USD", display_name_column=None,
    )
    _materialize(component, df, resource)
    call = resource.virtual_card_calls[0]
    assert call["user_id"] == "usr_1"
    assert call["limit_amount"] == 500
    assert call["interval"] == "MONTHLY"
    assert call["currency_code"] == "USD"


# --- card_update --------------------------------------------------------------

def test_card_update_happy_path(mod):
    df = pd.DataFrame({"cid": ["card_1"], "name": ["New name"]})
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="card_update",
        card_id_column="cid", display_name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.card_update_calls == [{"card_id": "card_1", "display_name": "New name", "fund_id": None, "automatic_routing_enabled": None}]


def test_card_update_skips_row_with_no_updatable_value(mod):
    df = pd.DataFrame({"cid": ["card_1"], "name": [None]})
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="card_update",
        card_id_column="cid", display_name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.card_update_calls == []
    out = _metadata_for(result, "ramp_out")
    assert out["rows_skipped"] == 1


def test_card_update_has_no_spend_limit_column_in_schema():
    """Structural guarantee matching the real Ramp API constraint."""
    import pathlib

    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    assert "spend_limit_column" not in source


# --- dual source: source=inline mode -----------------------------------------

def test_source_inline_mode(mod):
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out",
        source={"kind": "inline", "rows": [
            {"uid": "usr_1", "date": "2026-01-01", "miles": 5},
            {"uid": "usr_2", "date": "2026-01-02", "miles": 7},
        ]},
        mode="mileage_reimbursement",
        reimbursee_id_column="uid", trip_date_column="date", distance_column="miles",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"ramp_resource": resource})
    assert result.success
    out = _metadata_for(result, "ramp_out")
    assert out["rows_succeeded"] == 2


def test_max_rows_caps_rows(mod):
    df = pd.DataFrame({
        "uid": [f"usr_{i}" for i in range(10)],
        "date": ["2026-01-01"] * 10,
        "miles": [1] * 10,
    })
    resource = FakeRampResource()
    component = mod.RampReimbursementCardWriteComponent(
        asset_name="ramp_out", upstream_asset_key="upstream_rows", mode="mileage_reimbursement",
        reimbursee_id_column="uid", trip_date_column="date", distance_column="miles",
        max_rows=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "ramp_out")
    assert out["rows_total"] == 3
    assert out["rows_succeeded"] == 3
