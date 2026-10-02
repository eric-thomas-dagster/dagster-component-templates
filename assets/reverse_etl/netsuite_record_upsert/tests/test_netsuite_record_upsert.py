"""Committed regression tests for NetSuiteRecordUpsertComponent.

The real NetSuite API is never called here -- `_netsuite_lookup_customer`
and `_netsuite_write_customer` (the two external, paid-API boundaries) are
monkeypatched wholesale, while dual source resolution, fields_map
application, lookup_field validation, the GET-then-PATCH-or-POST
create/update branching, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeNetSuiteResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource=None):
    resource = resource or FakeNetSuiteResource()
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"netsuite": resource})


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.NetSuiteRecordUpsertComponent(
            asset_name="x",
            fields_map={"customer_id": "externalId"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.NetSuiteRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"customer_id": "externalId"},
        ).build_defs(context=None)


def test_lookup_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.NetSuiteRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            lookup_field="externalId",
            fields_map={"company_name": "companyName"},
        ).build_defs(context=None)


def test_default_lookup_field_is_external_id(mod):
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="x",
        upstream_asset_key="foo",
        fields_map={"customer_id": "externalId"},
    )
    assert component.lookup_field == "externalId"
    component.build_defs(context=None)


def test_missing_upstream_columns_raises(mod):
    df = pd.DataFrame({"customer_id": ["EXT1"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId", "company_name": "companyName"},
    )
    with pytest.raises(Exception):
        _materialize(component, df)


# --- lookup-then-write branching (the core mechanic) ------------------------

def test_lookup_miss_creates_via_post(mod, monkeypatch):
    lookup_calls = []
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        lookup_calls.append((lookup_field, lookup_value))
        return None  # no existing match

    def _fake_write(resource, internal_id, body):
        write_calls.append((internal_id, body))
        return {"id": "999", "action": "created"}

    monkeypatch.setattr(mod, "_netsuite_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_netsuite_write_customer", _fake_write)

    df = pd.DataFrame({"customer_id": ["EXT1"], "company_name": ["Acme"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId", "company_name": "companyName"},
    )
    result = _materialize(component, df)
    assert result.success

    assert lookup_calls == [("externalId", "EXT1")]
    assert write_calls[0][0] is None  # internal_id=None signals create (POST)
    assert write_calls[0][1] == {"externalId": "EXT1", "companyName": "Acme"}

    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["api_requests"] == 2  # 1 lookup + 1 write


def test_lookup_hit_updates_via_patch(mod, monkeypatch):
    write_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        return "internal-123"  # existing match

    def _fake_write(resource, internal_id, body):
        write_calls.append((internal_id, body))
        return {"id": internal_id, "action": "updated"}

    monkeypatch.setattr(mod, "_netsuite_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_netsuite_write_customer", _fake_write)

    df = pd.DataFrame({"customer_id": ["EXT1"], "company_name": ["Acme Updated"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId", "company_name": "companyName"},
    )
    result = _materialize(component, df)
    assert result.success

    assert write_calls[0][0] == "internal-123"  # internal_id set -> PATCH path

    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_mixed_create_and_update_counted_separately(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        return "existing-id" if lookup_value == "EXT_EXISTING" else None

    def _fake_write(resource, internal_id, body):
        return {"id": internal_id or "new-id", "action": "updated" if internal_id else "created"}

    monkeypatch.setattr(mod, "_netsuite_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_netsuite_write_customer", _fake_write)

    df = pd.DataFrame({"customer_id": ["EXT_NEW", "EXT_EXISTING"], "company_name": ["New Co", "Existing Co"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId", "company_name": "companyName"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2


# --- missing lookup value / empty upstream ----------------------------------

def test_rows_missing_lookup_value_skipped_and_counted(mod, monkeypatch):
    calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        calls.append(lookup_value)
        return None

    def _fake_write(resource, internal_id, body):
        return {"id": "x", "action": "created"}

    monkeypatch.setattr(mod, "_netsuite_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_netsuite_write_customer", _fake_write)

    df = pd.DataFrame({"customer_id": ["EXT1", None], "company_name": ["Acme", "Orphan"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId", "company_name": "companyName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert calls == ["EXT1"]
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_skipped_no_key"] == 1


def test_empty_upstream_short_circuits_without_api_calls(mod, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "_netsuite_lookup_customer", lambda *a, **k: called.append(1))
    monkeypatch.setattr(mod, "_netsuite_write_customer", lambda *a, **k: called.append(1))

    df = pd.DataFrame({"customer_id": [], "company_name": []})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId", "company_name": "companyName"},
    )
    result = _materialize(component, df)
    assert result.success
    assert called == []
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod, monkeypatch):
    monkeypatch.setattr(mod, "_netsuite_lookup_customer", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_netsuite_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": [f"EXT{i}" for i in range(10)]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId"},
        batch_size=3,
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    monkeypatch.setattr(mod, "_netsuite_lookup_customer", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_netsuite_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        source={"kind": "inline", "rows": [{"customer_id": "EXT1"}, {"customer_id": "EXT2"}]},
        fields_map={"customer_id": "externalId"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"netsuite": FakeNetSuiteResource()})
    assert result.success
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_upserted"] == 2


def test_lookup_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_lookup(resource, lookup_field, lookup_value):
        raise RuntimeError("NetSuite 500")

    monkeypatch.setattr(mod, "_netsuite_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_netsuite_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"customer_id": ["EXT1"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_errored"] == 1
    assert "NetSuite 500" in out["first_errors"][0]


def test_write_exception_recorded_as_error_not_raised(mod, monkeypatch):
    monkeypatch.setattr(mod, "_netsuite_lookup_customer", lambda *a, **k: None)

    def _fake_write(resource, internal_id, body):
        raise RuntimeError("validation error on companyName")

    monkeypatch.setattr(mod, "_netsuite_write_customer", _fake_write)

    df = pd.DataFrame({"customer_id": ["EXT1"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        fields_map={"customer_id": "externalId"},
    )
    result = _materialize(component, df)
    assert result.success
    out = metadata_for(result, "netsuite_customer_upsert_out")
    assert out["rows_errored"] == 1
    assert "validation error" in out["first_errors"][0]


def test_custom_lookup_field_other_than_external_id(mod, monkeypatch):
    lookup_calls = []

    def _fake_lookup(resource, lookup_field, lookup_value):
        lookup_calls.append((lookup_field, lookup_value))
        return None

    monkeypatch.setattr(mod, "_netsuite_lookup_customer", _fake_lookup)
    monkeypatch.setattr(mod, "_netsuite_write_customer", lambda *a, **k: {"id": "x", "action": "created"})

    df = pd.DataFrame({"entity_name": ["Acme Corp"]})
    component = mod.NetSuiteRecordUpsertComponent(
        asset_name="netsuite_customer_upsert_out",
        upstream_asset_key="upstream_customers",
        lookup_field="entityid",
        fields_map={"entity_name": "entityid"},
    )
    result = _materialize(component, df)
    assert result.success
    assert lookup_calls == [("entityid", "Acme Corp")]
