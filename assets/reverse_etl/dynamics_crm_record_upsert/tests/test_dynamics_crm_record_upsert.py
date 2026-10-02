"""Committed regression tests for DynamicsCrmRecordUpsertComponent.

The real `dynamics_crm_resource` is never imported here -- a minimal fake
resource (conftest.py) stands in for the one external, paid-API boundary
(`upsert_by_key`), while everything this component actually owns -- dual
source resolution, validation, per-row body construction (including
excluding the alternate-key value from the body), action counting, error
collection, and metadata -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeDynamicsCrmResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_accounts", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"dynamics_crm": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata directly."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.DynamicsCrmRecordUpsertComponent(
            asset_name="x",
            entity_set_name="accounts",
            alternate_key_field="cr_external_id",
            fields_map={"account_id": "cr_external_id"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.DynamicsCrmRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            entity_set_name="accounts",
            alternate_key_field="cr_external_id",
            fields_map={"account_id": "cr_external_id"},
        ).build_defs(context=None)


def test_alternate_key_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.DynamicsCrmRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            entity_set_name="accounts",
            alternate_key_field="cr_external_id",
            fields_map={"name": "name"},
        ).build_defs(context=None)


# --- full asset body, against the fake resource ---------------------------

def test_created_updated_and_unknown_are_counted(mod):
    df = pd.DataFrame(
        {
            "account_id": ["ext-1", "ext-2", "ext-3"],
            "name": ["Acme", "Globex", "Initech"],
        }
    )
    resource = FakeDynamicsCrmResource(actions=["created", "updated", "unknown"])
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="dynamics_crm_accounts_mirror",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.calls) == 3
    out = _metadata_for(result, "dynamics_crm_accounts_mirror")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_unknown_action"] == 1
    assert out["rows_upserted"] == 3
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_key"] == 0
    assert out["dynamics_entity_set"] == "accounts"
    assert out["alternate_key_field"] == "cr_external_id"


def test_alternate_key_value_excluded_from_body(mod):
    df = pd.DataFrame({"account_id": ["ext-1"], "name": ["Acme"]})
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    call = resource.calls[0]
    assert call["entity_set"] == "accounts"
    assert call["key_name"] == "cr_external_id"
    assert call["key_value"] == "ext-1"
    # The alternate key's own value must NOT be duplicated into the body.
    assert "cr_external_id" not in call["body"]
    assert call["body"] == {"name": "Acme"}
    assert call["prefer_representation"] is True


def test_rows_with_no_key_are_skipped_and_counted(mod):
    df = pd.DataFrame({"account_id": ["ext-1", None, ""], "name": ["Acme", "Globex", "Initech"]})
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    # Empty string "" is a value (not None/NaN) -- only the real None is skipped.
    assert len(resource.calls) == 2
    out = _metadata_for(result, "out")
    assert out["rows_skipped_no_key"] == 1
    assert out["rows_upserted"] == 2


def test_none_and_nan_values_omitted_from_body(mod):
    df = pd.DataFrame(
        {
            "account_id": ["ext-1"],
            "name": ["Acme"],
            "industry": [None],
            "revenue": [float("nan")],
        }
    )
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={
            "account_id": "cr_external_id",
            "name": "name",
            "industry": "industrycode",
            "revenue": "revenue",
        },
    )
    result = _materialize(component, df, resource)
    assert result.success
    call = resource.calls[0]
    assert call["body"] == {"name": "Acme"}


def test_errors_are_collected_and_counted(mod):
    df = pd.DataFrame({"account_id": ["ext-1", "ext-2"], "name": ["Acme", "Globex"]})
    resource = FakeDynamicsCrmResource(raise_on={"ext-2"})
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "out")
    assert out["rows_created"] == 1
    assert out["rows_errored"] == 1
    assert "first_errors" in out
    assert "ext-2" in out["first_errors"][0]


def test_missing_required_columns_raises_failure(mod):
    df = pd.DataFrame({"name": ["Acme"]})  # missing account_id
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    with pytest.raises(Exception, match="Columns not in upstream"):
        _materialize(component, df, resource)


def test_empty_upstream_short_circuits(mod):
    df = pd.DataFrame({"account_id": [], "name": []})
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.calls == []
    out = _metadata_for(result, "out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"account_id": [f"ext-{i}" for i in range(10)], "name": [f"n{i}" for i in range(10)]})
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.calls) == 3
    out = _metadata_for(result, "out")
    assert out["rows_upserted"] == 3


def test_prefer_representation_false_is_passed_through_and_reported_unknown(mod):
    df = pd.DataFrame({"account_id": ["ext-1"], "name": ["Acme"]})
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        upstream_asset_key="upstream_accounts",
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
        prefer_representation=False,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.calls[0]["prefer_representation"] is False
    out = _metadata_for(result, "out")
    assert out["rows_unknown_action"] == 1
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 0


def test_source_inline_mode(mod):
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        source={"kind": "inline", "rows": [{"account_id": "ext-1", "name": "Acme"}, {"account_id": "ext-2", "name": "Globex"}]},
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"dynamics_crm": resource})
    assert result.success
    out = _metadata_for(result, "out")
    assert out["rows_upserted"] == 2


def test_source_csv_mode(mod, tmp_path):
    csv_path = tmp_path / "accounts.csv"
    csv_path.write_text("account_id,name\next-1,Acme\next-2,Globex\n")
    resource = FakeDynamicsCrmResource()
    component = mod.DynamicsCrmRecordUpsertComponent(
        asset_name="out",
        source={"kind": "csv", "path": str(csv_path)},
        entity_set_name="accounts",
        alternate_key_field="cr_external_id",
        fields_map={"account_id": "cr_external_id", "name": "name"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"dynamics_crm": resource})
    assert result.success
    out = _metadata_for(result, "out")
    assert out["rows_upserted"] == 2
