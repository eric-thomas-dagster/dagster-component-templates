"""The pipeline's `context_policy:` and the receipt store's @context_asset share one declaration, so a
gateway reading context_policies() sees the same thing either way. This pins them together."""
import importlib.util
from dataclasses import fields
from pathlib import Path

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def receipts():
    path = Path(__file__).parents[4] / "resources/context_receipt_store/component.py"
    spec = importlib.util.spec_from_file_location("receipts_for_policy_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_declaration_cannot_drift_between_pipeline_and_decorator(mod, receipts):
    assert mod._CONTEXT_POLICY_METADATA_KEY == receipts.CONTEXT_POLICY_METADATA_KEY
    assert mod._CONTEXT_POLICY_FIELDS == {f.name for f in fields(receipts.ContextPolicy)}
    assert mod._SENSITIVITY_LEVELS == receipts.SENSITIVITY_LEVELS


def _with_raw(defs):
    import dagster as dg

    @dg.asset(name="raw")
    def raw():
        return 1

    return dg.Definitions.merge(defs, dg.Definitions(assets=[raw]))


def _component(mod, **kw):
    return mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
        steps=[{"id": "c", "op": "chunk"}], **kw)


def test_pipeline_policy_is_discoverable_by_the_gateway_reader(mod, receipts):
    import dagster as dg
    defs = _component(mod, context_policy={"scope": "customer_id", "sensitivity": "confidential",
                                           "token_budget": 3000, "access_tags": ["support"]}).build_defs(None)
    policies = receipts.context_policies(_with_raw(defs))
    assert policies == {"kb": receipts.ContextPolicy(scope="customer_id", sensitivity="confidential",
                                                     token_budget=3000, access_tags=("support",))}
    spec = list(defs.assets)[0].get_asset_spec(dg.AssetKey("kb"))
    assert spec.tags["context/sensitivity"] == "confidential" and spec.tags["context/scope"] == "customer_id"
    assert receipts.context_policies(_with_raw(_component(mod).build_defs(None))) == {}  # opt-in


@pytest.mark.parametrize("policy,match", [
    ({"bogus": 1}, "unknown field"),
    ({"sensitivity": "secret"}, "sensitivity"),
    ({"token_budget": 0}, "token_budget"),
])
def test_pipeline_rejects_invalid_policy(mod, policy, match):
    with pytest.raises(ValueError, match=match):
        _component(mod, context_policy=policy).build_defs(None)
