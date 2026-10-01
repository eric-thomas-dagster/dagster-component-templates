"""Committed regression tests for the changes made to
EnrichedDbtCloudWorkspaceComponent in response to a customer report that
dbt-mesh cross-project lineage overrides were fragile, and that automation
conditions could only be changed by patching the component:

1. Per-model `meta.dagster.automation_condition` support -- this component
   previously had NONE at all (unlike its Core sibling); the base
   DbtCloudComponent's translator only reads the older, narrower
   `meta.dagster.auto_materialize_policy: {type: eager|lazy}` shape.
2. `default_automation_condition` -- a YAML-settable fallback, lowest
   precedence (per-model meta, freshness-failure, lag-tolerance all win).
3. `external_packages` mesh stub keys now go through this component's own
   configured translator (`get_asset_spec`) instead of a bare model-name
   guess, so they respect schema nesting like every other dagster-dbt key.
4. `asset_overrides` can be keyed by dbt `unique_id` in addition to the
   serialized AssetKey string.
"""
import dagster as dg
import pytest

from .conftest import load_component_module, make_component, requires_dagster_dbt

pytestmark = requires_dagster_dbt


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def component(mod):
    from dagster_dbt import DbtCloudWorkspace

    workspace = DbtCloudWorkspace(account_id=1, token="fake-token", project_id=1, environment_id=1)
    return make_component(mod, workspace=workspace)


def _model_spec(mod, unique_id: str, key=("my_model",)) -> dg.AssetSpec:
    return dg.AssetSpec(key=dg.AssetKey(list(key)), metadata={mod._UNIQUE_ID_KEY: unique_id})


# --- per-model meta.dagster.automation_condition (new capability) ------

def test_per_model_meta_automation_condition_now_works(mod, component):
    """Previously a no-op in this component -- the only way to set a
    per-model automation condition was patching the component itself."""
    manifest = {
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "unique_id": "model.test_project.my_model",
                "meta": {"dagster": {"automation_condition": {"preset": "on_missing"}}},
                "config": {},
            }
        }
    }
    spec = _model_spec(mod, "model.test_project.my_model")
    enriched = component._enrich_spec(spec, manifest)
    assert enriched.automation_condition == dg.AutomationCondition.on_missing()


# --- default_automation_condition precedence ---------------------------

def test_default_automation_condition_applies_when_nothing_else_set(mod, component):
    component.default_automation_condition = {"preset": "eager"}
    manifest = {
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "unique_id": "model.test_project.my_model",
                "meta": {},
                "config": {},
            }
        }
    }
    spec = _model_spec(mod, "model.test_project.my_model")
    enriched = component._enrich_spec(spec, manifest)
    assert enriched.automation_condition == dg.AutomationCondition.eager()


def test_default_automation_condition_does_not_override_per_model_meta(mod, component):
    component.default_automation_condition = {"preset": "eager"}
    manifest = {
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "unique_id": "model.test_project.my_model",
                "meta": {"dagster": {"automation_condition": {"preset": "on_missing"}}},
                "config": {},
            }
        }
    }
    spec = _model_spec(mod, "model.test_project.my_model")
    enriched = component._enrich_spec(spec, manifest)
    assert enriched.automation_condition == dg.AutomationCondition.on_missing()


def test_default_automation_condition_does_not_override_freshness_failed(mod, component):
    component.derive_freshness_policies = True
    component.auto_trigger_on_freshness_failure = True
    component.default_automation_condition = {"preset": "eager"}
    manifest = {
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "unique_id": "model.test_project.my_model",
                "meta": {},
                "config": {"freshness": {"build_after": {"count": 1, "period": "hour"}}},
            }
        }
    }
    spec = _model_spec(mod, "model.test_project.my_model")
    enriched = component._enrich_spec(spec, manifest)
    assert enriched.automation_condition == dg.AutomationCondition.freshness_failed()


def test_no_default_automation_condition_leaves_it_unset(mod, component):
    manifest = {
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "unique_id": "model.test_project.my_model",
                "meta": {},
                "config": {},
            }
        }
    }
    spec = _model_spec(mod, "model.test_project.my_model")
    enriched = component._enrich_spec(spec, manifest)
    assert enriched.automation_condition is None


# --- external_packages mesh stub key fix --------------------------------

def test_external_package_stub_key_respects_configured_schema(mod, component):
    """Regression test for the actual reported bug: the old bare-alias
    fallback ignored a model's configured schema entirely, so the stub key
    never matched the upstream project's real, schema-nested published key."""
    component.external_packages = ["shared_core"]
    manifest = {
        "nodes": {
            "model.shared_core.customer_summary": {
                "resource_type": "model",
                "unique_id": "model.shared_core.customer_summary",
                "package_name": "shared_core",
                "name": "customer_summary",
                "alias": "customer_summary",
                "description": "Shared customer summary.",
                "meta": {},
                "config": {"schema": "shared"},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "customer_summary"],
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["shared", "customer_summary"])


def test_external_package_stub_key_honors_meta_asset_key_override(mod, component):
    component.external_packages = ["shared_core"]
    manifest = {
        "nodes": {
            "model.shared_core.customer_summary": {
                "resource_type": "model",
                "unique_id": "model.shared_core.customer_summary",
                "package_name": "shared_core",
                "name": "customer_summary",
                "alias": "customer_summary",
                "meta": {"dagster": {"asset_key": ["custom", "key"]}},
                "config": {"schema": "shared"},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "customer_summary"],
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["custom", "key"])


def test_external_package_stub_key_no_schema_matches_bare_name(mod, component):
    """No configured schema: translator and the old bare-alias fallback
    agree -- confirms the fix is a no-op for the common unnested case."""
    component.external_packages = ["shared_core"]
    manifest = {
        "nodes": {
            "model.shared_core.widget": {
                "resource_type": "model",
                "unique_id": "model.shared_core.widget",
                "package_name": "shared_core",
                "name": "widget",
                "alias": "widget",
                "meta": {},
                "config": {},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "widget"],
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["widget"])


# --- asset_overrides unique_id lookup -----------------------------------

def test_resolve_override_deps_by_unique_id(mod):
    overrides = {
        "model.shared_core.customer_summary": mod.AssetOverride(depends_on=["ext/thing"]),
    }
    result = mod._resolve_override_deps(
        overrides, lookup_key="shared/customer_summary", unique_id="model.shared_core.customer_summary"
    )
    assert result == [dg.AssetKey(["ext", "thing"])]


def test_resolve_override_deps_by_asset_key_string_still_works(mod):
    overrides = {
        "shared/customer_summary": mod.AssetOverride(depends_on=["ext/thing"]),
    }
    result = mod._resolve_override_deps(
        overrides, lookup_key="shared/customer_summary", unique_id="model.shared_core.customer_summary"
    )
    assert result == [dg.AssetKey(["ext", "thing"])]
