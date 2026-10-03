"""Committed regression tests for EnrichedDbtProjectComponent, covering:

1. `default_automation_condition` -- a YAML-settable fallback, lowest
   precedence (per-model meta, freshness-failure, lag-tolerance all win).
2. `external_packages` mesh stub keys now go through this project's own
   configured translator (`get_asset_spec`) instead of a bare model-name
   guess, so they respect schema nesting like every other dagster-dbt key.
3. `asset_overrides` can be keyed by dbt `unique_id` in addition to the
   serialized AssetKey string.
4. `_build_child_map`: dbt v2 / Fusion manifests omit the top-level
   `child_map` key entirely (confirmed against dagster_dbt's own identical
   workaround) -- include_exposures/include_metrics/include_semantic_models
   must still find their children by walking `depends_on.nodes` instead.
5. `_patch_sqlglot_column_lineage_compat`: a shim for dagster-io/dagster#34098
   (unmerged as of writing; also submitted as dagster-io/internal#27143).
   Applies once, is idempotent on repeat calls/module reloads, and no-ops
   once the installed dagster-dbt already has the real fix.
"""
import dagster as dg
import pytest

from .conftest import FIXTURE_PROJECT_DIR, load_component_module, requires_dagster_dbt

pytestmark = requires_dagster_dbt


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def component(mod):
    from dagster_dbt import DbtProject

    return mod.EnrichedDbtProjectComponent(
        project=DbtProject(project_dir=str(FIXTURE_PROJECT_DIR)),
    )


def _model_spec(mod, unique_id: str, key=("my_model",)) -> dg.AssetSpec:
    return dg.AssetSpec(key=dg.AssetKey(list(key)), metadata={mod._UNIQUE_ID_KEY: unique_id})


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
    assert enriched.freshness_policy is not None


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
    (e.g. "customer_summary") never matched the upstream project's real,
    schema-nested published key (e.g. "shared"/"customer_summary")."""
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
                "original_file_path": "models/customer_summary.sql",
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
                "original_file_path": "models/customer_summary.sql",
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
                "original_file_path": "models/widget.sql",
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


def test_resolve_override_deps_asset_key_string_wins_over_unique_id(mod):
    overrides = {
        "shared/customer_summary": mod.AssetOverride(depends_on=["ext/by_key"]),
        "model.shared_core.customer_summary": mod.AssetOverride(depends_on=["ext/by_uid"]),
    }
    result = mod._resolve_override_deps(
        overrides, lookup_key="shared/customer_summary", unique_id="model.shared_core.customer_summary"
    )
    assert result == [dg.AssetKey(["ext", "by_key"])]


def test_resolve_override_deps_no_match_returns_empty(mod):
    overrides = {"other/key": mod.AssetOverride(depends_on=["ext/thing"])}
    result = mod._resolve_override_deps(overrides, lookup_key="shared/customer_summary", unique_id=None)
    assert result == []


# --- _build_child_map: dbt v2 / Fusion manifests (no child_map key) -----

_FUSION_SHAPED_MANIFEST = {
    # No "child_map" key at all -- this is exactly what a dbt v2/Fusion
    # manifest looks like; dbt v1 manifests have always included it.
    "nodes": {
        "model.test_project.my_model": {
            "resource_type": "model",
            "unique_id": "model.test_project.my_model",
            "meta": {},
            "config": {},
            "depends_on": {"nodes": []},
        },
    },
    "exposures": {
        "exposure.test_project.my_dashboard": {
            "name": "my_dashboard",
            "type": "dashboard",
            "description": "A dashboard.",
            "maturity": "high",
            "owner": {"email": "a@b.com"},
            "depends_on": {"nodes": ["model.test_project.my_model"]},
        },
    },
}


def test_build_child_map_falls_back_when_manifest_omits_it(mod):
    child_map = mod._build_child_map(_FUSION_SHAPED_MANIFEST)
    assert child_map["model.test_project.my_model"] == ["exposure.test_project.my_dashboard"]


def test_build_child_map_prefers_manifests_own_child_map_when_present(mod):
    manifest = {**_FUSION_SHAPED_MANIFEST, "child_map": {"some_other_id": ["x"]}}
    assert mod._build_child_map(manifest) == {"some_other_id": ["x"]}


def test_include_exposures_still_works_against_a_fusion_shaped_manifest(mod, component):
    component.include_exposures = True
    spec = _model_spec(mod, "model.test_project.my_model")
    enriched = component._enrich_spec(spec, _FUSION_SHAPED_MANIFEST)
    exposures_md = enriched.metadata["dbt_docs/exposures"]
    exposures = exposures_md.value if hasattr(exposures_md, "value") else exposures_md
    assert len(exposures) == 1
    assert exposures[0]["name"] == "my_dashboard"


def test_sqlglot_lineage_patch_applies_and_is_idempotent(mod):
    import dagster_dbt.core.dbt_cli_event as cli_event_mod
    import dagster_dbt.core.dbt_event_iterator as iterator_mod

    # The module fixture already imported this component once (patching at
    # module-load time), so by the time this test runs the patch should
    # already be in place -- confirm that, then confirm calling it again
    # (simulating a second component instance loading in the same process)
    # is a safe no-op rather than crashing or double-wrapping.
    assert getattr(
        cli_event_mod._build_column_lineage_metadata,
        "_is_sqlglot_lineage_compat_patch",
        False,
    )
    assert cli_event_mod._build_column_lineage_metadata is iterator_mod._build_column_lineage_metadata

    mod._patch_sqlglot_column_lineage_compat()
    mod._patch_sqlglot_column_lineage_compat()

    assert getattr(
        cli_event_mod._build_column_lineage_metadata,
        "_is_sqlglot_lineage_compat_patch",
        False,
    )


def test_sqlglot_lineage_patch_no_ops_once_upstream_is_fixed(mod, monkeypatch):
    import dagster_dbt.core.dbt_cli_event as cli_event_mod
    import dagster_dbt.core.dbt_event_iterator as iterator_mod

    def _already_fixed_upstream(*args, **kwargs):
        """Stand-in for a hypothetical future dagster-dbt release that
        already contains the real fix -- its source text would include
        `optimized_node_sql`, same as our patched version does."""
        optimized_node_sql = None  # noqa: F841
        return {}

    monkeypatch.setattr(cli_event_mod, "_build_column_lineage_metadata", _already_fixed_upstream)
    monkeypatch.setattr(iterator_mod, "_build_column_lineage_metadata", _already_fixed_upstream)

    mod._patch_sqlglot_column_lineage_compat()

    # Must still be the exact function we set above -- the shim should have
    # detected the upstream fix (via the `optimized_node_sql` marker in its
    # source) and left it alone rather than wrapping it again.
    assert cli_event_mod._build_column_lineage_metadata is _already_fixed_upstream
    assert iterator_mod._build_column_lineage_metadata is _already_fixed_upstream
