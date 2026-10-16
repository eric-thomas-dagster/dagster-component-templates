"""Committed regression tests for EnrichedDbtProjectComponent, covering:

1. `default_automation_condition` -- a YAML-settable fallback, lowest
   precedence (per-model meta, freshness-failure, lag-tolerance all win).
2. `external_packages` mesh stub keys now go through this project's own
   configured translator (`get_asset_spec`) instead of a bare model-name
   guess, so they respect schema nesting like every other dagster-dbt key.
3. `asset_overrides` can be keyed by dbt `unique_id` in addition to the
   serialized AssetKey string, and its `asset_key:` field can force a
   mesh stub's own key to a specific value when the two projects'
   manifests resolve the same model's `config.schema` differently (a
   real customer-reported cross-manifest mismatch the translator fix
   alone can't close). `external_package_schemas` fills in a missing
   schema for a WHOLE package at once (scales past one-override-per-
   model). `external_package_manifest_paths` is the fully correct fix
   when reachable: read the upstream package's REAL manifest.json and
   compute the stub key by running the translator against it directly --
   exact parity, zero guessing.
4. `_build_child_map`: dbt v2 / Fusion manifests omit the top-level
   `child_map` key entirely (confirmed against dagster_dbt's own identical
   workaround) -- include_exposures/include_metrics/include_semantic_models
   must still find their children by walking `depends_on.nodes` instead.
5. `_patch_sqlglot_column_lineage_compat`: a shim for dagster-io/dagster#34098
   (unmerged as of writing; also submitted as dagster-io/internal#27143).
   Applies once, is idempotent on repeat calls/module reloads, and no-ops
   once the installed dagster-dbt already has the real fix.
6. `_patch_dbt_selector_for_missing_config`: a real customer bug (hooks
   with no `config` block crashed `select`/`exclude` resolution with
   `AttributeError: 'NoneType' object has no attribute 'enabled'` --
   dagster_dbt's manifest-selection shim doesn't default `config` the way
   dbt-core's own node parsing does). Applies once, idempotent, fixes the
   exact crash end-to-end via `dagster_dbt.utils._select_unique_ids_from_manifest`.
"""
import json
from pathlib import Path

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


# --- asset_overrides.asset_key: forcing a mismatched mesh stub key ------
#
# Real customer-reported bug: a downstream ("gold") project's manifest can
# legitimately resolve an upstream ("silver") model's config.schema
# differently than the upstream project's own manifest does (two separate
# `dbt parse` runs, same translator logic, different compiled config data)
# -- e.g. gold's compiled node for a mesh model has no config.schema at
# all, so the stub falls back to a bare name, while silver's own code
# location publishes a schema-nested key for the SAME model. Dagster then
# sees two unrelated assets and cross-project lineage silently breaks.
# `asset_overrides` keyed by unique_id, with an `asset_key:` entry, forces
# the stub to the correct value.

def test_external_package_stub_key_forced_via_asset_override(mod, component):
    """The translator computes a bare "filtered_usage_events" key (no
    config.schema on this node, matching the real customer report), but
    asset_overrides forces it to match the upstream project's real
    published key."""
    component.external_packages = ["shared_core"]
    component.asset_overrides = {
        "model.shared_core.filtered_usage_events": mod.AssetOverride(
            asset_key="silver_cursorapi/filtered_usage_events"
        ),
    }
    manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {},  # no schema -- translator alone would compute a bare key
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["silver_cursorapi", "filtered_usage_events"])


def test_external_package_stub_key_without_override_stays_bare(mod, component):
    """Non-regression: without an asset_overrides entry, the translator's
    own (possibly-mismatched) key is unchanged -- the override is additive,
    not a behavior change for every other mesh stub."""
    component.external_packages = ["shared_core"]
    manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["filtered_usage_events"])


def test_resolve_override_asset_key_by_unique_id(mod):
    overrides = {
        "model.shared_core.filtered_usage_events": mod.AssetOverride(
            asset_key="silver_cursorapi/filtered_usage_events"
        ),
    }
    result = mod._resolve_override_asset_key(
        overrides, lookup_key="filtered_usage_events", unique_id="model.shared_core.filtered_usage_events"
    )
    assert result == dg.AssetKey(["silver_cursorapi", "filtered_usage_events"])


def test_resolve_override_asset_key_by_computed_key_string_still_works(mod):
    overrides = {
        "filtered_usage_events": mod.AssetOverride(asset_key="silver_cursorapi/filtered_usage_events"),
    }
    result = mod._resolve_override_asset_key(
        overrides, lookup_key="filtered_usage_events", unique_id="model.shared_core.filtered_usage_events"
    )
    assert result == dg.AssetKey(["silver_cursorapi", "filtered_usage_events"])


def test_resolve_override_asset_key_no_match_returns_none(mod):
    overrides = {"other/key": mod.AssetOverride(asset_key="x/y")}
    result = mod._resolve_override_asset_key(overrides, lookup_key="filtered_usage_events", unique_id=None)
    assert result is None


# --- external_package_manifest_paths: the real-manifest fix ------------
#
# The most correct fix for the mesh key mismatch: when silver's own
# manifest.json is reachable from gold's build/deploy environment (shared
# artifact store, shared filesystem, etc.), read it directly and compute
# the stub key by running the translator against that REAL manifest
# instead of gold's own incomplete copy of the node -- exact parity by
# construction, no per-model or per-package guessing.

def test_external_package_manifest_path_uses_real_manifest_for_key(mod, component, tmp_path):
    """The real manifest has config.schema set (silver's own compile
    resolved it); gold's own copy of the node does NOT. Confirms the stub
    key comes from the REAL manifest file, not gold's incomplete one."""
    real_silver_manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {"schema": "silver_cursorapi"},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            }
        }
    }
    manifest_path = tmp_path / "silver_manifest.json"
    manifest_path.write_text(json.dumps(real_silver_manifest))

    component.external_packages = ["shared_core"]
    component.external_package_manifest_paths = {"shared_core": str(manifest_path)}
    gold_manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {},  # gold's own incomplete copy -- no schema
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            }
        }
    }
    specs = component._build_external_package_specs(gold_manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["silver_cursorapi", "filtered_usage_events"])


def test_external_package_manifest_path_missing_file_falls_back_gracefully(mod, component, tmp_path):
    """A missing/unreadable manifest file must not crash spec building --
    falls back to the existing schema-default/bare-translator path."""
    component.external_packages = ["shared_core"]
    component.external_package_manifest_paths = {"shared_core": str(tmp_path / "does_not_exist.json")}
    component.external_package_schemas = {"shared_core": "silver_cursorapi"}
    gold_manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            }
        }
    }
    specs = component._build_external_package_specs(gold_manifest)
    assert len(specs) == 1
    # Falls through to the schema-default mechanism, not a crash.
    assert specs[0].key == dg.AssetKey(["silver_cursorapi", "filtered_usage_events"])


def test_external_package_manifest_path_is_cached_per_package(mod, component, tmp_path):
    """Reading + parsing the real manifest file happens at most once per
    package per component instance, not once per model."""
    manifest_path = tmp_path / "silver_manifest.json"
    manifest_path.write_text(json.dumps({"nodes": {}}))

    read_calls = []
    real_read_text = Path.read_text

    def _counting_read_text(self, *args, **kwargs):
        if self == manifest_path:
            read_calls.append(1)
        return real_read_text(self, *args, **kwargs)

    import unittest.mock as mock
    with mock.patch.object(Path, "read_text", _counting_read_text):
        component.external_packages = ["shared_core"]
        component.external_package_manifest_paths = {"shared_core": str(manifest_path)}
        gold_manifest = {
            "nodes": {
                f"model.shared_core.model_{i}": {
                    "resource_type": "model",
                    "unique_id": f"model.shared_core.model_{i}",
                    "package_name": "shared_core",
                    "name": f"model_{i}",
                    "alias": f"model_{i}",
                    "meta": {},
                    "config": {},
                    "depends_on": {"nodes": []},
                    "fqn": ["shared_core", f"model_{i}"],
                    "original_file_path": f"models/model_{i}.sql",
                }
                for i in range(3)
            }
        }
        component._build_external_package_specs(gold_manifest)
    assert len(read_calls) == 1


def test_external_package_schema_default_fills_in_missing_schema(mod, component):
    """Scales the single-model asset_key override to a whole package: when
    EVERY model in the package is missing config.schema (the common mesh
    case, not just one or two models), one package-level default fixes all
    of them in one line instead of one asset_overrides entry per model."""
    component.external_packages = ["shared_core"]
    component.external_package_schemas = {"shared_core": "silver_cursorapi"}
    manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {},  # no schema at all
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            },
            "model.shared_core.other_model": {
                "resource_type": "model",
                "unique_id": "model.shared_core.other_model",
                "package_name": "shared_core",
                "name": "other_model",
                "alias": "other_model",
                "meta": {},
                "config": {},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "other_model"],
                "original_file_path": "models/other_model.sql",
            },
        }
    }
    specs = component._build_external_package_specs(manifest)
    keys = {spec.key for spec in specs}
    assert keys == {
        dg.AssetKey(["silver_cursorapi", "filtered_usage_events"]),
        dg.AssetKey(["silver_cursorapi", "other_model"]),
    }


def test_external_package_schema_default_does_not_override_existing_schema(mod, component):
    """A model that DOES already carry its own config.schema keeps it --
    the package default only fills in what's missing, it never clobbers
    real data."""
    component.external_packages = ["shared_core"]
    component.external_package_schemas = {"shared_core": "silver_cursorapi"}
    manifest = {
        "nodes": {
            "model.shared_core.customer_summary": {
                "resource_type": "model",
                "unique_id": "model.shared_core.customer_summary",
                "package_name": "shared_core",
                "name": "customer_summary",
                "alias": "customer_summary",
                "meta": {},
                "config": {"schema": "already_set"},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "customer_summary"],
                "original_file_path": "models/customer_summary.sql",
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["already_set", "customer_summary"])


def test_asset_override_asset_key_wins_over_package_schema_default(mod, component):
    """Precedence check: when both a package-level schema default AND a
    specific asset_overrides.asset_key apply to the same model, the more
    specific per-model override wins."""
    component.external_packages = ["shared_core"]
    component.external_package_schemas = {"shared_core": "silver_cursorapi"}
    component.asset_overrides = {
        "model.shared_core.filtered_usage_events": mod.AssetOverride(
            asset_key="totally_different/key"
        ),
    }
    manifest = {
        "nodes": {
            "model.shared_core.filtered_usage_events": {
                "resource_type": "model",
                "unique_id": "model.shared_core.filtered_usage_events",
                "package_name": "shared_core",
                "name": "filtered_usage_events",
                "alias": "filtered_usage_events",
                "meta": {},
                "config": {},
                "depends_on": {"nodes": []},
                "fqn": ["shared_core", "filtered_usage_events"],
                "original_file_path": "models/filtered_usage_events.sql",
            }
        }
    }
    specs = component._build_external_package_specs(manifest)
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["totally_different", "key"])


def test_resolve_override_asset_key_ignores_depends_on_only_overrides(mod):
    """An override entry with only `depends_on` (no `asset_key`) must not
    be mistaken for a key override -- these two features share the same
    override dict but are independent."""
    overrides = {
        "model.shared_core.filtered_usage_events": mod.AssetOverride(depends_on=["fx_rates"]),
    }
    result = mod._resolve_override_asset_key(
        overrides, lookup_key="filtered_usage_events", unique_id="model.shared_core.filtered_usage_events"
    )
    assert result is None


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


# --- _patch_dbt_selector_for_missing_config -----------------------------

def test_dbt_selector_patch_applies_and_is_idempotent(mod):
    import dbt.graph.selector as dbt_selector_mod

    # The module fixture already imported this component once (patching at
    # module-load time), so the patch should already be in place.
    assert getattr(
        dbt_selector_mod.NodeSelector._is_graph_member, "_dcc_patched", False
    )
    patched_fn = dbt_selector_mod.NodeSelector._is_graph_member

    # Calling it again (simulating a second component instance loading in
    # the same process) must be a safe no-op, not a second wrap.
    mod._patch_dbt_selector_for_missing_config()
    assert dbt_selector_mod.NodeSelector._is_graph_member is patched_fn


def test_dbt_selector_patch_fixes_select_exclude_against_manifest_with_hook_nodes(mod):
    """Reproduces the real customer crash (KDP, 2026-10-14): a manifest
    with an on-run-end hook ("operation") node -- which dbt-core itself
    never gives a `config` block in manifest.json, relying on its own
    dataclass default of `NodeConfig(enabled=True)` -- crashed
    dagster_dbt's `select`/`exclude` resolution the moment ANY selector
    was configured, because dagster_dbt's manifest shim returns bare
    `None` instead of a default-enabled config for a node with no `config`
    key. Confirmed via direct reproduction against the installed
    dagster-dbt/dbt-core before this patch existed.
    """
    from dagster_dbt.utils import _select_unique_ids_from_manifest

    manifest_json = {
        "nodes": {
            "model.pkg.my_model": {
                "resource_type": "model",
                "unique_id": "model.pkg.my_model",
                "config": {"enabled": True},
                "fqn": ["pkg", "my_model"],
                "name": "my_model",
                "package_name": "pkg",
                "path": "my_model.sql",
                "original_file_path": "models/my_model.sql",
                "tags": [],
            },
            "operation.pkg.pkg-on-run-end-0": {
                # No "config" key at all -- matches a real manifest.json's
                # hook/operation nodes.
                "resource_type": "operation",
                "unique_id": "operation.pkg.pkg-on-run-end-0",
                "fqn": ["pkg", "pkg-on-run-end-0"],
                "name": "pkg-on-run-end-0",
                "package_name": "pkg",
                "path": "hooks.sql",
                "original_file_path": "hooks.sql",
                "tags": [],
            },
        },
        "sources": {}, "metrics": {}, "exposures": {}, "semantic_models": {},
        "saved_queries": {}, "selectors": {}, "unit_tests": {}, "functions": {},
        "child_map": {"model.pkg.my_model": [], "operation.pkg.pkg-on-run-end-0": []},
        "parent_map": {"model.pkg.my_model": [], "operation.pkg.pkg-on-run-end-0": []},
    }

    selected = _select_unique_ids_from_manifest(
        select="fqn:*",
        exclude="package:some_other_pkg",
        selector=None,
        manifest_json=manifest_json,
    )

    # Both the real model and the hook node are selected (neither matches
    # the exclude), same as dbt-core's own real parse would produce.
    assert selected == {"model.pkg.my_model", "operation.pkg.pkg-on-run-end-0"}
