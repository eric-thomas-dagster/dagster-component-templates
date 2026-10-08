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
5. `_build_child_map`: dbt v2 / Fusion manifests omit the top-level
   `child_map` key entirely (confirmed against dagster_dbt's own identical
   workaround) -- include_exposures/include_metrics/include_semantic_models
   must still find their children by walking `depends_on.nodes` instead.
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


# --- mirror_jobs: per-run config override + real materializations ------
#
# Previously the mirrored job's trigger op had no config_schema at all
# (steps_override could only be changed by editing job_trigger_defaults in
# the component's own YAML and redeploying -- the opposite of "re-run with
# a different selector for this run only, without editing the job
# definition"), AND emitted no AssetMaterializations for the dbt models it
# actually builds -- a job could genuinely succeed with zero asset tiles
# turning green. Both fixed: steps_override is a real per-run config
# (confirmed against the installed dagster-dbt client -- trigger_job_run
# only accepts job_id + steps_override, nothing else), and a successful run
# now materializes real asset keys derived from that run's own
# run_results.json + manifest.json via this component's own translator --
# not a hand-maintained, driftable asset-key list.

_FAKE_NODE = {
    "resource_type": "model",
    "name": "stg_site_details",
    "package_name": "fuel_and_trading",
    "fqn": ["fuel_and_trading", "staging", "stg_site_details"],
    "unique_id": "model.fuel_and_trading.stg_site_details",
    "config": {"schema": None},
}


class _FakeTriggerClient:
    """Records every trigger_job_run call; no real network access. Mirrors
    the real dagster-dbt cloud_v2 client's actual method signatures."""
    def __init__(self, run_status=10, result_status="success"):
        self.calls = []
        self._run_status = run_status  # DbtCloudJobRunStatusType.SUCCESS == 10
        self._result_status = result_status

    def trigger_job_run(self, job_id, steps_override=None):
        self.calls.append({"job_id": job_id, "steps_override": steps_override})
        return {"id": 999}

    def poll_run(self, run_id, poll_interval=None, poll_timeout=None):
        return {"id": run_id, "status": self._run_status}

    def get_run_results_json(self, run_id):
        return {"results": [{"unique_id": _FAKE_NODE["unique_id"], "status": self._result_status, "execution_time": 1.23}]}

    def get_run_manifest_json(self, run_id):
        return {"nodes": {_FAKE_NODE["unique_id"]: _FAKE_NODE}}


def _build_mirrored_job(mod, component, monkeypatch):
    monkeypatch.setattr(
        mod, "_list_dbt_cloud_jobs_via_client",
        lambda workspace: [{"id": 42, "name": "test_job", "job_type": "other"}],
    )
    defs = component._build_mirror_jobs_addendum()
    (job,) = defs.jobs
    return job


def _mirrored_component(mod, fake_client, **overrides):
    import types
    from dagster_dbt import DagsterDbtTranslator
    return make_component(
        mod,
        workspace=types.SimpleNamespace(client=fake_client),
        mirror_jobs="job",
        translator=DagsterDbtTranslator(),
        **overrides,
    )


def test_mirrored_job_materializes_real_models_from_run_results(mod, monkeypatch):
    fake_client = _FakeTriggerClient()
    component = _mirrored_component(mod, fake_client)
    job = _build_mirrored_job(mod, component, monkeypatch)

    result = job.execute_in_process()
    assert result.success
    assert fake_client.calls == [{"job_id": 42, "steps_override": None}]

    materializations = result.asset_materializations_for_node("trigger_dbt_cloud_test_job")
    assert len(materializations) == 1
    assert materializations[0].asset_key == dg.AssetKey(["stg_site_details"])


def test_mirrored_job_run_config_overrides_steps_without_editing_yaml(mod, monkeypatch):
    fake_client = _FakeTriggerClient()
    component = _mirrored_component(mod, fake_client, job_trigger_defaults={"steps_override": ["dbt build"]})
    job = _build_mirrored_job(mod, component, monkeypatch)

    result = job.execute_in_process(
        run_config={
            "ops": {
                "trigger_dbt_cloud_test_job": {
                    "config": {"steps_override": ["dbt build --select tag:hourly"]}
                }
            }
        }
    )
    assert result.success
    # The per-run override replaces job_trigger_defaults' steps_override for
    # this run only -- no YAML edit, no redeploy.
    assert fake_client.calls == [{"job_id": 42, "steps_override": ["dbt build --select tag:hourly"]}]


def test_mirrored_job_uses_job_trigger_defaults_when_no_run_config_given(mod, monkeypatch):
    fake_client = _FakeTriggerClient()
    component = _mirrored_component(mod, fake_client, job_trigger_defaults={"steps_override": ["dbt build"]})
    job = _build_mirrored_job(mod, component, monkeypatch)

    result = job.execute_in_process()
    assert result.success
    assert fake_client.calls == [{"job_id": 42, "steps_override": ["dbt build"]}]


def test_mirrored_job_fails_when_dbt_cloud_run_status_is_not_success(mod, monkeypatch):
    fake_client = _FakeTriggerClient(run_status=20)  # DbtCloudJobRunStatusType.ERROR
    component = _mirrored_component(mod, fake_client)
    job = _build_mirrored_job(mod, component, monkeypatch)

    result = job.execute_in_process(raise_on_error=False)
    assert not result.success


def test_mirrored_job_fails_when_a_model_errors_even_though_the_run_status_is_success(mod, monkeypatch):
    """A dbt Cloud run can report overall success while individual models
    fail (e.g. with --select flags that don't fail the whole invocation) --
    per-model status from run_results.json is the real source of truth."""
    fake_client = _FakeTriggerClient(result_status="error")
    component = _mirrored_component(mod, fake_client)
    job = _build_mirrored_job(mod, component, monkeypatch)

    result = job.execute_in_process(raise_on_error=False)
    assert not result.success
