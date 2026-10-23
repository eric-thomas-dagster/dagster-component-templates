"""Real, committed tests for two confirmed gaps a demo-builder hit subclassing
AzureDataFactoryComponent (component-feedback/2026-09-03-azure-data-factory-demo-mode-seam.md):

1. No execution seam: the pipeline-trigger-and-poll closure called the private
   module-level `_get_adf_client` directly, with no override point -- the demo
   had to monkeypatch that free function globally to inject a fake client.
   Fixed: a new `get_adf_client` method is the one execution seam every ADF
   client construction goes through (pipeline trigger+poll, trigger start,
   observation sensor); overridable by a normal subclass, no monkeypatching.

2. The observation sensor ignored `assets_by_pipeline_name`'s `key:` override:
   it built each AssetMaterialization's key as `f"adf_pipeline_{pipeline_name}"`
   -- the raw default shape -- instead of resolving the actual overridden spec
   key(s). Confirmed empirically in the original report: materializing a
   pipeline asset under an overridden key, then evaluating the sensor,
   produced an AssetObservation against the unoverridden default key, which
   never attaches to the visible asset. Also: one pipeline can expand to
   SEVERAL asset specs via assets_by_pipeline_name (a list override) -- the
   old code could only ever guess at one key, never several. Fixed: the
   pipeline-asset loop now records each pipeline's real, resolved AssetKey(s)
   in a pipeline_name -> [AssetKey] map, and the sensor emits one
   AssetMaterialization per resolved key from that map.
"""
import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("adf_seam_test", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

AzureDataFactoryComponent = mod.AzureDataFactoryComponent
AzureDataFactoryResource = mod.AzureDataFactoryResource


@pytest.fixture(autouse=True)
def _fake_azure_sdk(monkeypatch):
    """The real `azure-mgmt-datafactory` package isn't installed in this test
    environment, and the sensor body does `from azure.mgmt.datafactory.models
    import RunFilterParameters` unconditionally -- inject a minimal fake
    module chain into sys.modules so that import succeeds, using
    monkeypatch.setitem (auto-reverted after each test) rather than a bare
    `sys.modules[...] = ...` assignment, which would otherwise permanently
    shadow a real `azure` package for the rest of the test session (same
    convention as assets/ingestion/adls_to_database_asset's own test suite).
    """
    class RunFilterParameters:
        def __init__(self, last_updated_after=None, last_updated_before=None):
            self.last_updated_after = last_updated_after
            self.last_updated_before = last_updated_before

    if "azure" in sys.modules:
        azure_mod = sys.modules["azure"]
    else:
        azure_mod = types.ModuleType("azure")
        monkeypatch.setitem(sys.modules, "azure", azure_mod)

    if "azure.mgmt" in sys.modules:
        mgmt_mod = sys.modules["azure.mgmt"]
    else:
        mgmt_mod = types.ModuleType("azure.mgmt")
        monkeypatch.setitem(sys.modules, "azure.mgmt", mgmt_mod)
        monkeypatch.setattr(azure_mod, "mgmt", mgmt_mod, raising=False)

    if "azure.mgmt.datafactory" in sys.modules:
        df_mod = sys.modules["azure.mgmt.datafactory"]
    else:
        df_mod = types.ModuleType("azure.mgmt.datafactory")
        monkeypatch.setitem(sys.modules, "azure.mgmt.datafactory", df_mod)
        monkeypatch.setattr(mgmt_mod, "datafactory", df_mod, raising=False)

    if "azure.mgmt.datafactory.models" in sys.modules and hasattr(sys.modules["azure.mgmt.datafactory.models"], "RunFilterParameters"):
        models_mod = sys.modules["azure.mgmt.datafactory.models"]
    else:
        models_mod = types.ModuleType("azure.mgmt.datafactory.models")
        models_mod.RunFilterParameters = RunFilterParameters
        monkeypatch.setitem(sys.modules, "azure.mgmt.datafactory.models", models_mod)
        monkeypatch.setattr(df_mod, "models", models_mod, raising=False)


def _component(**overrides):
    attrs = dict(
        workspace=AzureDataFactoryResource(
            subscription_id="sub-1", resource_group_name="rg-1", factory_name="factory-1",
        ),
        import_pipelines=True,
        import_triggers=False,
    )
    attrs.update(overrides)
    return AzureDataFactoryComponent(**attrs)


class _FakeClient:
    """Stands in for a real DataFactoryManagementClient -- tracks that it was
    actually used, so tests confirm the execution seam dispatches through it."""

    def __init__(self):
        self.calls = []

        class _PipelineRuns:
            def __init__(self, outer):
                self.outer = outer

            def query_by_factory(self, rg, factory, filter_params):
                self.outer.calls.append(("query_by_factory", rg, factory))
                return _FakeRunsResult(self.outer.runs)

        class _TriggerRuns:
            def query_by_factory(self, rg, factory, filter_params):
                class _R:
                    value = []
                return _R()

        self.pipeline_runs = _PipelineRuns(self)
        self.trigger_runs = _TriggerRuns()
        self.runs = []


class _FakeRun:
    def __init__(self, pipeline_name, status="Succeeded", run_id="run-1"):
        self.pipeline_name = pipeline_name
        self.status = status
        self.run_id = run_id
        self.run_start = datetime.now(timezone.utc) - timedelta(minutes=5)
        self.run_end = datetime.now(timezone.utc)
        self.message = None


class _FakeRunsResult:
    def __init__(self, runs):
        self.value = runs


def test_get_adf_client_is_the_one_seam_subclasses_can_override():
    """A demo/test subclass overrides ONLY get_adf_client -- no monkeypatching
    the private module function required."""
    fake = _FakeClient()
    fake.runs = [_FakeRun("nightly_ingest")]

    class _DemoComponent(AzureDataFactoryComponent):
        def get_adf_client(self, subscription_id, tenant_id, client_id, client_secret):
            return fake

    comp = _DemoComponent(
        workspace=AzureDataFactoryResource(subscription_id="s", resource_group_name="rg", factory_name="f"),
        import_pipelines=True, import_triggers=False, polling_sensor=True,
    )
    defs = mod._build_adf_defs(
        pipelines=[{"name": "nightly_ingest"}], trigger_names=[],
        subscription_id="s", resource_group_name="rg", factory_name="f",
        tenant_id=None, client_id=None, client_secret=None,
        group_name="adf", import_pipelines=True, import_triggers=False,
        generate_sensor=True, poll_interval_seconds=60,
        filter_by_name_pattern=None, exclude_name_pattern=None,
        get_client_fn=comp.get_adf_client,
    )
    sensor_def = next(iter(defs.sensors))
    context = dg.build_sensor_context()
    result = sensor_def(context)

    assert ("query_by_factory", "rg", "f") in fake.calls
    assert len(result.asset_events) == 1
    assert result.asset_events[0].asset_key == dg.AssetKey(["adf_pipeline_nightly_ingest"])


def test_sensor_observes_against_the_overridden_key_not_the_default():
    """The exact bug reported: an assets_by_pipeline_name key: override must
    reach the sensor's emitted AssetMaterialization."""
    fake = _FakeClient()
    fake.runs = [_FakeRun("legacy_nightly_ingestion")]

    defs = mod._build_adf_defs(
        pipelines=[{"name": "legacy_nightly_ingestion"}], trigger_names=[],
        subscription_id="s", resource_group_name="rg", factory_name="f",
        tenant_id=None, client_id=None, client_secret=None,
        group_name="adf", import_pipelines=True, import_triggers=False,
        generate_sensor=True, poll_interval_seconds=60,
        filter_by_name_pattern=None, exclude_name_pattern=None,
        assets_by_pipeline_name={"legacy_nightly_ingestion": {"key": "legacy/orchestration/nightly_ingest"}},
        get_client_fn=lambda *a: fake,
    )
    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())

    assert len(result.asset_events) == 1
    assert result.asset_events[0].asset_key == dg.AssetKey(["legacy", "orchestration", "nightly_ingest"])
    # The default, unoverridden shape must NOT appear.
    assert result.asset_events[0].asset_key != dg.AssetKey(["adf_pipeline_legacy_nightly_ingestion"])


def test_sensor_emits_one_event_per_expanded_spec_when_override_is_a_list():
    """assets_by_pipeline_name can expand ONE pipeline into SEVERAL asset specs
    -- the sensor must emit against every resolved key, not just one guess."""
    fake = _FakeClient()
    fake.runs = [_FakeRun("shared_pipeline")]

    defs = mod._build_adf_defs(
        pipelines=[{"name": "shared_pipeline"}], trigger_names=[],
        subscription_id="s", resource_group_name="rg", factory_name="f",
        tenant_id=None, client_id=None, client_secret=None,
        group_name="adf", import_pipelines=True, import_triggers=False,
        generate_sensor=True, poll_interval_seconds=60,
        filter_by_name_pattern=None, exclude_name_pattern=None,
        assets_by_pipeline_name={"shared_pipeline": [
            {"key": "team_a/shared_pipeline"}, {"key": "team_b/shared_pipeline"},
        ]},
        get_client_fn=lambda *a: fake,
    )
    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())

    keys = {e.asset_key for e in result.asset_events}
    assert keys == {dg.AssetKey(["team_a", "shared_pipeline"]), dg.AssetKey(["team_b", "shared_pipeline"])}


def test_sensor_skips_a_run_for_a_pipeline_not_in_the_imported_set():
    fake = _FakeClient()
    fake.runs = [_FakeRun("some_other_pipeline_not_imported")]

    defs = mod._build_adf_defs(
        pipelines=[{"name": "nightly_ingest"}], trigger_names=[],
        subscription_id="s", resource_group_name="rg", factory_name="f",
        tenant_id=None, client_id=None, client_secret=None,
        group_name="adf", import_pipelines=True, import_triggers=False,
        generate_sensor=True, poll_interval_seconds=60,
        filter_by_name_pattern=None, exclude_name_pattern=None,
        get_client_fn=lambda *a: fake,
    )
    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())
    assert not result.asset_events
