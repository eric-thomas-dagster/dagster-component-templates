"""Real, committed tests for AzureSynapseComponent, covering gaps found against the ADF-converter
feedback (component-feedback reports, read from the component's own code before fixing, not guessed):

1. `notebooks:`/`spark_jobs:` let the component skip live Synapse discovery entirely -- build_defs
   constructs NO Azure client/credential at all when both are declared and import_pipelines=False
   (a placeholder/unresolved tenant_id used to fail CODE-LOCATION LOAD regardless, since build_defs
   unconditionally built a management client -- unused anywhere -- AND an artifacts client up front).
2. `dry_run` (pipeline/notebook/spark_job) logs what would run and skips the real Azure call AND the
   client construction inside the asset body.
3. The notebook asset used to catch ANY exception (including real failures) from a call to a method
   (`notebook_operation_result.begin_execute_notebook`) that doesn't exist on the real SDK at all, and
   reported a fake `status: "Skipped"` success. Fixed to use the real API
   (`run_notebook.begin_create_run` + a separate `run_notebook.get_status` poll to a terminal
   `run_status`) and to let real failures PROPAGATE instead of being swallowed.
4. Neither the notebook nor the Spark job asset checked the FINAL state of the run it started (both
   only reported the SUBMISSION response's initial, non-terminal state). Fixed: both now poll to a
   real terminal state and raise if it's not a successful one.
5. SynapseSparkJobDeclaration intentionally has NO args/conf/sizing override fields: confirmed (not
   guessed) that the real `begin_execute_spark_job_definition` API takes only the job definition's
   name, with no per-execution parameter override at all.

Mocks ONLY the Azure SDK boundary: azure-identity/azure-mgmt-synapse/azure-synapse-artifacts/
azure-synapse-spark are not pip-installed in this environment at all, so `component.py`'s own lazy
imports are exercised for real (confirming they ARE lazy -- the module imports fine with no azure
packages present), and `azure.synapse.artifacts.models`/`azure.synapse.spark` are injected into
sys.modules as minimal fakes so the real control-flow (submit, poll, terminal-state detection,
exception propagation) runs for real against them.
"""
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()

AzureSynapseComponent = mod.AzureSynapseComponent
SynapseNotebookDeclaration = mod.SynapseNotebookDeclaration
SynapseSparkJobDeclaration = mod.SynapseSparkJobDeclaration


def _component(**overrides):
    attrs = dict(subscription_id="sub", resource_group_name="rg", workspace_name="ws", import_pipelines=False)
    attrs.update(overrides)
    return AzureSynapseComponent(**attrs)


@pytest.fixture(autouse=True)
def _no_real_sleeping(monkeypatch):
    """The real poll loops sleep 15s between polls -- harmless in production, but these tests exercise
    multi-poll sequences (e.g. ["running", "success"]) and shouldn't burn real wall-clock time for it."""
    monkeypatch.setattr(mod.time, "sleep", lambda seconds: None)


# ── module itself loads with zero azure packages installed ──────────────────────────────────────────

def test_azure_sdk_is_not_installed_in_this_environment():
    """Confirms the premise: component.py's azure imports are genuinely lazy, not module-level --
    this test module only loaded at all (via conftest's load_component_module) because they are."""
    with pytest.raises(ModuleNotFoundError):
        import azure.identity  # noqa: F401


# ── declared items: build_defs constructs NO Azure client at all ────────────────────────────────────

def test_declared_notebooks_and_spark_jobs_build_defs_with_zero_azure_client_construction(monkeypatch):
    comp = _component(
        import_spark_jobs=True, import_notebooks=True,
        notebooks=[SynapseNotebookDeclaration(name="scoring")],
        spark_jobs=[SynapseSparkJobDeclaration(name="etl_job")],
    )

    def _boom(*a, **k):
        raise AssertionError("no Azure client should be constructed when everything is declared statically")
    monkeypatch.setattr(comp, "_get_artifacts_client", _boom)
    monkeypatch.setattr(comp, "_get_management_client", _boom)

    defs = comp.build_defs(context=None)
    keys = {a.key for a in defs.assets}
    assert dg.AssetKey(["synapse_notebook_scoring"]) in keys
    assert dg.AssetKey(["synapse_spark_job_etl_job"]) in keys


def test_declared_notebook_parameters_are_threaded_through_to_the_asset(monkeypatch):
    comp = _component(
        import_notebooks=True, dry_run=True,
        notebooks=[SynapseNotebookDeclaration(name="scoring", parameters={"as_of_date": "2024-01-01"})],
    )
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: (_ for _ in ()).throw(AssertionError("dry_run must not build a client")))
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_notebook_scoring"]))
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success


def test_synapse_spark_job_declaration_has_no_override_fields():
    """Confirmed (not guessed) real-API limitation: begin_execute_spark_job_definition takes only the
    job definition's name. Fields that would silently do nothing are not offered at all."""
    import dataclasses
    field_names = {f.name for f in dataclasses.fields(SynapseSparkJobDeclaration)}
    assert field_names == {"name"}


# ── dry_run ──────────────────────────────────────────────────────────────────────────────────────────

def test_pipeline_dry_run_skips_the_real_azure_call(monkeypatch):
    """_get_pipeline_assets still needs a client to DISCOVER pipeline names (pipelines have no
    declared-list bypass -- only notebooks/spark_jobs do, per the ADF-converter ask), but dry_run must
    still skip the real trigger call INSIDE the asset body."""
    comp = _component(import_pipelines=True, dry_run=True)

    class _FakeDiscoveryClient:
        class pipeline:
            @staticmethod
            def get_pipelines_by_workspace():
                return [types.SimpleNamespace(name="orders_load")]

    assets = comp._get_pipeline_assets(_FakeDiscoveryClient())
    asset_def = assets[0]
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: (_ for _ in ()).throw(AssertionError("dry_run must not build a client inside the asset body")))
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success


def test_spark_job_dry_run_skips_the_real_azure_call(monkeypatch):
    comp = _component(import_spark_jobs=True, dry_run=True, spark_jobs=[SynapseSparkJobDeclaration(name="etl_job")])
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: (_ for _ in ()).throw(AssertionError("no client in dry_run")))
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_spark_job_etl_job"]))
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success


def test_notebook_dry_run_skips_the_real_azure_call(monkeypatch):
    comp = _component(import_notebooks=True, dry_run=True, notebooks=[SynapseNotebookDeclaration(name="scoring")])
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: (_ for _ in ()).throw(AssertionError("no client in dry_run")))
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_notebook_scoring"]))
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success


# ── notebook: real submit+poll control flow, against injected fake SDK modules ─────────────────────

@pytest.fixture
def fake_notebook_sdk(monkeypatch):
    models_mod = types.ModuleType("azure.synapse.artifacts.models")

    class RunNotebookParameter:
        def __init__(self, type, value):
            self.type, self.value = type, value

    class RunNotebookRequest:
        def __init__(self, notebook, parameters=None):
            self.notebook, self.parameters = notebook, parameters

    models_mod.RunNotebookParameter = RunNotebookParameter
    models_mod.RunNotebookRequest = RunNotebookRequest
    monkeypatch.setitem(sys.modules, "azure.synapse.artifacts.models", models_mod)
    return models_mod


class _FakeNotebookPoller:
    def result(self):
        return None


class _FakeRunNotebookClient:
    """Simulates run_notebook.begin_create_run (submission) + run_notebook.get_status (poll)."""

    def __init__(self, status_sequence):
        self._statuses = list(status_sequence)
        self.create_run_calls = []
        self.get_status_calls = []

    def begin_create_run(self, run_id, request):
        self.create_run_calls.append((run_id, request))
        return _FakeNotebookPoller()

    def get_status(self, run_id):
        self.get_status_calls.append(run_id)
        run_status = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
        return types.SimpleNamespace(result=types.SimpleNamespace(run_status=run_status))


def _notebook_component_with_fake_client(monkeypatch, status_sequence):
    comp = _component(import_notebooks=True, notebooks=[SynapseNotebookDeclaration(name="scoring", parameters={"n": 3})])
    fake_run_notebook = _FakeRunNotebookClient(status_sequence)
    fake_client = types.SimpleNamespace(
        notebook=types.SimpleNamespace(get_notebook=lambda name: types.SimpleNamespace(properties=types.SimpleNamespace())),
        run_notebook=fake_run_notebook,
    )
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: fake_client)
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_notebook_scoring"]))
    return asset_def, fake_run_notebook


def test_notebook_real_run_polls_to_success(monkeypatch, fake_notebook_sdk):
    asset_def, fake_run_notebook = _notebook_component_with_fake_client(monkeypatch, ["running", "success"])
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success
    assert len(fake_run_notebook.create_run_calls) == 1
    # parameters were wrapped as RunNotebookParameter via the real request object
    _, request = fake_run_notebook.create_run_calls[0]
    assert request.parameters["n"].value == 3
    assert request.parameters["n"].type == "int"


def test_notebook_real_run_raises_on_a_real_failed_terminal_state(monkeypatch, fake_notebook_sdk):
    asset_def, _ = _notebook_component_with_fake_client(monkeypatch, ["running", "error"])
    with pytest.raises(Exception, match="run_status='error'"):
        dg.materialize([asset_def], raise_on_error=True)


def test_notebook_real_run_does_not_swallow_an_unrelated_exception(fake_notebook_sdk):
    """The ORIGINAL bug: any exception (including a real one) from the submission call was caught and
    reported as a fake 'Skipped' success. Confirms that no longer happens -- a real exception from the
    client itself (not a status-polling result) propagates."""
    comp = _component(import_notebooks=True, notebooks=[SynapseNotebookDeclaration(name="scoring")])

    class _BoomClient:
        notebook = types.SimpleNamespace(get_notebook=lambda name: types.SimpleNamespace(properties=types.SimpleNamespace()))

        class run_notebook:
            @staticmethod
            def begin_create_run(run_id, request):
                raise ConnectionError("network blip")

    import unittest.mock
    with unittest.mock.patch.object(comp, "_get_artifacts_client", return_value=_BoomClient()):
        defs = comp.build_defs(context=None)
        asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_notebook_scoring"]))
        with pytest.raises(Exception, match="network blip"):
            dg.materialize([asset_def], raise_on_error=True)


# ── spark job: real submit+poll control flow, against injected fake SparkClient ─────────────────────

def _install_fake_spark_sdk(monkeypatch, state_sequence):
    """Installs a fresh azure.synapse.spark.SparkClient fake whose get_spark_batch_job replays
    state_sequence (repeating the last entry), and returns the list of constructed instances so a
    test can assert on how SparkClient(...) was called (e.g. which spark_pool_name)."""
    states = list(state_sequence)
    instances = []

    class _FakeSparkClient:
        def __init__(self, credential, endpoint, spark_pool_name):
            self.credential, self.endpoint, self.spark_pool_name = credential, endpoint, spark_pool_name
            self.spark_batch = self
            instances.append(self)

        def get_spark_batch_job(self, batch_id):
            state = states.pop(0) if len(states) > 1 else states[0]
            return types.SimpleNamespace(state=state)

    spark_mod = types.ModuleType("azure.synapse.spark")
    spark_mod.SparkClient = _FakeSparkClient
    monkeypatch.setitem(sys.modules, "azure.synapse.spark", spark_mod)
    return instances


def _spark_component_with_fake_client(monkeypatch, state_sequence, spark_pool_name="pool-1"):
    comp = _component(import_spark_jobs=True, spark_jobs=[SynapseSparkJobDeclaration(name="etl_job")])
    instances = _install_fake_spark_sdk(monkeypatch, state_sequence)

    fake_client = types.SimpleNamespace(
        spark_job_definition=types.SimpleNamespace(
            begin_execute_spark_job_definition=lambda name: types.SimpleNamespace(
                result=lambda: types.SimpleNamespace(id="batch-123", state="not_started", spark_pool_name=spark_pool_name)
            ),
        ),
    )
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: fake_client)
    monkeypatch.setattr(comp, "_get_credential", lambda: "fake-credential")
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_spark_job_etl_job"]))
    return comp, asset_def, instances


def test_spark_job_real_run_polls_to_success(monkeypatch):
    _comp, asset_def, _instances = _spark_component_with_fake_client(monkeypatch, ["running", "success"])
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success


def test_spark_job_real_run_raises_on_dead_terminal_state(monkeypatch):
    _comp, asset_def, _instances = _spark_component_with_fake_client(monkeypatch, ["running", "dead"])
    with pytest.raises(Exception, match="state='dead'"):
        dg.materialize([asset_def], raise_on_error=True)


def test_spark_job_falls_back_to_default_spark_pool_name_when_submission_response_lacks_one(monkeypatch):
    comp, asset_def, instances = _spark_component_with_fake_client(monkeypatch, ["success"], spark_pool_name=None)
    comp.default_spark_pool_name = "fallback-pool"
    dg.materialize([asset_def], raise_on_error=True)
    assert instances[-1].spark_pool_name == "fallback-pool"


def test_spark_job_requires_azure_synapse_spark_package_for_polling(monkeypatch):
    """Without azure-synapse-spark installed (and no fake injected), a clear error names the real
    missing dependency -- not a silent fake success."""
    comp = _component(import_spark_jobs=True, spark_jobs=[SynapseSparkJobDeclaration(name="etl_job")])
    fake_client = types.SimpleNamespace(
        spark_job_definition=types.SimpleNamespace(
            begin_execute_spark_job_definition=lambda name: types.SimpleNamespace(
                result=lambda: types.SimpleNamespace(id="batch-123", state="not_started", spark_pool_name="pool-1")
            ),
        ),
    )
    monkeypatch.setattr(comp, "_get_artifacts_client", lambda: fake_client)
    monkeypatch.delitem(sys.modules, "azure.synapse.spark", raising=False)
    defs = comp.build_defs(context=None)
    asset_def = next(a for a in defs.assets if a.key == dg.AssetKey(["synapse_spark_job_etl_job"]))
    with pytest.raises(Exception, match="azure-synapse-spark"):
        dg.materialize([asset_def], raise_on_error=True)
