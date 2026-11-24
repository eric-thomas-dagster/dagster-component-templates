"""Tests for DataHubExportJobComponent.

Covers the two gaps this fixes:

1. `_push` used to be a bare module-level function called without `self.`,
   so a subclass had no way to override it -- the only way to fake the
   DataHub HTTP call for a demo/test was monkeypatching the module. It's
   now an instance method, so a subclass override is reached from inside
   the real op body with NO monkeypatching of `requests` or this module.

2. `dry_run` -- when True, the op logs the aspect proposals it would push
   and skips the real HTTP call entirely (zero egress), which this file
   verifies by overriding `_push` to raise if it's ever called.

Like `castordoc_export_job`/`openlineage_export_job`, the op body calls
`context.repository_def`, which Dagster only populates when a job executes
as part of a *loaded repository* -- not via `dg.build_op_context()` (its
`repository_def` property unconditionally raises) nor via
`Definitions(...).get_job_def(...).execute_in_process()` (confirmed: raises
"No repository definition was set on the step context"). So, following the
same precedent used elsewhere in this repo for exactly this constraint
(e.g. `enhanced_data_quality_checks`'s `_run_check` helper), the op's
underlying undecorated function is invoked directly
(`node.compute_fn.decorated_fn(fake_context)`) against a real
`get_repository_def()` and a lightweight fake context exposing just the
attributes the op body actually reads.
"""
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()
DataHubExportJobComponent = mod.DataHubExportJobComponent


class _FakeLog:
    def __init__(self):
        self.infos = []
        self.warnings = []

    def info(self, msg):
        self.infos.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)


class _FakeInstance:
    """Stands in for DagsterInstance for the two calls the op body makes."""

    def __init__(self):
        self.tags_added = {}

    def get_runs(self, filters=None, limit=None):
        return []  # no prior successful run to dedupe against

    def add_run_tags(self, run_id, tags):
        self.tags_added.update(tags)


class _FakeOpContext:
    def __init__(self, repository_def, instance=None, run_id="test-run-id"):
        self.repository_def = repository_def
        self.instance = instance or _FakeInstance()
        self.run_id = run_id
        self.log = _FakeLog()


def _repo_def_and_op_node(component: "DataHubExportJobComponent"):
    """Build a real repository containing a couple of lineaged assets plus
    the component's job, and return (repository_def, the walk-and-push op node)."""

    @dg.asset(group_name="staging", kinds={"sql"})
    def orders():
        return 1

    @dg.asset(deps=[dg.AssetKey("orders")], group_name="ml")
    def ml_model():
        return 2

    built = component.build_defs(None)
    job_def = next(iter(built.jobs))
    repo_def = dg.Definitions(assets=[orders, ml_model], jobs=[job_def]).get_repository_def()
    node = next(n for n in job_def.nodes if n.name == f"{component.job_name}_op")
    return repo_def, node.definition


# ── build_defs wiring (same pattern as castordoc_export_job) ───────────────

def test_build_defs_wires_job_name_and_op():
    component = DataHubExportJobComponent(job_name="sync_job")
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    assert job_def.name == "sync_job"
    assert f"{component.job_name}_op" in [node.name for node in job_def.nodes]


def test_build_defs_attaches_schedule_when_configured():
    component = DataHubExportJobComponent(job_name="sync_job", schedule="0 3 * * *", default_status="RUNNING")
    defs = component.build_defs(None)
    schedule_def = next(iter(defs.schedules))
    assert schedule_def.cron_schedule == "0 3 * * *"
    assert schedule_def.default_status == dg.DefaultScheduleStatus.RUNNING


# ── dry_run: no network call, logs the would-be payload ────────────────────

def test_dry_run_never_calls_push():
    class _AssertNoPush(DataHubExportJobComponent):
        def _push(self, log, transformed, base_url, token_env):
            raise AssertionError("_push must not be called when dry_run=True")

    component = _AssertNoPush(job_name="sync_job", dry_run=True)
    repo_def, node = _repo_def_and_op_node(component)
    fake_context = _FakeOpContext(repository_def=repo_def)

    result = node.compute_fn.decorated_fn(fake_context)

    assert result["pushed"] is False
    assert result["dry_run"] is True
    assert result["aspects_would_push"] > 0


def test_dry_run_logs_the_transformed_payload():
    component = DataHubExportJobComponent(job_name="sync_job", dry_run=True)
    repo_def, node = _repo_def_and_op_node(component)
    fake_context = _FakeOpContext(repository_def=repo_def)

    node.compute_fn.decorated_fn(fake_context)

    joined_log = "\n".join(fake_context.log.infos)
    assert "DRY RUN" in joined_log
    assert "ingestProposal" not in joined_log  # no HTTP call was logged, only the proposals
    assert "entityUrn" in joined_log  # the actual payload content was logged


def test_dry_run_does_not_persist_the_payload_hash_run_tag():
    """A dry run must not poison only_export_on_change's dedupe for the next
    real run -- it never writes the datahub_export/payload_hash tag."""
    component = DataHubExportJobComponent(job_name="sync_job", dry_run=True)
    repo_def, node = _repo_def_and_op_node(component)
    fake_instance = _FakeInstance()
    fake_context = _FakeOpContext(repository_def=repo_def, instance=fake_instance)

    node.compute_fn.decorated_fn(fake_context)

    assert fake_instance.tags_added == {}


# ── _push is an overridable seam (instance method, not a bare function) ────

def test_subclass_overriding_push_is_used_instead_of_a_real_http_call():
    calls = []

    class _RecordingPush(DataHubExportJobComponent):
        def _push(self, log, transformed, base_url, token_env):
            calls.append((len(transformed), base_url, token_env))

    component = _RecordingPush(
        job_name="sync_job", catalog_url="https://demo.example.com/api/gms",
        api_token_env="DATAHUB_JOB_TEST_TOKEN", only_export_on_change=False,
    )
    repo_def, node = _repo_def_and_op_node(component)
    fake_context = _FakeOpContext(repository_def=repo_def)

    result = node.compute_fn.decorated_fn(fake_context)

    assert len(calls) == 1
    aspects_count, base_url, token_env = calls[0]
    assert aspects_count > 0
    assert base_url == "https://demo.example.com/api/gms"
    assert token_env == "DATAHUB_JOB_TEST_TOKEN"
    assert result["pushed"] is True


def test_push_instance_method_raises_without_token_env(monkeypatch):
    monkeypatch.delenv("DATAHUB_JOB_MISSING_TOKEN", raising=False)
    component = DataHubExportJobComponent(job_name="sync_job", api_token_env="DATAHUB_JOB_MISSING_TOKEN")
    import pytest
    with pytest.raises(RuntimeError, match="DATAHUB_JOB_MISSING_TOKEN"):
        component._push(_FakeLog(), [{"proposal": {}}], "https://x", "DATAHUB_JOB_MISSING_TOKEN")


def test_push_instance_method_posts_each_proposal(monkeypatch):
    monkeypatch.setenv("DATAHUB_JOB_TEST_TOKEN", "sekret")
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            pass

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append((url, json, headers))
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)

    component = DataHubExportJobComponent(job_name="sync_job", catalog_url="https://x/api/gms", api_token_env="DATAHUB_JOB_TEST_TOKEN")
    transformed = [{"proposal": {"entityUrn": "urn:li:dataset:(a,b,PROD)"}}]
    component._push(_FakeLog(), transformed, component.catalog_url, component.api_token_env)

    assert len(calls) == 1
    url, payload, headers = calls[0]
    assert url == "https://x/api/gms/aspects?action=ingestProposal"
    assert payload == transformed[0]
    assert headers["Authorization"] == "Bearer sekret"
