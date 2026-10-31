"""Real, committed tests for the "candidates to fold into fabric_workspace"
follow-up review of EnrichedFabricWorkspaceComponent (a Love's-POC subclass
built on top of the round-1 fixes). Covers the items that generalize well
into the base component:

- A: configurable max_retries/retry_backoff_seconds with exponential
  backoff, and a capacity/throttle keyword fallback when Fabric's own
  failureReason.isRetriable flag is absent (never overrides an explicit
  isRetriable=False).
- B: load_date_parameter (+ timezone) auto-injected into every run, with a
  'fabric/load_date_override' run tag for a one-off backfill.
- C: a `failure_class` metadata value + `[class]` description prefix
  (auth / infrastructure / collision / other -- not five buckets, since
  "data" vs "fabric platform bug" isn't reliably distinguishable from
  available signals; see the module docstring on `_classify_fabric_failure`).
- D: success now returns a real `MaterializeResult` with job id/status/url
  metadata, not the raw Fabric state dict as the asset's bare Output value.
- E: `exit_value_file` reads a notebook's own exit signal from OneLake
  (ADLS Gen2-compatible API, a separate storage.azure.com OAuth scope),
  and `data_failure_exit_values` turns a matching value into a real Failure.
- F: `pool` field reaches the @asset kwargs; `fabric/skip` run tag skips
  the actual trigger with an audited MaterializeResult.
- N: native partitioning via the repo's shared `_build_partitions_def`
  convention (daily/weekly/monthly/hourly/static/dynamic/cron/multi-axis),
  plus `partition_parameter_name` injecting the run's partition key into
  the triggered Fabric job's own parameters (same convention as
  azure_data_factory).
- R: a name collision between load_date_parameter and partition_parameter_name
  no longer silently sends "today" over a backfill's actual partition key --
  the partition key wins (the run tag override still wins over both).
- S: item_partitions (keyed by display name) lets one item be partitioned
  independently of the component-wide partition_type -- including fully
  un-partitioning a specific item even when the component-wide fields ARE
  set, instead of every runnable item in the instance sharing one
  partitions_def.
- T: partition_end_offset/partition_timezone thread through to every
  time-window partitions definition (Daily/Weekly/Monthly/Hourly/cron) --
  without them a daily set only ever holds completed UTC days, so a
  notebook meant to run intraday under an eager AutomationCondition would
  re-run yesterday's partition instead of processing today.
"""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fabric_workspace_round2", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FabricWorkspaceComponent = _mod.FabricWorkspaceComponent
FabricResource = _mod.FabricResource


def _component(**overrides):
    attrs = dict(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_pipelines=True,
    )
    attrs.update(overrides)
    return FabricWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


def _one_pipeline_state(name="nightly_ingest"):
    return {"pipelines": [{"id": "item-1", "displayName": name, "workspaceId": "ws-1"}]}


def _two_pipeline_state(name_a="live_notebook", name_b="part_b_notebook"):
    return {"pipelines": [
        {"id": "item-1", "displayName": name_a, "workspaceId": "ws-1"},
        {"id": "item-2", "displayName": name_b, "workspaceId": "ws-1"},
    ]}


def _asset_by_name(defs, name_fragment):
    for a in defs.assets:
        if name_fragment in str(a.key):
            return a
    raise AssertionError(f"no asset matching {name_fragment!r} in {[str(a.key) for a in defs.assets]}")


class _FakeResponse:
    def __init__(self, status_code=200, json_body=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_body or {}
        self.headers = headers or {}
        self.text = text or ""

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 300:
            raise Exception(f"HTTP {self.status_code}")


@pytest.fixture
def auth(monkeypatch):
    monkeypatch.setattr(FabricResource, "get_token", lambda self: "fake-token")
    monkeypatch.setattr(FabricResource, "get_storage_token", lambda self: "fake-storage-token")


def _ctx():
    return dg.build_asset_context()


# ── A: configurable retry / backoff ──────────────────────────────────────

def test_custom_max_retries_and_backoff_are_used(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "CapacityThrottled", "message": "paused", "isRetriable": True},
    }))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        max_retries=5, retry_backoff_seconds=10,
    )
    with pytest.raises(dg.RetryRequested) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.max_retries == 5
    assert exc_info.value.seconds_to_wait == 10  # attempt 0 in a direct-invocation test context


def test_explicit_isretriable_false_is_never_overridden_by_keyword_fallback(auth, monkeypatch):
    """A message that happens to mention 'capacity' should NOT force a retry
    when Fabric explicitly says isRetriable=False."""
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "BadNotebookCode", "message": "ran out of capacity in a loop variable", "isRetriable": False},
    }))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure):
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")


def test_keyword_fallback_applies_only_when_isretriable_is_absent(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "Unknown", "message": "the capacity is paused"},  # no isRetriable key at all
    }))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.RetryRequested):
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")


def test_retriable_trigger_failure_response_also_requests_a_retry(auth, monkeypatch):
    """A non-2xx response to the INITIAL trigger POST (not just a polled
    Failed state) should also be retried when it looks like a capacity/
    rate-limit issue."""
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(429, json_body={"errorCode": "TooManyRequestsForJobs", "message": "rate limited"}))

    comp = _component()
    with pytest.raises(dg.RetryRequested):
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")


# ── C: failure classification ────────────────────────────────────────────

def test_failure_class_metadata_and_description_prefix(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "InsufficientPrivileges", "message": "caller lacks access", "isRetriable": False},
    }))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.metadata["failure_class"].value == "auth"
    assert exc_info.value.description.startswith("[auth]")


def test_deduped_failure_class_is_collision(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Deduped", "id": "job-1"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.metadata["failure_class"].value == "collision"


# ── D: MaterializeResult with job metadata on success ────────────────────

def test_success_returns_a_materialize_result_with_job_metadata(tmp_path, auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/abc"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-abc"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    md = events[0].materialization.metadata
    assert md["fabric_job_instance_id"].value == "job-abc"
    assert md["fabric_job_status"].value == "Completed"
    assert md["fabric_job_instance_url"].value == "https://fabric/instances/abc"


# ── F: pool + fabric/skip ─────────────────────────────────────────────────

def test_pool_field_reaches_the_asset(tmp_path):
    comp = _component(pool="fabric_jobs")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert asset_def.op.pool == "fabric_jobs"


def test_fabric_skip_tag_skips_the_actual_trigger(tmp_path, auth, monkeypatch):
    import requests
    trigger_calls = []
    monkeypatch.setattr(requests, "post", lambda *a, **k: trigger_calls.append(1) or _FakeResponse(202))

    comp = _component()
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=True, tags={"fabric/skip": "true"})
    assert result.success
    assert trigger_calls == []  # the real Fabric trigger was never called
    events = result.get_asset_materialization_events()
    assert events[0].materialization.metadata["fabric_skipped"].value is True


# ── B: load_date_parameter + override tag ────────────────────────────────

def test_load_date_parameter_injects_todays_date(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        load_date_parameter="load_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success

    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert "load_date" in params
    assert params["load_date"]["type"] == "Text"

    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["fabric/load_date"].value == params["load_date"]["value"]


def test_load_date_override_tag_replaces_the_computed_date(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        load_date_parameter="load_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(
        list(defs.assets), raise_on_error=True,
        tags={"fabric/load_date_override": "2020-01-01"},
    )
    assert result.success
    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"]["value"] == "2020-01-01"


# ── E: OneLake exit-value read ────────────────────────────────────────────

def test_exit_value_is_read_from_the_configured_lakehouse_not_the_notebook_item(tmp_path, auth, monkeypatch):
    """A Notebook has no OneLake storage of its own -- the exit-value file
    must be read from lakehouse_id, never the triggered item's own id."""
    import requests

    def _get(url, headers=None, **k):
        if "onelake.dfs.fabric.microsoft.com" in url:
            assert url == "https://onelake.dfs.fabric.microsoft.com/ws-1/lh-1/Files/exit_value.txt"
            assert "item-1" not in url
            assert headers["Authorization"] == "Bearer fake-storage-token"
            return _FakeResponse(200, text="ok")
        return _FakeResponse(200, {"status": "Completed", "id": "job-1"})

    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", _get)

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        lakehouse_id="lh-1",
        exit_value_file="exit_value.txt",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["fabric/exit_value"].value == "ok"


def test_data_failure_exit_value_turns_a_completed_job_into_a_failure(tmp_path, auth, monkeypatch):
    import requests

    def _get(url, headers=None, **k):
        if "onelake.dfs.fabric.microsoft.com" in url:
            return _FakeResponse(200, text="no_rows_processed")
        return _FakeResponse(200, {"status": "Completed", "id": "job-1"})

    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", _get)

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        lakehouse_id="lh-1",
        exit_value_file="exit_value.txt",
        data_failure_exit_values=["no_rows_processed"],
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


def test_lakehouse_id_required_when_exit_value_file_is_set():
    with pytest.raises(Exception, match="lakehouse_id is required"):
        _component(exit_value_file="exit_value.txt")


def test_exit_value_read_failure_fails_closed_instead_of_silently_skipping(tmp_path, auth, monkeypatch):
    """A 404/network/auth failure reading the exit-value file must raise,
    not warn-and-return-None -- the whole point of the field is to catch
    bad data that would otherwise pass as a green materialization."""
    import requests

    def _get(url, headers=None, **k):
        if "onelake.dfs.fabric.microsoft.com" in url:
            return _FakeResponse(404, text="not found")
        return _FakeResponse(200, {"status": "Completed", "id": "job-1"})

    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", _get)

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        lakehouse_id="lh-1",
        exit_value_file="exit_value.txt",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


def test_exit_value_file_load_date_placeholder_is_substituted(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _get(url, headers=None, **k):
        if "onelake.dfs.fabric.microsoft.com" in url:
            captured["url"] = url
            return _FakeResponse(200, text="ok")
        return _FakeResponse(200, {"status": "Completed", "id": "job-1"})

    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", _get)

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        lakehouse_id="lh-1",
        exit_value_file="exit_values/{load_date}.txt",
        load_date_parameter="load_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(
        list(defs.assets), raise_on_error=True,
        tags={"fabric/load_date_override": "2020-01-01"},
    )
    assert result.success
    assert captured["url"] == "https://onelake.dfs.fabric.microsoft.com/ws-1/lh-1/Files/exit_values/2020-01-01.txt"


# ── L: failure-classification consistency ──────────────────────────────────

def test_trigger_failure_path_has_class_prefix_and_metadata(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        403, json_body={"errorCode": "SomeUnrecognisedCode", "message": "nope"},
    ))

    comp = _component()
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.metadata["failure_class"].value == "auth"
    assert exc_info.value.description.startswith("[auth]")


def test_item_not_found_gets_a_stale_state_hint(auth, monkeypatch):
    """Component-feedback: a notebook deleted/renamed in Fabric stays an
    asset until the next refresh-defs-state; a run against it previously
    just surfaced the raw Fabric ItemNotFound error with no hint that the
    CACHE, not the trigger logic, is stale."""
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        404, json_body={"errorCode": "ItemNotFound", "message": "Requested item ID not found."},
    ))

    comp = _component()
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert "refresh-defs-state" in exc_info.value.description
    assert "stale" in exc_info.value.description


def test_other_errors_get_no_stale_state_hint(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        403, json_body={"errorCode": "InsufficientPrivileges", "message": "nope"},
    ))

    comp = _component()
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert "refresh-defs-state" not in exc_info.value.description


def test_token_failure_carries_failure_class_and_prefix(monkeypatch):
    class _BoomResource(FabricResource):
        def get_token(self):
            raise RuntimeError("boom")

    comp = _component(workspace=_BoomResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.metadata["failure_class"].value == "auth"
    assert exc_info.value.description.startswith("[auth]")


def test_401_trigger_response_classified_as_auth_even_with_an_unrecognised_error_code(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        401, json_body={"errorCode": "TotallyUnknownCode", "message": "access denied somehow"},
    ))

    comp = _component()
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.metadata["failure_class"].value == "auth"


def test_polled_failure_without_401_and_without_auth_keywords_stays_other(auth, monkeypatch):
    """http_status_code only applies on the trigger-request path -- a polled
    job-status Failed has no HTTP status of its own, so an unrecognised
    errorCode there still lands in 'other', not 'auth'."""
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "TotallyUnknownCode", "message": "something broke", "isRetriable": False},
    }))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert exc_info.value.metadata["failure_class"].value == "other"


# ── M: per-item pool override ────────────────────────────────────────────

def test_item_pools_overrides_the_component_wide_pool_for_a_matching_item(tmp_path):
    comp = _component(pool="shared_pool", item_pools={"nightly_ingest": "special_pool"})
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state(name="nightly_ingest")))
    asset_def = next(iter(defs.assets))
    assert asset_def.op.pool == "special_pool"


def test_item_pools_falls_back_to_component_wide_pool_for_a_non_matching_item(tmp_path):
    comp = _component(pool="shared_pool", item_pools={"some_other_item": "special_pool"})
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state(name="nightly_ingest")))
    asset_def = next(iter(defs.assets))
    assert asset_def.op.pool == "shared_pool"


# ── N: native partitioning ──────────────────────────────────────────────
# The repo has a mature, widely-shared `_build_partitions_def` convention
# (~670 components, incl. the directly-analogous databricks_clean_room_
# notebook_run and snowflake_task) and azure_data_factory already
# demonstrates injecting context.partition_key into a triggered pipeline's
# own parameters -- so this was NOT out of scope, just not yet ported.

def test_daily_partition_type_builds_a_real_partitions_def(tmp_path):
    comp = _component(partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert isinstance(asset_def.partitions_def, dg.DailyPartitionsDefinition)


def test_cron_partition_type_builds_a_time_window_partitions_def(tmp_path):
    comp = _component(partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert isinstance(asset_def.partitions_def, dg.TimeWindowPartitionsDefinition)


def test_no_partition_type_leaves_the_asset_unpartitioned(tmp_path):
    comp = _component()
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert asset_def.partitions_def is None


def test_daily_partition_type_without_start_raises_a_clear_config_error(tmp_path):
    comp = _component(partition_type="daily")
    with pytest.raises(Exception, match="requires partition_start"):
        comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))


def test_partition_values_accepts_a_list_and_normalizes_to_csv():
    comp = _component(partition_type="static", partition_values=["a", "b", "c"])
    assert comp.partition_values == "a,b,c"


def test_partition_key_reaches_fabric_parameters_via_materialize(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        partition_type="daily",
        partition_start="2024-01-01",
        partition_parameter_name="run_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    result = dg.materialize([asset_def], partition_key="2024-06-15", raise_on_error=True)
    assert result.success

    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["run_date"]["value"] == "2024-06-15"

    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["fabric/partition_key"].value == "2024-06-15"


def test_partition_key_not_injected_without_partition_parameter_name(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    result = dg.materialize([asset_def], partition_key="2024-06-15", raise_on_error=True)
    assert result.success
    assert captured["json"] is None


# ── R: load_date_parameter / partition_parameter_name collision ──────────

def test_partition_key_wins_over_todays_date_on_a_name_collision(tmp_path, auth, monkeypatch):
    """Component-feedback repro: a backfill of 2026-01-03 must send
    load_date=2026-01-03, not today's date, when load_date_parameter and
    partition_parameter_name point at the same Fabric parameter name."""
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        partition_type="daily",
        partition_start="2024-01-01",
        partition_parameter_name="load_date",
        load_date_parameter="load_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    result = dg.materialize([asset_def], partition_key="2026-01-03", raise_on_error=True)
    assert result.success

    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"]["value"] == "2026-01-03"
    assert len(params) == 1  # one Fabric parameter, not two separate load_date writes


def test_load_date_override_tag_still_wins_over_the_collision(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        partition_type="daily",
        partition_start="2024-01-01",
        partition_parameter_name="load_date",
        load_date_parameter="load_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    result = dg.materialize(
        [asset_def], partition_key="2026-01-03", raise_on_error=True,
        tags={"fabric/load_date_override": "1999-12-31"},
    )
    assert result.success
    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"]["value"] == "1999-12-31"


def test_no_collision_when_parameter_names_differ(tmp_path, auth, monkeypatch):
    """Different names for load_date_parameter and partition_parameter_name:
    both parameters are sent, load_date still computes 'today' as before."""
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        partition_type="daily",
        partition_start="2024-01-01",
        partition_parameter_name="run_date",
        load_date_parameter="load_date",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    result = dg.materialize([asset_def], partition_key="2026-01-03", raise_on_error=True)
    assert result.success

    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["run_date"]["value"] == "2026-01-03"
    assert params["load_date"]["value"] != "2026-01-03"  # computed as today, unaffected


# ── S: item_partitions per-item override ──────────────────────────────────

def test_item_partitions_partitions_only_the_named_item(tmp_path):
    """Component-wide partition_type is unset; only the item with an
    item_partitions entry gets a real partitions_def."""
    comp = _component(
        item_partitions={
            "live_notebook": {"partition_type": "daily", "partition_start": "2024-01-01"},
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _two_pipeline_state()))
    live = _asset_by_name(defs, "live_notebook")
    other = _asset_by_name(defs, "part_b_notebook")
    assert isinstance(live.partitions_def, dg.DailyPartitionsDefinition)
    assert other.partitions_def is None


def test_item_partitions_can_explicitly_unpartition_one_item(tmp_path):
    """Component-wide partition_type IS set; an empty item_partitions entry
    fully un-partitions that one item instead of inheriting the default."""
    comp = _component(
        partition_type="daily",
        partition_start="2024-01-01",
        item_partitions={"part_b_notebook": {}},
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _two_pipeline_state()))
    live = _asset_by_name(defs, "live_notebook")
    other = _asset_by_name(defs, "part_b_notebook")
    assert isinstance(live.partitions_def, dg.DailyPartitionsDefinition)
    assert other.partitions_def is None


def test_item_partitions_overrides_partition_parameter_name_per_item(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured.setdefault("calls", []).append(json)
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        partition_parameter_name="default_param",
        item_partitions={
            "live_notebook": {
                "partition_type": "daily", "partition_start": "2024-01-01",
                "partition_parameter_name": "override_param",
            },
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _two_pipeline_state()))
    live = _asset_by_name(defs, "live_notebook")
    result = dg.materialize([live], partition_key="2024-06-15", raise_on_error=True)
    assert result.success
    params = {p["name"]: p for p in captured["calls"][0]["parameters"]}
    assert params["override_param"]["value"] == "2024-06-15"
    assert "default_param" not in params


# ── T: partition_end_offset / partition_timezone ──────────────────────────

def test_partition_end_offset_reaches_the_daily_partitions_def(tmp_path):
    comp = _component(partition_type="daily", partition_start="2024-01-01", partition_end_offset=1)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert asset_def.partitions_def.end_offset == 1


def test_partition_end_offset_defaults_to_zero_completed_windows_only(tmp_path):
    comp = _component(partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert asset_def.partitions_def.end_offset == 0


def test_partition_timezone_reaches_the_daily_partitions_def(tmp_path):
    comp = _component(partition_type="daily", partition_start="2024-01-01", partition_timezone="America/New_York")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert asset_def.partitions_def.timezone == "America/New_York"


def test_partition_end_offset_and_timezone_reach_the_cron_partitions_def(tmp_path):
    comp = _component(
        partition_type="cron", partition_start="2024-01-01",
        partition_cron_schedule="0 0 * * 1-5",
        partition_end_offset=1, partition_timezone="America/New_York",
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    asset_def = next(iter(defs.assets))
    assert isinstance(asset_def.partitions_def, dg.TimeWindowPartitionsDefinition)
    assert asset_def.partitions_def.end_offset == 1
    assert asset_def.partitions_def.timezone == "America/New_York"


def test_item_partitions_can_override_end_offset_and_timezone_per_item(tmp_path):
    """item_partitions fully replaces the component-wide fields -- an item
    entry that doesn't set partition_end_offset/partition_timezone gets
    Dagster's own defaults (0/UTC), not the component-wide custom values."""
    comp = _component(
        partition_type="daily", partition_start="2024-01-01",
        partition_end_offset=1, partition_timezone="America/New_York",
        item_partitions={
            "part_b_notebook": {
                "partition_type": "daily", "partition_start": "2024-01-01",
                "partition_end_offset": 0,
            },
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _two_pipeline_state()))
    live = _asset_by_name(defs, "live_notebook")
    other = _asset_by_name(defs, "part_b_notebook")
    assert live.partitions_def.end_offset == 1
    assert live.partitions_def.timezone == "America/New_York"
    assert other.partitions_def.end_offset == 0
    assert other.partitions_def.timezone == "UTC"  # Dagster's own default, not inherited from the component


# ── execution_data / item_execution_data ──────────────────────────────────
# Airflow-comparison finding: Fabric's own REST reference confirms
# executionData ("fixed static data, defined by the specific item job
# type") and the top-level parameters list can be sent TOGETHER in the same
# request. This is the escape hatch for job types whose required config
# (e.g. Spark Job Definition's executableFile/mainClass/...) isn't exposed
# as a dedicated field yet.

def test_execution_data_merges_alongside_parameters(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        default_parameters={"load_date": "2024-01-01"},
        execution_data={"tableName": "Table1"},
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success

    assert captured["json"]["executionData"] == {"tableName": "Table1"}
    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"]["value"] == "2024-01-01"


def test_item_execution_data_merges_over_the_component_wide_default(tmp_path, auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        execution_data={"tableName": "default_table"},
        item_execution_data={"nightly_ingest": {"tableName": "override_table", "extra": "x"}},
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state(name="nightly_ingest")))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert captured["json"]["executionData"] == {"tableName": "override_table", "extra": "x"}


def test_execution_data_merges_into_notebook_executiondata_alongside_parameters(tmp_path, auth, monkeypatch):
    """For Notebook, execution_data must merge alongside the auto-built
    executionData.parameters dict, not replace it."""
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0),
        import_pipelines=False, import_notebooks=True,
        default_parameters={"load_date": "2024-01-01"},
        execution_data={"configuration": {"useStarterPool": True}},
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, {"notebooks": [{"id": "item-1", "displayName": "nb1", "workspaceId": "ws-1"}]}))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success

    exec_data = captured["json"]["executionData"]
    assert exec_data["parameters"]["load_date"] == {"value": "2024-01-01", "type": "string"}
    assert exec_data["configuration"] == {"useStarterPool": True}


def test_unmapped_item_type_raises_instead_of_silently_defaulting_to_pipeline(auth):
    """Previously `job_type_map.get(item_type, "Pipeline")` silently
    triggered an unrecognised item type as if it were a DataPipeline."""
    comp = _component()
    with pytest.raises(dg.Failure, match="has no known job type mapping"):
        comp._trigger_item_run(_ctx(), "item-1", "SparkJobDefinition")
