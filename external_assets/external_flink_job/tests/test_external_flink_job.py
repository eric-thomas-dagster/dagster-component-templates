"""Real, committed tests for ExternalFlinkJobComponent.

The Flink analog of external_kafka_asset's `topics:` tests: single-job vs
`jobs:` list shape validation, and the shared observation sensor polling
every tracked job over ONE /jobs/overview call per tick. Mocks only the
external call (`_fetch_jobs_overview`, which does the real HTTP GET --
already exercised end-to-end for real in flink_job_run_asset's tests
against a real local server; this file focuses on entry resolution,
name-matching, and data_version/sensor wiring).
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
ExternalFlinkJobComponent = mod.ExternalFlinkJobComponent


def _specs_by_key(defs: dg.Definitions):
    return {s.key: s for s in defs.assets}


# --- shape validation -------------------------------------------------------

def test_single_job_mode_declares_one_asset():
    comp = ExternalFlinkJobComponent(jobmanager_url="http://x:8081", asset_key="streaming/enrichment", job_name="clickstream-enrichment")
    defs = comp.build_defs(None)
    specs = _specs_by_key(defs)
    assert dg.AssetKey(["streaming", "enrichment"]) in specs
    assert specs[dg.AssetKey(["streaming", "enrichment"])].metadata["job_name"] == "clickstream-enrichment"


def test_jobs_list_declares_several_assets_from_one_instance():
    comp = ExternalFlinkJobComponent(
        jobmanager_url="http://x:8081",
        jobs=[
            {"key": "streaming/enrichment", "job_name": "clickstream-enrichment"},
            {"key": "streaming/aggregation", "job_name": "clickstream-aggregation"},
        ],
    )
    defs = comp.build_defs(None)
    specs = _specs_by_key(defs)
    assert len(specs) == 2
    assert dg.AssetKey(["streaming", "aggregation"]) in specs


def test_both_single_and_multi_shapes_set_is_rejected():
    comp = ExternalFlinkJobComponent(
        jobmanager_url="http://x:8081", asset_key="a", job_name="b",
        jobs=[{"key": "c", "job_name": "d"}],
    )
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_shape_set_is_rejected():
    comp = ExternalFlinkJobComponent(jobmanager_url="http://x:8081")
    with pytest.raises(ValueError, match="Set either"):
        comp.build_defs(None)


def test_duplicate_keys_in_jobs_rejected():
    comp = ExternalFlinkJobComponent(
        jobmanager_url="http://x:8081",
        jobs=[{"key": "a", "job_name": "j1"}, {"key": "a", "job_name": "j2"}],
    )
    with pytest.raises(ValueError, match="duplicate key"):
        comp.build_defs(None)


def test_multi_job_sensor_requires_explicit_sensor_name():
    comp = ExternalFlinkJobComponent(
        jobmanager_url="http://x:8081",
        jobs=[{"key": "a", "job_name": "j1"}],
        create_observation_sensor=True,
    )
    with pytest.raises(ValueError, match="sensor_name is required"):
        comp.build_defs(None)


def test_single_job_sensor_derives_a_default_name():
    comp = ExternalFlinkJobComponent(
        jobmanager_url="http://x:8081", asset_key="a", job_name="my-job",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    assert defs.get_sensor_def("my_job__observation_sensor") is not None


# --- sensor behavior ---------------------------------------------------------
#
# `_fetch_jobs_overview` is an instance method (an overridable seam -- see
# test_fetch_jobs_overview_is_an_overridable_seam_no_monkeypatching below), so
# these tests fake it by subclassing and overriding the method, not by
# monkeypatching the module-level function (there no longer is one).

def _overview(jobs):
    return [dict(j) for j in jobs]


def test_shared_sensor_observes_every_tracked_job_over_one_overview_call():
    calls = []

    class _FakeFlink(ExternalFlinkJobComponent):
        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            calls.append(jobmanager_url)
            return _overview([
                {"jid": "jid1", "name": "enrichment", "state": "RUNNING", "start-time": 1000, "duration": 5000},
                {"jid": "jid2", "name": "aggregation", "state": "RUNNING", "start-time": 2000, "duration": 3000},
            ])

    comp = _FakeFlink(
        jobmanager_url="http://x:8081",
        jobs=[
            {"key": "streaming/enrichment", "job_name": "enrichment"},
            {"key": "streaming/aggregation", "job_name": "aggregation"},
        ],
        create_observation_sensor=True,
        sensor_name="flink_multi_sensor",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("flink_multi_sensor")

    context = dg.build_sensor_context()
    result = sensor_def(context)

    assert len(calls) == 1  # ONE /jobs/overview call covers every tracked job
    assert len(result.asset_events) == 2
    keys = {e.asset_key for e in result.asset_events}
    assert keys == {dg.AssetKey(["streaming", "enrichment"]), dg.AssetKey(["streaming", "aggregation"])}


def test_sensor_matches_jobs_by_name_not_jid():
    """The real Flink gotcha this component exists to handle correctly: jid changes
    every resubmission, so matching must be by the job's stable configured name."""
    class _FakeFlink(ExternalFlinkJobComponent):
        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            return _overview([{"jid": "a-brand-new-jid-each-run", "name": "enrichment", "state": "RUNNING", "start-time": 1000, "duration": 1}])

    comp = _FakeFlink(
        jobmanager_url="http://x:8081", asset_key="streaming/enrichment", job_name="enrichment",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("enrichment__observation_sensor")
    result = sensor_def(dg.build_sensor_context())
    assert result.asset_events[0].metadata["jid"].value == "a-brand-new-jid-each-run"


def test_job_not_present_in_overview_is_skipped_not_errored():
    class _FakeFlink(ExternalFlinkJobComponent):
        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            return _overview([])  # job hasn't been submitted yet, or expired from the job store

    comp = _FakeFlink(
        jobmanager_url="http://x:8081", asset_key="streaming/enrichment", job_name="enrichment",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("enrichment__observation_sensor")
    result = sensor_def(dg.build_sensor_context())
    assert result.skip_reason is not None
    assert result.asset_events == []


def test_overview_fetch_failure_skips_cleanly_without_raising():
    class _FakeFlink(ExternalFlinkJobComponent):
        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            raise RuntimeError("connection refused")

    comp = _FakeFlink(
        jobmanager_url="http://x:8081", asset_key="streaming/enrichment", job_name="enrichment",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("enrichment__observation_sensor")
    result = sensor_def(dg.build_sensor_context())
    assert "connection refused" in str(result.skip_reason)


def test_data_version_changes_when_state_changes():
    states = iter(["RUNNING", "FINISHED"])

    class _FakeFlink(ExternalFlinkJobComponent):
        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            return _overview([{"jid": "j1", "name": "batch-job", "state": next(states), "start-time": 1000, "duration": 1}])

    comp = _FakeFlink(
        jobmanager_url="http://x:8081", asset_key="streaming/batch", job_name="batch-job",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("batch_job__observation_sensor")

    from dagster._core.definitions.data_version import DATA_VERSION_TAG
    first = sensor_def(dg.build_sensor_context()).asset_events[0].tags[DATA_VERSION_TAG]
    second = sensor_def(dg.build_sensor_context()).asset_events[0].tags[DATA_VERSION_TAG]
    assert first != second


def test_emit_materialization_false_emits_observation_instead():
    class _FakeFlink(ExternalFlinkJobComponent):
        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            return _overview([{"jid": "j1", "name": "enrichment", "state": "RUNNING", "start-time": 1, "duration": 1}])

    comp = _FakeFlink(
        jobmanager_url="http://x:8081", asset_key="streaming/enrichment", job_name="enrichment",
        create_observation_sensor=True, emit_materialization=False,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("enrichment__observation_sensor")
    result = sensor_def(dg.build_sensor_context())
    assert isinstance(result.asset_events[0], dg.AssetObservation)


def test_fetch_jobs_overview_is_an_overridable_seam_no_monkeypatching():
    """The actual gap this fixes: `_fetch_jobs_overview` used to be a bare
    module-level function called without `self.`, so a subclass could not
    override it -- the only way to fake it for a demo/test was monkeypatching
    the module. Now it's an instance method, so a subclass override works
    with NO monkeypatching of `requests` or this module at all."""

    class _DemoFlinkJobComponent(ExternalFlinkJobComponent):
        """A demo/test subclass -- the README's demo-mode pattern."""

        def _fetch_jobs_overview(self, jobmanager_url, headers, request_timeout_seconds):
            return [{"jid": "demo-jid-1", "name": "demo-job", "state": "RUNNING", "start-time": 1700000000000, "duration": 42000}]

    comp = _DemoFlinkJobComponent(
        jobmanager_url="http://demo-cluster:8081", asset_key="streaming/demo_job", job_name="demo-job",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("demo_job__observation_sensor")
    result = sensor_def(dg.build_sensor_context())

    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.metadata["jid"].value == "demo-jid-1"
    assert event.metadata["state"].value == "RUNNING"


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def():
    comp = ExternalFlinkJobComponent(
        jobmanager_url="http://x:8081", asset_key="a", job_name="j",
        partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5",
    )
    spec = next(iter(comp.build_defs(None).assets))
    assert isinstance(spec.partitions_def, dg.TimeWindowPartitionsDefinition)
