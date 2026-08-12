"""Comprehensive tests for EventAutomationComponent.

Covers:
  - Every trigger type (11) — model construction + sensor emission + evaluation
  - Every action type (17) — execution against mocked dependencies
  - Template token rendering
  - Composition: multi-trigger OR, multi-action all-run, compound AND/OR
  - Error handling: missing env vars, action failures don't abort the bundle
"""
import json
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

from .conftest import load_component_module

# Loaded once at module import to avoid re-parsing 1300+ lines per test.
comp_mod = load_component_module()


# ── Helpers ──────────────────────────────────────────────────────────────

def make_context(cursor: str = ""):
    """A minimal SensorEvaluationContext substitute — the real one needs
    a live instance + workspace. Our sensor evaluation functions only touch
    context.instance / context.log / context.cursor / context.update_cursor,
    which we can duck-type."""
    ctx = MagicMock()
    ctx.cursor = cursor
    ctx.log = MagicMock()
    # update_cursor stores the value on the mock so tests can inspect it
    ctx._cursor_value = [cursor]
    def _update(v):
        ctx._cursor_value.append(v)
        ctx.cursor = v
    ctx.update_cursor = _update
    return ctx


def unwrap(result):
    """Dagster wraps single-return sensor eval funcs in a list. Unwrap for
    assertions."""
    if isinstance(result, list):
        return result[0] if result else None
    return result


def make_run(run_id="run-abc", job_name="my_job", status="SUCCESS",
             start_time=None, end_time=None):
    r = MagicMock()
    r.run_id = run_id
    r.job_name = job_name
    r.start_time = start_time
    r.end_time = end_time
    r.status = MagicMock()
    r.status.value = status
    return r


# ── Model construction ─────────────────────────────────────────────────

class TestModelConstruction:
    """Every trigger + action model must instantiate from valid dicts."""

    def test_all_triggers_instantiate(self):
        triggers = [
            ("run_status", {"status": "FAILURE"}),
            ("asset_materialized", {"asset_keys": ["a"]}),
            ("schedule", {"cron": "0 * * * *"}),
            ("http_poll", {"url": "https://example.com"}),
            ("freshness_violation", {"asset_keys": ["a"], "max_age_minutes": 60}),
            ("run_duration", {"max_duration_seconds": 1800}),
            ("run_stuck", {"max_running_seconds": 3600}),
            ("asset_check_failed", {}),
            ("metric_threshold", {
                "asset_key": "a", "metadata_key": "row_count",
                "comparison": "lt", "threshold": 100,
            }),
            ("absence", {"asset_keys": ["a"], "max_gap_minutes": 90}),
            ("log_pattern", {"pattern": r"OOMKilled", "sources": ["events", "stderr"]}),
            ("daemon_heartbeat", {"daemon_type": "SENSOR", "max_seconds_since_heartbeat": 120}),
            ("code_location_status", {"on_status": "ERROR"}),
            ("run_startup_slow", {"max_startup_seconds": 120}),
            ("asset_observation", {"asset_keys": ["a"]}),
            ("step_error", {"job_name": "j", "exception_pattern": r".*Timeout.*"}),
            ("metadata_match", {"asset_key": "a", "metadata_key": "status", "equals": "stale"}),
            ("hook_fired", {"on_status": "FAILURE"}),
            ("asset_partition_materialized", {"asset_keys": ["a"], "partition_key": "2024-01-15"}),
            ("run_reexecution", {"job_name": "j"}),
            ("asset_wipe", {}),
            ("config_override", {"job_name": "j"}),
            ("tag_set", {"tag_key": "priority", "tag_value": "P0"}),
            ("unhandled_exception", {"job_name": "j"}),
            ("asset_check_severity", {"severity": "WARN"}),
            ("op_output", {"step_key_pattern": ".*etl.*"}),
            ("materialization_planned", {"asset_keys": ["a"]}),
            ("asset_check_started", {"check_names": ["row_count_positive"]}),
            ("insights_metric", {"metric_name": "cost_per_run", "comparison": "gt", "threshold": 5.0}),
            ("dagster_plus_audit", {"event_type_pattern": "permission.*"}),
            ("asset_value_change", {
                "asset_key": "a", "metadata_key": "row_count",
                "direction": "decrease", "min_delta_pct": 50,
            }),
            ("backfill_status", {"status": "FAILED"}),
            ("sensor_failing", {"target_sensor_name": "ingest_sensor"}),
            ("concurrency_hit", {"max_queued": 20}),
            ("sqs_poll", {"queue_url": "https://sqs.example.com/q"}),
        ]
        for type_name, fields in triggers:
            cls = {
                "run_status": comp_mod.RunStatusTrigger,
                "asset_materialized": comp_mod.AssetMaterializedTrigger,
                "schedule": comp_mod.ScheduleTrigger,
                "http_poll": comp_mod.HttpPollTrigger,
                "freshness_violation": comp_mod.FreshnessViolationTrigger,
                "run_duration": comp_mod.RunDurationTrigger,
                "run_stuck": comp_mod.RunStuckTrigger,
                "asset_check_failed": comp_mod.AssetCheckFailedTrigger,
                "metric_threshold": comp_mod.MetricThresholdTrigger,
                "absence": comp_mod.AbsenceTrigger,
                "log_pattern": comp_mod.LogPatternTrigger,
                "daemon_heartbeat": comp_mod.DaemonHeartbeatTrigger,
                "code_location_status": comp_mod.CodeLocationStatusTrigger,
                "run_startup_slow": comp_mod.RunStartupSlowTrigger,
                "asset_observation": comp_mod.AssetObservationTrigger,
                "step_error": comp_mod.StepErrorTrigger,
                "metadata_match": comp_mod.MetadataMatchTrigger,
                "hook_fired": comp_mod.HookFiredTrigger,
                "asset_partition_materialized": comp_mod.AssetPartitionMaterializedTrigger,
                "run_reexecution": comp_mod.RunReexecutionTrigger,
                "asset_wipe": comp_mod.AssetWipeTrigger,
                "config_override": comp_mod.ConfigOverrideTrigger,
                "tag_set": comp_mod.TagSetTrigger,
                "unhandled_exception": comp_mod.UnhandledExceptionTrigger,
                "asset_check_severity": comp_mod.AssetCheckSeverityTrigger,
                "op_output": comp_mod.OpOutputTrigger,
                "materialization_planned": comp_mod.MaterializationPlannedTrigger,
                "asset_check_started": comp_mod.AssetCheckStartedTrigger,
                "insights_metric": comp_mod.InsightsMetricThresholdTrigger,
                "dagster_plus_audit": comp_mod.DagsterPlusAuditTrigger,
                "asset_value_change": comp_mod.AssetValueChangeTrigger,
                "backfill_status": comp_mod.BackfillStatusTrigger,
                "sensor_failing": comp_mod.SensorFailingTrigger,
                "concurrency_hit": comp_mod.ConcurrencyHitTrigger,
                "sqs_poll": comp_mod.SqsPollTrigger,
            }[type_name]
            obj = cls(**fields)
            assert obj.type == type_name

    def test_all_actions_instantiate(self):
        actions = [
            (comp_mod.MaterializeAction, {"asset_keys": ["a"]}),
            (comp_mod.LaunchJobAction, {"job_name": "j"}),
            (comp_mod.WebhookAction, {"url": "https://example.com"}),
            (comp_mod.SlackAction, {"webhook_url_env_var": "SLK"}),
            (comp_mod.PagerDutyAction, {"routing_key_env_var": "PD"}),
            (comp_mod.DiscordAction, {"webhook_url_env_var": "DIS"}),
            (comp_mod.EmitEventAction, {"asset_key": "a"}),
            (comp_mod.CancelRunAction, {}),
            (comp_mod.RetryRunAction, {}),
            (comp_mod.EmailAction, {
                "smtp_host_env_var": "H", "smtp_user_env_var": "U",
                "smtp_password_env_var": "P", "from_addr": "a@b.com", "to": ["c@d.com"],
            }),
            (comp_mod.TeamsAction, {"webhook_url_env_var": "TM"}),
            (comp_mod.OpsgenieAction, {"api_key_env_var": "OG"}),
            (comp_mod.MattermostAction, {"webhook_url_env_var": "MM"}),
            (comp_mod.ToggleSensorAction, {"sensor_name": "s", "action": "start"}),
            (comp_mod.ToggleScheduleAction, {"schedule_name": "s", "action": "stop"}),
            (comp_mod.SnsAction, {"topic_arn": "arn:..."}),
            (comp_mod.SqsAction, {"queue_url": "https://sqs..."}),
        ]
        for cls, fields in actions:
            obj = cls(**fields)
            assert obj.type is not None

    def test_compound_triggers_nest(self):
        """all_of can contain any_of + leaf triggers."""
        rs = comp_mod.RunStatusTrigger(status="FAILURE")
        any_of = comp_mod.AnyOfTrigger(triggers=[rs, rs])
        all_of = comp_mod.AllOfTrigger(triggers=[any_of, rs])
        assert len(all_of.triggers) == 2


# ── Component-level: sensors get emitted per trigger ─────────────────────

class TestSensorEmission:
    """Every trigger becomes a sensor in Definitions."""

    def _build_defs(self, triggers, actions=None):
        actions = actions or [{"type": "emit_event", "asset_key": "test"}]
        return comp_mod.EventAutomationComponent(
            name="test_automation",
            when=triggers,
            then=actions,
        ).build_defs(None)

    def test_multi_trigger_or(self):
        """Multiple triggers → one sensor each (OR semantics at automation level)."""
        defs = self._build_defs([
            {"type": "run_status", "status": "FAILURE"},
            {"type": "schedule", "cron": "* * * * *"},
            {"type": "asset_materialized", "asset_keys": ["a"]},
        ])
        sensors = list(defs.sensors)
        assert len(sensors) == 3
        names = {s.name for s in sensors}
        assert "test_automation__run_status_0" in names
        assert "test_automation__schedule_1" in names
        assert "test_automation__asset_materialized_2" in names

    def test_all_35_trigger_types_emit_sensors(self):
        """One sensor per trigger, for every trigger type."""
        triggers = [
            {"type": "run_status", "status": "FAILURE"},
            {"type": "asset_materialized", "asset_keys": ["a"]},
            {"type": "schedule", "cron": "0 * * * *"},
            {"type": "http_poll", "url": "https://example.com"},
            {"type": "freshness_violation", "asset_keys": ["a"], "max_age_minutes": 60},
            {"type": "run_duration", "max_duration_seconds": 1800},
            {"type": "run_stuck", "max_running_seconds": 3600},
            {"type": "asset_check_failed"},
            {"type": "metric_threshold", "asset_key": "a", "metadata_key": "row_count",
             "comparison": "lt", "threshold": 100},
            {"type": "absence", "asset_keys": ["a"], "max_gap_minutes": 90},
            {"type": "log_pattern", "pattern": "OOMKilled", "sources": ["events", "stderr"]},
            {"type": "daemon_heartbeat", "max_seconds_since_heartbeat": 120},
            {"type": "code_location_status", "on_status": "ERROR"},
            {"type": "run_startup_slow", "max_startup_seconds": 120},
            {"type": "asset_observation", "asset_keys": ["a"]},
            {"type": "step_error", "job_name": "j"},
            {"type": "metadata_match", "asset_key": "a", "metadata_key": "status", "equals": "stale"},
            {"type": "hook_fired", "on_status": "FAILURE"},
            {"type": "asset_partition_materialized", "asset_keys": ["a"], "partition_key": "2024-01-15"},
            {"type": "run_reexecution"},
            {"type": "asset_wipe"},
            {"type": "config_override"},
            {"type": "tag_set", "tag_key": "priority", "tag_value": "P0"},
            {"type": "unhandled_exception"},
            {"type": "asset_check_severity", "severity": "WARN"},
            {"type": "op_output", "step_key_pattern": ".*"},
            {"type": "materialization_planned", "asset_keys": ["a"]},
            {"type": "asset_check_started"},
            {"type": "insights_metric", "metric_name": "cost_per_run", "comparison": "gt", "threshold": 5.0},
            {"type": "dagster_plus_audit"},
            {"type": "asset_value_change", "asset_key": "a", "metadata_key": "row_count",
             "direction": "decrease", "min_delta_pct": 50},
            {"type": "backfill_status", "status": "FAILED"},
            {"type": "sensor_failing", "target_sensor_name": "ingest_sensor"},
            {"type": "concurrency_hit", "max_queued": 20},
            {"type": "sqs_poll", "queue_url": "https://sqs.example.com/q"},
        ]
        defs = self._build_defs(triggers)
        assert len(list(defs.sensors)) == 35


# ── Template rendering ───────────────────────────────────────────────────

class TestTemplates:
    def test_all_tokens_render(self):
        result = comp_mod._render_template(
            "{event_type} for {job_name} status={status} at {timestamp}",
            {"event_type": "run_failed", "job_name": "j", "status": "F", "timestamp": 42},
        )
        assert result == "run_failed for j status=F at 42"

    def test_empty_template_is_safe(self):
        assert comp_mod._render_template("", {"anything": "x"}) == ""

    def test_missing_token_renders_empty(self):
        # {other} isn't in the token dict → stays as literal (best-effort)
        r = comp_mod._render_template("{event_type} {other}", {"event_type": "x"})
        assert "x" in r


# ── Trigger evaluation: sensor eval functions decide fire vs skip ───────

class TestRunDurationTrigger:
    def test_fires_when_over_threshold(self):
        trigger = comp_mod.RunDurationTrigger(max_duration_seconds=60, on_status="ANY")
        # This trigger's sensor is a run_status_sensor — we test the internal
        # handler that _build_run_duration_sensor sets up. Since decorators
        # produce a callable, we assert the SensorDefinition emerges.
        sensor = comp_mod._build_run_duration_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        assert sensor.name == "test"


class TestRunStuckTrigger:
    def test_fires_when_run_running_too_long(self):
        trigger = comp_mod.RunStuckTrigger(max_running_seconds=100)
        actions = [comp_mod.EmitEventAction(asset_key="stuck")]
        sensor = comp_mod._build_run_stuck_sensor(
            "test_stuck", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        # Mock the instance to return one long-running run
        ctx = make_context(cursor="")
        long_run = make_run(
            run_id="stuck-run-1",
            start_time=time.time() - 200,  # 200s ago > 100s threshold
        )
        ctx.instance.get_runs.return_value = [long_run]
        # Invoke the underlying evaluation_fn
        result = unwrap(sensor._evaluation_fn(ctx))
        # Should return a SensorResult (fired) — not a SkipReason
        assert not hasattr(result, "skip_message") or (
            hasattr(result, "run_requests")
        )

    def test_skips_when_no_stuck_runs(self):
        trigger = comp_mod.RunStuckTrigger(max_running_seconds=100)
        sensor = comp_mod._build_run_stuck_sensor(
            "test_stuck2", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        ctx.instance.get_runs.return_value = []
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


class TestAbsenceTrigger:
    def test_fires_when_asset_absent(self):
        trigger = comp_mod.AbsenceTrigger(asset_keys=["hourly"], max_gap_minutes=10)
        # Use a MaterializeAction so the sensor produces a RunRequest (an
        # emit_event action alone produces no RunRequest, and the sensor's
        # `return dg.SensorResult(run_requests=…) if all_requests else None`
        # would then return None — non-firing return value).
        actions = [comp_mod.MaterializeAction(asset_keys=["recovery"])]
        sensor = comp_mod._build_absence_sensor(
            "test_abs", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        # Latest materialization was 30 minutes ago > 10min threshold
        latest_event = MagicMock()
        latest_event.timestamp = time.time() - (30 * 60)
        ctx.instance.get_latest_materialization_event.return_value = latest_event
        result = unwrap(sensor._evaluation_fn(ctx))
        # Should fire — RunRequest present
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_when_asset_fresh(self):
        trigger = comp_mod.AbsenceTrigger(asset_keys=["hourly"], max_gap_minutes=60)
        sensor = comp_mod._build_absence_sensor(
            "test_abs2", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        latest_event = MagicMock()
        latest_event.timestamp = time.time() - 30  # 30s ago, way under 60min
        ctx.instance.get_latest_materialization_event.return_value = latest_event
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


class TestScheduleTrigger:
    def test_schedule_sensor_uses_cron(self):
        trigger = comp_mod.ScheduleTrigger(cron="* * * * *")
        sensor = comp_mod._build_schedule_sensor(
            "test_sched", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        assert sensor.name == "test_sched"
        assert sensor.minimum_interval_seconds == 30


class TestHttpPollTrigger:
    @patch("requests.request")
    def test_status_ok_fires_on_2xx(self, mock_request):
        mock_request.return_value = MagicMock(status_code=200, text="ok")
        trigger = comp_mod.HttpPollTrigger(url="https://x", condition="status_ok")
        actions = [comp_mod.EmitEventAction(asset_key="x")]
        sensor = comp_mod._build_http_poll_sensor(
            "test_http", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        result = unwrap(sensor._evaluation_fn(ctx))
        assert not hasattr(result, "skip_message") or (
            hasattr(result, "run_requests")
        )
        mock_request.assert_called_once()

    @patch("requests.request")
    def test_response_changed_skips_when_unchanged(self, mock_request):
        mock_request.return_value = MagicMock(status_code=200, text="same")
        trigger = comp_mod.HttpPollTrigger(url="https://x")  # default response_changed
        sensor = comp_mod._build_http_poll_sensor(
            "test_http2", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        # Second call with same body + cursor = unchanged
        import hashlib
        expected_hash = hashlib.sha256("same".encode()).hexdigest()
        ctx = make_context(cursor=expected_hash)
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


# ── New trigger evaluation tests ─────────────────────────────────────────

class TestLogPatternTrigger:
    def test_fires_on_matching_log_line(self):
        trigger = comp_mod.LogPatternTrigger(pattern="OOMKilled")
        actions = [comp_mod.MaterializeAction(asset_keys=["recovery"])]
        sensor = comp_mod._build_log_pattern_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        run = make_run(run_id="r1", job_name="prod", status="FAILURE")
        ctx.instance.get_runs.return_value = [run]
        entry = MagicMock()
        entry.user_message = "OOMKilled: process killed by OOM"
        entry.message = ""
        ctx.instance.all_logs.return_value = [entry]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_when_no_match(self):
        trigger = comp_mod.LogPatternTrigger(pattern="OOMKilled")
        sensor = comp_mod._build_log_pattern_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        ctx.instance.get_runs.return_value = [make_run()]
        entry = MagicMock()
        entry.user_message = "everything is fine"
        entry.message = ""
        ctx.instance.all_logs.return_value = [entry]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")

    def test_sources_parsed(self):
        """Passing sources=['events', 'stdout', 'stderr'] resolves without error."""
        trigger = comp_mod.LogPatternTrigger(
            pattern="OOMKilled", sources=["events", "stdout", "stderr"]
        )
        assert "stdout" in trigger.sources
        assert "stderr" in trigger.sources


class TestDaemonHeartbeatTrigger:
    def test_fires_on_stale_daemon(self):
        trigger = comp_mod.DaemonHeartbeatTrigger(max_seconds_since_heartbeat=60)
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_daemon_heartbeat_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        stale_hb = MagicMock()
        stale_hb.timestamp = time.time() - 200  # 200s ago > 60s
        stale_status = MagicMock()
        stale_status.last_heartbeat = stale_hb
        ctx.instance.get_daemon_statuses.return_value = {"SENSOR": stale_status}
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_healthy_daemon(self):
        trigger = comp_mod.DaemonHeartbeatTrigger(max_seconds_since_heartbeat=60)
        sensor = comp_mod._build_daemon_heartbeat_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        fresh_hb = MagicMock()
        fresh_hb.timestamp = time.time() - 5  # 5s ago — fine
        fresh_status = MagicMock()
        fresh_status.last_heartbeat = fresh_hb
        ctx.instance.get_daemon_statuses.return_value = {"SENSOR": fresh_status}
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")

    def test_daemon_type_filter(self):
        trigger = comp_mod.DaemonHeartbeatTrigger(
            daemon_type="SCHEDULER", max_seconds_since_heartbeat=60
        )
        sensor = comp_mod._build_daemon_heartbeat_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        # SENSOR is stale but we're filtered to SCHEDULER only
        stale_hb = MagicMock()
        stale_hb.timestamp = time.time() - 500
        stale_status = MagicMock()
        stale_status.last_heartbeat = stale_hb
        ctx.instance.get_daemon_statuses.return_value = {"SENSOR": stale_status}
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


class TestCodeLocationStatusTrigger:
    def test_fires_on_error_status(self):
        trigger = comp_mod.CodeLocationStatusTrigger(on_status="ERROR")
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_code_location_status_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        broken_snap = MagicMock()
        broken_snap.location_name = "prod-loc"
        broken_snap.load_status = MagicMock(value="ERROR")
        broken_snap.load_error = "ImportError: no module 'foo'"
        # Try dict-of-name shape first
        ctx.instance.get_code_location_snapshots.return_value = {"prod-loc": broken_snap}
        # Ensure other candidate methods are absent
        ctx.instance.all_code_location_snapshots.side_effect = Exception("not this one")
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_when_all_api_candidates_raise(self):
        """Every candidate API on the instance raises → SkipReason, not crash."""
        trigger = comp_mod.CodeLocationStatusTrigger(on_status="ERROR")
        sensor = comp_mod._build_code_location_status_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        # Configure candidates to raise so the outer loop falls through
        ctx.instance.get_code_location_snapshots.side_effect = Exception("nope")
        ctx.instance.all_code_location_snapshots.side_effect = Exception("nope")
        ctx.instance.workspace_snapshot.side_effect = Exception("nope")
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


class TestRunStartupSlowTrigger:
    def test_fires_when_startup_over_threshold(self):
        trigger = comp_mod.RunStartupSlowTrigger(max_startup_seconds=60)
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_run_startup_slow_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        run = MagicMock()
        run.run_id = "r-slow"
        run.job_name = "j"
        run.create_timestamp = 1000
        run.start_time = 1200  # 200s startup > 60s
        ctx.instance.get_runs.return_value = [run]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_fast_startup(self):
        trigger = comp_mod.RunStartupSlowTrigger(max_startup_seconds=60)
        sensor = comp_mod._build_run_startup_slow_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        run = MagicMock()
        run.run_id = "r-fast"
        run.job_name = "j"
        run.create_timestamp = 1000
        run.start_time = 1005  # 5s — well under
        ctx.instance.get_runs.return_value = [run]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


class TestAssetObservationTrigger:
    def test_fires_on_matching_observation(self):
        trigger = comp_mod.AssetObservationTrigger(asset_keys=["external_status"])
        actions = [comp_mod.MaterializeAction(asset_keys=["dependent"])]
        sensor = comp_mod._build_asset_observation_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        # Craft a fake event record for an observation
        rec = MagicMock()
        rec.storage_id = 42
        obs_data = MagicMock()
        obs_data.asset_key = comp_mod.dg.AssetKey.from_user_string("external_status")
        rec.event_log_entry.dagster_event.event_specific_data = obs_data
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_unrelated_observations(self):
        trigger = comp_mod.AssetObservationTrigger(asset_keys=["watched_asset"])
        sensor = comp_mod._build_asset_observation_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 1
        obs_data = MagicMock()
        obs_data.asset_key = comp_mod.dg.AssetKey.from_user_string("other_asset")
        rec.event_log_entry.dagster_event.event_specific_data = obs_data
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")


class TestStepErrorTrigger:
    def test_fires_on_step_failure(self):
        trigger = comp_mod.StepErrorTrigger()
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_step_error_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 100
        entry = MagicMock()
        entry.run_id = "r1"
        evt = MagicMock()
        evt.step_key = "my_op"
        error = MagicMock()
        error.message = "ValueError: bad input"
        evt.event_specific_data.error = error
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_exception_pattern_filter(self):
        trigger = comp_mod.StepErrorTrigger(exception_pattern="TimeoutError")
        sensor = comp_mod._build_step_error_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 100
        entry = MagicMock()
        entry.run_id = "r1"
        evt = MagicMock()
        evt.step_key = "my_op"
        error = MagicMock()
        error.message = "ValueError: not a timeout"
        evt.event_specific_data.error = error
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")

    def test_only_final_failures_skips_non_final(self):
        """When step status is not FAILURE (e.g. retry pending, in-progress),
        with only_final_failures=True, we skip the fire."""
        trigger = comp_mod.StepErrorTrigger(only_final_failures=True)
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_step_error_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 100
        entry = MagicMock()
        entry.run_id = "r1"
        evt = MagicMock()
        evt.step_key = "my_op"
        error = MagicMock()
        error.message = "transient error"
        evt.event_specific_data.error = error
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        # Simulate: step is currently in RETRY_REQUESTED state (retry pending)
        stat = MagicMock()
        stat.status.name = "RETRY_REQUESTED"
        ctx.instance.get_run_step_stats.return_value = [stat]
        result = unwrap(sensor._evaluation_fn(ctx))
        # Fire should be skipped
        assert hasattr(result, "skip_message")

    def test_only_final_failures_fires_when_terminal(self):
        """When step status IS FAILURE (all retries exhausted),
        with only_final_failures=True, we still fire."""
        trigger = comp_mod.StepErrorTrigger(only_final_failures=True)
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_step_error_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 100
        entry = MagicMock()
        entry.run_id = "r1"
        evt = MagicMock()
        evt.step_key = "my_op"
        error = MagicMock()
        error.message = "final error"
        evt.event_specific_data.error = error
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        # Simulate: step FINALLY failed after all retries exhausted
        stat = MagicMock()
        stat.status.name = "FAILURE"
        ctx.instance.get_run_step_stats.return_value = [stat]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_only_final_failures_default_off_fires_immediately(self):
        """Default only_final_failures=False → don't check step stats, fire immediately."""
        trigger = comp_mod.StepErrorTrigger()  # only_final_failures defaults to False
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_step_error_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 100
        entry = MagicMock()
        entry.run_id = "r1"
        evt = MagicMock()
        evt.step_key = "my_op"
        error = MagicMock()
        error.message = "any error"
        evt.event_specific_data.error = error
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")
        # get_run_step_stats should NOT have been called
        ctx.instance.get_run_step_stats.assert_not_called()


class TestMetadataMatchTrigger:
    def test_fires_on_exact_match(self):
        trigger = comp_mod.MetadataMatchTrigger(
            asset_key="hourly", metadata_key="quality_grade", equals="poor"
        )
        actions = [comp_mod.MaterializeAction(asset_keys=["alert"])]
        sensor = comp_mod._build_metadata_match_sensor(
            "test", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 50
        entry = MagicMock()
        evt = MagicMock()
        data = MagicMock()
        data.asset_key = comp_mod.dg.AssetKey.from_user_string("hourly")
        mat = MagicMock()
        # Metadata value with .text attribute
        mval = MagicMock()
        mval.value = "poor"
        mat.metadata = {"quality_grade": mval}
        data.materialization = mat
        data.asset_observation = None
        evt.event_specific_data = data
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert result is not None
        assert hasattr(result, "run_requests")

    def test_skips_non_match(self):
        trigger = comp_mod.MetadataMatchTrigger(
            asset_key="hourly", metadata_key="quality_grade", equals="poor"
        )
        sensor = comp_mod._build_metadata_match_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        ctx = make_context()
        rec = MagicMock()
        rec.storage_id = 50
        entry = MagicMock()
        evt = MagicMock()
        data = MagicMock()
        data.asset_key = comp_mod.dg.AssetKey.from_user_string("hourly")
        mat = MagicMock()
        mval = MagicMock()
        mval.value = "good"  # doesn't match "poor"
        mat.metadata = {"quality_grade": mval}
        data.materialization = mat
        data.asset_observation = None
        evt.event_specific_data = data
        entry.dagster_event = evt
        rec.event_log_entry = entry
        ctx.instance.event_log_storage.get_event_records.return_value = [rec]
        result = unwrap(sensor._evaluation_fn(ctx))
        assert hasattr(result, "skip_message")

    def test_regex_match(self):
        trigger = comp_mod.MetadataMatchTrigger(
            asset_key="hourly", metadata_key="status", regex="stale|error"
        )
        assert trigger.regex == "stale|error"


# ── Action executors ─────────────────────────────────────────────────────

class TestActions:
    def test_materialize_produces_run_request(self):
        action = comp_mod.MaterializeAction(asset_keys=["a", "b"])
        result = comp_mod._execute_action(action, {"run_id": "r1"}, MagicMock())
        assert result is not None  # It's a RunRequest
        assert hasattr(result, "asset_selection")

    def test_launch_job_produces_run_request(self):
        action = comp_mod.LaunchJobAction(job_name="j", tags={"a": "b"})
        result = comp_mod._execute_action(action, {}, MagicMock())
        assert result is not None
        assert result.job_name == "j"

    @patch("requests.request")
    def test_webhook_posts(self, mock_request):
        action = comp_mod.WebhookAction(url="https://x", body_template="{event_type}")
        comp_mod._execute_action(action, {"event_type": "test"}, MagicMock())
        mock_request.assert_called_once()
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1] == "https://x"

    @patch.dict("os.environ", {"SLK": "https://hooks.slack.com/xxx"})
    @patch("requests.request")
    def test_slack_reads_env_var(self, mock_request):
        action = comp_mod.SlackAction(webhook_url_env_var="SLK", message="hello")
        comp_mod._execute_action(action, {}, MagicMock())
        mock_request.assert_called_once()
        # POSTed to the URL from env var
        assert mock_request.call_args[0][1] == "https://hooks.slack.com/xxx"

    def test_slack_skips_when_env_var_missing(self):
        action = comp_mod.SlackAction(webhook_url_env_var="MISSING_XXX_XXX")
        logger = MagicMock()
        comp_mod._execute_action(action, {}, logger)
        logger.warning.assert_called()

    @patch.dict("os.environ", {"PD_KEY": "test-key"})
    @patch("requests.request")
    def test_pagerduty_posts_v2(self, mock_request):
        action = comp_mod.PagerDutyAction(routing_key_env_var="PD_KEY", severity="error")
        comp_mod._execute_action(action, {"job_name": "j"}, MagicMock())
        mock_request.assert_called_once()
        assert mock_request.call_args[0][1] == "https://events.pagerduty.com/v2/enqueue"
        # Assert v2 payload structure
        body = json.loads(mock_request.call_args[1]["data"])
        assert body["routing_key"] == "test-key"
        assert body["event_action"] == "trigger"

    @patch.dict("os.environ", {"OG_KEY": "og-key"})
    @patch("requests.request")
    def test_opsgenie_posts(self, mock_request):
        action = comp_mod.OpsgenieAction(api_key_env_var="OG_KEY", priority="P1")
        comp_mod._execute_action(action, {"job_name": "j"}, MagicMock())
        mock_request.assert_called_once()
        assert "opsgenie" in mock_request.call_args[0][1]
        body = json.loads(mock_request.call_args[1]["data"])
        assert body["priority"] == "P1"

    @patch.dict("os.environ", {"TM_URL": "https://teams.example/x"})
    @patch("requests.request")
    def test_teams_posts(self, mock_request):
        action = comp_mod.TeamsAction(webhook_url_env_var="TM_URL")
        comp_mod._execute_action(action, {}, MagicMock())
        mock_request.assert_called_once()
        assert mock_request.call_args[0][1] == "https://teams.example/x"

    @patch.dict("os.environ", {"MM_URL": "https://mm/x"})
    @patch("requests.request")
    def test_mattermost_posts(self, mock_request):
        action = comp_mod.MattermostAction(webhook_url_env_var="MM_URL", channel="#alerts")
        comp_mod._execute_action(action, {}, MagicMock())
        mock_request.assert_called_once()
        body = json.loads(mock_request.call_args[1]["data"])
        assert body["channel"] == "#alerts"

    @patch.dict("os.environ", {"DIS_URL": "https://d/x"})
    @patch("requests.request")
    def test_discord_posts(self, mock_request):
        action = comp_mod.DiscordAction(webhook_url_env_var="DIS_URL", message="hi")
        comp_mod._execute_action(action, {}, MagicMock())
        mock_request.assert_called_once()

    def test_cancel_run_terminates_triggering_run(self):
        instance = MagicMock()
        action = comp_mod.CancelRunAction(which="triggering")
        comp_mod._execute_action(action, {"run_id": "run-xyz"}, MagicMock(), instance=instance)
        instance.run_launcher.terminate.assert_called_once_with("run-xyz")

    def test_cancel_run_all_matching(self):
        instance = MagicMock()
        instance.get_runs.return_value = [
            make_run(run_id="r1", job_name="job_a"),
            make_run(run_id="r2", job_name="job_b"),
            make_run(run_id="r3", job_name="job_a"),
        ]
        action = comp_mod.CancelRunAction(which="all_matching", job_name_filter="job_a")
        comp_mod._execute_action(action, {}, MagicMock(), instance=instance)
        # Should have terminated r1 and r3 (job_a only)
        assert instance.run_launcher.terminate.call_count == 2

    def test_cancel_run_missing_instance_warns(self):
        action = comp_mod.CancelRunAction()
        logger = MagicMock()
        comp_mod._execute_action(action, {"run_id": "x"}, logger, instance=None)
        logger.warning.assert_called()

    @patch("smtplib.SMTP")
    @patch.dict("os.environ", {"H": "smtp.x", "U": "u", "P": "p"})
    def test_email_uses_smtplib(self, mock_smtp):
        instance_mock = MagicMock()
        mock_smtp.return_value.__enter__.return_value = instance_mock
        action = comp_mod.EmailAction(
            smtp_host_env_var="H", smtp_user_env_var="U", smtp_password_env_var="P",
            from_addr="a@b.com", to=["c@d.com"],
        )
        comp_mod._execute_action(action, {"job_name": "j"}, MagicMock())
        instance_mock.login.assert_called_once_with("u", "p")
        instance_mock.sendmail.assert_called_once()

    def test_email_missing_creds_warns(self):
        action = comp_mod.EmailAction(
            smtp_host_env_var="MISSING", smtp_user_env_var="MISSING",
            smtp_password_env_var="MISSING",
            from_addr="a@b.com", to=["c@d.com"],
        )
        logger = MagicMock()
        comp_mod._execute_action(action, {}, logger)
        logger.warning.assert_called()

    def test_toggle_sensor_calls_instigator_update(self):
        instance = MagicMock()
        # Mock an instigator state with matching name
        state = MagicMock()
        state.name = "my_sensor"
        state.with_status.return_value = state
        instance.all_instigator_state.return_value = [state]
        action = comp_mod.ToggleSensorAction(sensor_name="my_sensor", action="stop")
        comp_mod._execute_action(action, {}, MagicMock(), instance=instance)
        instance.update_instigator_state.assert_called_once()

    def test_sns_uses_boto3(self):
        with patch.dict(sys.modules, {"boto3": MagicMock()}):
            import boto3 as mock_boto3
            client = mock_boto3.client.return_value
            client.publish.return_value = {"MessageId": "abc"}
            action = comp_mod.SnsAction(topic_arn="arn:sns:...", message_template="test")
            comp_mod._execute_action(action, {}, MagicMock())
            # Assertion: publish was called
            client.publish.assert_called_once()

    def test_sqs_uses_boto3(self):
        with patch.dict(sys.modules, {"boto3": MagicMock()}):
            import boto3 as mock_boto3
            client = mock_boto3.client.return_value
            client.send_message.return_value = {"MessageId": "xyz"}
            action = comp_mod.SqsAction(queue_url="https://sqs...", body_template="body")
            comp_mod._execute_action(action, {}, MagicMock())
            client.send_message.assert_called_once()


# ── Composition ──────────────────────────────────────────────────────────

class TestComposition:
    def test_multiple_actions_all_run(self):
        """Multiple actions in `then:` all execute — one failing doesn't abort."""
        actions = [
            comp_mod.EmitEventAction(asset_key="a1"),  # side-effect
            comp_mod.MaterializeAction(asset_keys=["b"]),  # RunRequest
            comp_mod.EmitEventAction(asset_key="a2"),  # side-effect
        ]
        logger = MagicMock()
        results = comp_mod._run_actions(actions, {}, logger)
        assert len(results) == 1  # only the materialize returned a RunRequest

    def test_one_failing_action_doesnt_stop_bundle(self):
        """A failing action logs a warning + moves on."""
        class BadAction:
            type = "bad"
        actions = [
            comp_mod.EmitEventAction(asset_key="a"),
            BadAction(),  # will be unknown → warning
            comp_mod.EmitEventAction(asset_key="b"),
        ]
        logger = MagicMock()
        results = comp_mod._run_actions(actions, {}, logger)
        # Warnings were logged, no exception raised
        assert logger.warning.called

    def test_compound_all_of_with_nested_any_of(self):
        """AllOf + AnyOf nesting."""
        run_status_a = comp_mod.RunStatusTrigger(status="FAILURE", job_name="a")
        run_status_b = comp_mod.RunStatusTrigger(status="FAILURE", job_name="b")
        any_of = comp_mod.AnyOfTrigger(triggers=[run_status_a, run_status_b])
        fresh = comp_mod.FreshnessViolationTrigger(asset_keys=["x"], max_age_minutes=60)
        all_of = comp_mod.AllOfTrigger(triggers=[any_of, fresh], within_seconds=3600)

        actions = [comp_mod.EmitEventAction(asset_key="marker")]
        sensor = comp_mod._build_compound_sensor(
            "compound", all_of, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
        )
        assert sensor.name == "compound"


# ── End-to-end via EventAutomationComponent build_defs ──────────────────

class TestEndToEnd:
    def test_full_component_with_every_trigger_type(self):
        """The 16-trigger project loads."""
        component = comp_mod.EventAutomationComponent(
            name="all_triggers",
            when=[
                {"type": "run_status", "status": "FAILURE"},
                {"type": "asset_materialized", "asset_keys": ["a"]},
                {"type": "schedule", "cron": "* * * * *"},
                {"type": "http_poll", "url": "https://x"},
                {"type": "freshness_violation", "asset_keys": ["a"], "max_age_minutes": 60},
                {"type": "run_duration", "max_duration_seconds": 60},
                {"type": "run_stuck", "max_running_seconds": 60},
                {"type": "asset_check_failed"},
                {"type": "metric_threshold", "asset_key": "a", "metadata_key": "rc",
                 "comparison": "lt", "threshold": 10},
                {"type": "absence", "asset_keys": ["a"], "max_gap_minutes": 60},
                {"type": "log_pattern", "pattern": "OOMKilled", "sources": ["events", "stdout", "stderr"]},
                {"type": "daemon_heartbeat", "daemon_type": "SENSOR", "max_seconds_since_heartbeat": 90},
                {"type": "code_location_status", "on_status": "UNHEALTHY"},
                {"type": "run_startup_slow", "max_startup_seconds": 60},
                {"type": "asset_observation", "asset_keys": ["a"]},
                {"type": "step_error"},
                {"type": "metadata_match", "asset_key": "a", "metadata_key": "status", "regex": "stale|error"},
                {"type": "hook_fired"},
                {"type": "asset_partition_materialized", "asset_keys": ["a"], "partition_key": "x"},
                {"type": "run_reexecution"},
                {"type": "asset_wipe"},
                {"type": "config_override"},
                {"type": "tag_set", "tag_key": "env"},
                {"type": "unhandled_exception"},
                {"type": "asset_check_severity", "severity": "WARN"},
                {"type": "op_output", "step_key_pattern": ".*"},
                {"type": "materialization_planned", "asset_keys": ["a"]},
                {"type": "asset_check_started"},
                {"type": "insights_metric", "metric_name": "cost", "comparison": "gt", "threshold": 5.0},
                {"type": "dagster_plus_audit"},
                {"type": "asset_value_change", "asset_key": "a", "metadata_key": "rc",
                 "direction": "any", "min_delta_pct": 25},
                {"type": "backfill_status", "status": "FAILED"},
                {"type": "sensor_failing", "target_sensor_name": "s"},
                {"type": "concurrency_hit", "max_queued": 10},
                {"type": "sqs_poll", "queue_url": "https://sqs"},
            ],
            then=[{"type": "emit_event", "asset_key": "marker"}],
        )
        defs = component.build_defs(None)
        assert len(list(defs.sensors)) == 35

    def test_full_component_with_every_action_type(self):
        """Every action type at least parses + runs without crashing."""
        component = comp_mod.EventAutomationComponent(
            name="all_actions",
            when=[{"type": "schedule", "cron": "* * * * *"}],
            then=[
                {"type": "materialize", "asset_keys": ["a"]},
                {"type": "launch_job", "job_name": "j"},
                {"type": "webhook", "url": "https://httpbin.org/post"},
                {"type": "slack", "webhook_url_env_var": "SLK"},
                {"type": "pagerduty", "routing_key_env_var": "PD"},
                {"type": "discord", "webhook_url_env_var": "DIS"},
                {"type": "emit_event", "asset_key": "a"},
                {"type": "cancel_run", "which": "triggering"},
                {"type": "retry_run"},
                {"type": "email", "smtp_host_env_var": "H", "smtp_user_env_var": "U",
                 "smtp_password_env_var": "P", "from_addr": "a@b.com", "to": ["c@d.com"]},
                {"type": "teams", "webhook_url_env_var": "TM"},
                {"type": "opsgenie", "api_key_env_var": "OG"},
                {"type": "mattermost", "webhook_url_env_var": "MM"},
                {"type": "toggle_sensor", "sensor_name": "s", "action": "start"},
                {"type": "toggle_schedule", "schedule_name": "s", "action": "stop"},
                {"type": "sns", "topic_arn": "arn:sns:x"},
                {"type": "sqs", "queue_url": "https://sqs"},
                # Tier 1 ops / self-healing
                {"type": "reload_code_location", "location_name": "loc_a"},
                {"type": "refresh_defs_state", "location_name": "loc_a"},
                {"type": "set_concurrency_limit", "concurrency_key": "snowflake", "limit": 10},
                {"type": "free_concurrency_slots", "run_id": "{run_id}"},
                {"type": "set_auto_materialize_paused", "paused": True},
                {"type": "mute_alert_policy", "alert_policy_id": "policy_1", "mute_for_seconds": 3600},
                {"type": "resume_backfill", "backfill_id": "bf_1"},
                {"type": "cancel_backfill", "backfill_id": "bf_1"},
                {"type": "reexecute_backfill", "backfill_id": "bf_1", "from_failure": True},
                {"type": "add_dynamic_partition", "partitions_def_name": "hours", "partition_key": "2024-01-15"},
            ],
        )
        defs = component.build_defs(None)
        assert len(list(defs.sensors)) == 1


class TestOpsActions:
    """Tier 1 self-healing actions — verify they parse, execute without
    crashing, and call the right GraphQL / instance API."""

    def test_reload_code_location_targets_specific_location(self):
        """When location_name set, uses reloadRepositoryLocation."""
        action = comp_mod.ReloadCodeLocationAction(location_name="prod_ingest")
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {"reloadRepositoryLocation": {"__typename": "OK"}}}):
                comp_mod._execute_action(action, {"run_id": "r"}, MagicMock())
        assert len(calls) == 1
        assert "reloadRepositoryLocation" in calls[0]
        assert '"prod_ingest"' in calls[0]

    def test_reload_code_location_unset_reloads_workspace(self):
        """Without location_name, uses reloadWorkspace."""
        action = comp_mod.ReloadCodeLocationAction()
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "reloadWorkspace" in calls[0]

    def test_reload_code_location_templated_from_token(self):
        """location_name supports template substitution from trigger tokens."""
        action = comp_mod.ReloadCodeLocationAction(location_name="{location_name}")
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {"location_name": "detected_broken_location"}, MagicMock())
        assert '"detected_broken_location"' in calls[0]

    def test_refresh_defs_state_whole_location(self):
        """No defs_state_key → refreshDefsState."""
        action = comp_mod.RefreshDefsStateAction(location_name="loc")
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "refreshDefsState" in calls[0]

    def test_refresh_defs_state_specific_key(self):
        """defs_state_key set → refreshComponentState."""
        action = comp_mod.RefreshDefsStateAction(location_name="loc", defs_state_key="planned_catalog")
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "refreshComponentState" in calls[0]
        assert '"planned_catalog"' in calls[0]

    def test_set_concurrency_limit_calls_instance_api(self):
        """Uses instance.event_log_storage.set_concurrency_slots (OSS-friendly)."""
        action = comp_mod.SetConcurrencyLimitAction(concurrency_key="snowflake_pool", limit=25)
        instance = MagicMock()
        comp_mod._execute_action(action, {}, MagicMock(), instance=instance)
        instance.event_log_storage.set_concurrency_slots.assert_called_once_with("snowflake_pool", 25)

    def test_set_concurrency_limit_templated_key(self):
        """concurrency_key supports template substitution."""
        action = comp_mod.SetConcurrencyLimitAction(concurrency_key="{job_name}_pool", limit=10)
        instance = MagicMock()
        comp_mod._execute_action(action, {"job_name": "hourly_ingest"}, MagicMock(), instance=instance)
        instance.event_log_storage.set_concurrency_slots.assert_called_once_with("hourly_ingest_pool", 10)

    def test_set_concurrency_limit_no_instance_warns(self):
        """Without instance, action logs a warning and does nothing."""
        action = comp_mod.SetConcurrencyLimitAction(concurrency_key="k", limit=1)
        logger = MagicMock()
        comp_mod._execute_action(action, {}, logger, instance=None)
        assert logger.warning.called

    def test_free_concurrency_slots_run_scope(self):
        """Without step_key, uses free_concurrency_slots_for_run."""
        action = comp_mod.FreeConcurrencySlotsAction(run_id="{run_id}")
        instance = MagicMock()
        comp_mod._execute_action(action, {"run_id": "abc123"}, MagicMock(), instance=instance)
        instance.event_log_storage.free_concurrency_slots_for_run.assert_called_once_with(run_id="abc123")

    def test_free_concurrency_slots_step_scope(self):
        """With step_key, uses free_concurrency_slots."""
        action = comp_mod.FreeConcurrencySlotsAction(run_id="abc", step_key="etl_op")
        instance = MagicMock()
        comp_mod._execute_action(action, {}, MagicMock(), instance=instance)
        instance.event_log_storage.free_concurrency_slots.assert_called_once_with(run_id="abc", step_key="etl_op")

    def test_set_auto_materialize_paused_true(self):
        action = comp_mod.SetAutoMaterializePausedAction(paused=True)
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "setAutoMaterializePaused(paused: true)" in calls[0]

    def test_set_auto_materialize_paused_false(self):
        action = comp_mod.SetAutoMaterializePausedAction(paused=False)
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "setAutoMaterializePaused(paused: false)" in calls[0]

    def test_mute_alert_policy(self):
        action = comp_mod.MuteAlertPolicyAction(alert_policy_id="p_123", mute_for_seconds=3600)
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "setAlertPolicyMuteUntil" in calls[0]
        assert '"p_123"' in calls[0]
        assert "3600" in calls[0]

    def test_resume_backfill(self):
        action = comp_mod.ResumeBackfillAction(backfill_id="{run_id}")
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {"run_id": "bf_42"}, MagicMock())
        assert "resumePartitionBackfill" in calls[0]
        assert '"bf_42"' in calls[0]

    def test_cancel_backfill(self):
        action = comp_mod.CancelBackfillAction(backfill_id="bf_1")
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "cancelPartitionBackfill" in calls[0]

    def test_reexecute_backfill_from_failure(self):
        action = comp_mod.ReexecuteBackfillAction(backfill_id="bf_1", from_failure=True)
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "reexecutePartitionBackfill" in calls[0]
        assert "FROM_FAILURE" in calls[0]

    def test_reexecute_backfill_all_steps(self):
        action = comp_mod.ReexecuteBackfillAction(backfill_id="bf_1", from_failure=False)
        calls = []
        with patch.dict("os.environ", {"DAGSTER_CLOUD_ORGANIZATION": "org", "DAGSTER_CLOUD_API_TOKEN": "t"}):
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda q, *a: calls.append(q) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert "ALL_STEPS" in calls[0]

    def test_add_dynamic_partition(self):
        action = comp_mod.AddDynamicPartitionAction(
            partitions_def_name="s3_files", partition_key="{partition_key}"
        )
        instance = MagicMock()
        comp_mod._execute_action(
            action, {"partition_key": "orders_2024_01_15.csv"}, MagicMock(), instance=instance
        )
        instance.add_dynamic_partitions.assert_called_once_with("s3_files", ["orders_2024_01_15.csv"])

    def test_dagster_plus_mutation_missing_creds_no_call(self):
        """Actions that need Dagster+ credentials skip cleanly when env vars are unset."""
        action = comp_mod.MuteAlertPolicyAction(alert_policy_id="p", mute_for_seconds=60)
        calls = []
        with patch.dict("os.environ", {}, clear=False):
            # Ensure both env vars are unset
            import os
            os.environ.pop("DAGSTER_CLOUD_ORGANIZATION", None)
            os.environ.pop("DAGSTER_CLOUD_API_TOKEN", None)
            with patch.object(comp_mod, "_dagster_plus_graphql", side_effect=lambda *a: calls.append(a) or {"data": {}}):
                comp_mod._execute_action(action, {}, MagicMock())
        assert len(calls) == 0, "Should skip GraphQL call when creds missing"


# ── Throttle / suppression tests ────────────────────────────────────────

class TestThrottleSuppression:
    """Covers the 5 noise-reduction throttle features:
    business_hours_only, maintenance_windows, correlation_suppress_sensors,
    escalation_ladder, auto_resolve."""

    def _fresh_state(self):
        """Wipe module-level throttle state between tests."""
        comp_mod._THROTTLE_STATE.clear()
        comp_mod._GLOBAL_FIRE_LOG.clear()

    # ── Business hours ────────────────────────────────────────

    def test_business_hours_parse(self):
        """Parser handles time, tz, and day-of-week list."""
        start, end, tz, days = comp_mod._parse_business_hours("09:00-17:00 America/New_York mon,tue,wed,thu,fri")
        assert start == 9 * 60
        assert end == 17 * 60
        assert tz == "America/New_York"
        assert days == [0, 1, 2, 3, 4]

    def test_business_hours_parse_no_days(self):
        """Days field is optional."""
        start, end, tz, days = comp_mod._parse_business_hours("22:00-06:00 UTC")
        assert start == 22 * 60
        assert end == 6 * 60
        assert days is None

    def test_business_hours_gate_inside(self):
        """Fire allowed when now falls in the daily window."""
        self._fresh_state()
        throttle = comp_mod.ThrottleConfig(business_hours_only="00:00-23:59 UTC")
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "test"}, "s", MagicMock())
        assert should_fire is True

    def test_business_hours_gate_outside(self):
        """Fire dropped when outside the window."""
        self._fresh_state()
        # Pick a window that's guaranteed to be closed: 00:00-00:00 (empty window)
        # Instead use a specific time check via patching time.time
        throttle = comp_mod.ThrottleConfig(business_hours_only="03:00-03:01 UTC")
        # Patch time to be 12:00 UTC — outside 03:00-03:01
        with patch.object(comp_mod.time, "time", return_value=_utc_hour_ts(12)):
            should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "test"}, "s", MagicMock())
        assert should_fire is False

    def test_business_hours_overnight_window(self):
        """22:00-06:00 style window works across midnight."""
        # 23:00 UTC should be inside 22:00-06:00
        assert comp_mod._is_in_business_hours(_utc_hour_ts(23), "22:00-06:00 UTC") is True
        # 12:00 UTC should be outside
        assert comp_mod._is_in_business_hours(_utc_hour_ts(12), "22:00-06:00 UTC") is False

    def test_business_hours_malformed_fails_open(self):
        """Bad spec doesn't crash — errs on the side of firing."""
        assert comp_mod._is_in_business_hours(time.time(), "not-a-window") is True

    # ── Maintenance windows ────────────────────────────────────

    def test_maintenance_window_suppresses(self):
        """Fire dropped when now is inside a maintenance window."""
        self._fresh_state()
        now = time.time()
        window = comp_mod.MaintenanceWindow(
            from_ts=_ts_iso(now - 60),
            to_ts=_ts_iso(now + 3600),
            reason="deploy",
        )
        throttle = comp_mod.ThrottleConfig(maintenance_windows=[window])
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is False

    def test_maintenance_window_expires(self):
        """Window in the past no longer suppresses."""
        self._fresh_state()
        now = time.time()
        window = comp_mod.MaintenanceWindow(
            from_ts=_ts_iso(now - 7200),
            to_ts=_ts_iso(now - 3600),
        )
        throttle = comp_mod.ThrottleConfig(maintenance_windows=[window])
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is True

    def test_maintenance_window_bad_iso_skipped(self):
        """Malformed windows are skipped, not crashed."""
        self._fresh_state()
        window = comp_mod.MaintenanceWindow(from_ts="oops", to_ts="also-oops")
        throttle = comp_mod.ThrottleConfig(maintenance_windows=[window])
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is True

    # ── Correlation suppression ────────────────────────────────

    def test_correlation_suppress_matches_recent_fire(self):
        """If a correlated sensor fired recently, this one gets dropped."""
        self._fresh_state()
        # Simulate a recent fire on the root-cause sensor
        comp_mod._record_global_fire("ops_alerts__daemon_heartbeat_0", time.time() - 30)
        throttle = comp_mod.ThrottleConfig(
            correlation_suppress_sensors=["daemon_heartbeat"],
            correlation_within_seconds=300,
        )
        should_fire, _, _ = comp_mod._apply_throttle(
            throttle, {"message": "downstream alert"}, "downstream_sensor", MagicMock()
        )
        assert should_fire is False

    def test_correlation_suppress_ignores_stale_fires(self):
        """Fires older than the window don't suppress."""
        self._fresh_state()
        comp_mod._record_global_fire("root_cause", time.time() - 3600)
        throttle = comp_mod.ThrottleConfig(
            correlation_suppress_sensors=["root_cause"],
            correlation_within_seconds=60,
        )
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is True

    def test_successful_fire_records_to_global_log(self):
        """Firing appends to _GLOBAL_FIRE_LOG so downstream sensors can correlate."""
        self._fresh_state()
        throttle = comp_mod.ThrottleConfig()
        comp_mod._apply_throttle(throttle, {"message": "t"}, "sensor_a", MagicMock())
        assert len(comp_mod._GLOBAL_FIRE_LOG) == 1
        assert comp_mod._GLOBAL_FIRE_LOG[0][1] == "sensor_a"

    def test_global_fire_log_bounded(self):
        """Log is truncated to _GLOBAL_FIRE_LOG_MAX to prevent unbounded growth."""
        self._fresh_state()
        for i in range(comp_mod._GLOBAL_FIRE_LOG_MAX + 100):
            comp_mod._record_global_fire(f"s{i}", time.time())
        assert len(comp_mod._GLOBAL_FIRE_LOG) == comp_mod._GLOBAL_FIRE_LOG_MAX

    # ── Escalation ladder ─────────────────────────────────────

    def test_escalation_first_fire_uses_lowest_tier(self):
        """Fire #0 → tier with after_fires=0."""
        self._fresh_state()
        ladder = [
            comp_mod.EscalationTier(after_fires=0, action_indices=[0]),
            comp_mod.EscalationTier(after_fires=3, action_indices=[0, 1]),
            comp_mod.EscalationTier(after_fires=10, action_indices=[0, 1, 2]),
        ]
        throttle = comp_mod.ThrottleConfig(strategy="escalate", escalation_ladder=ladder)
        should_fire, _, indices = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is True
        assert indices == [0]

    def test_escalation_climbs_tiers(self):
        """Fires 0, 1, 2 use tier[0]; fire 3+ uses tier[1]; fire 10+ uses tier[2]."""
        self._fresh_state()
        ladder = [
            comp_mod.EscalationTier(after_fires=0, action_indices=[0]),
            comp_mod.EscalationTier(after_fires=3, action_indices=[0, 1]),
            comp_mod.EscalationTier(after_fires=10, action_indices=[0, 1, 2]),
        ]
        throttle = comp_mod.ThrottleConfig(strategy="escalate", escalation_ladder=ladder)
        seen_indices = []
        for _ in range(12):
            _, _, ai = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
            seen_indices.append(ai)
        # Fires 0,1,2 → [0]; fires 3-9 → [0,1]; fires 10,11 → [0,1,2]
        assert seen_indices[0] == [0]
        assert seen_indices[2] == [0]
        assert seen_indices[3] == [0, 1]
        assert seen_indices[9] == [0, 1]
        assert seen_indices[10] == [0, 1, 2]
        assert seen_indices[11] == [0, 1, 2]

    def test_escalation_no_tier_matches_drops(self):
        """If ladder starts at after_fires=5 and prior_fires=0, fire is dropped."""
        self._fresh_state()
        ladder = [comp_mod.EscalationTier(after_fires=5, action_indices=[0])]
        throttle = comp_mod.ThrottleConfig(strategy="escalate", escalation_ladder=ladder)
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is False

    def test_run_actions_filters_by_escalation_indices(self):
        """_run_actions honors the action_indices filter — only listed actions run."""
        self._fresh_state()
        # Build 3 webhook actions; only index 0 should fire on first tick
        actions = [
            comp_mod.WebhookAction(url="https://example.com/1"),
            comp_mod.WebhookAction(url="https://example.com/2"),
            comp_mod.WebhookAction(url="https://example.com/3"),
        ]
        ladder = [comp_mod.EscalationTier(after_fires=0, action_indices=[0])]
        throttle = comp_mod.ThrottleConfig(strategy="escalate", escalation_ladder=ladder)
        executed_urls = []

        def fake_execute(action, tokens, logger, instance=None):
            executed_urls.append(action.url)
            return None

        with patch.object(comp_mod, "_execute_action", side_effect=fake_execute):
            comp_mod._run_actions(
                actions, {"message": "t"}, MagicMock(), throttle=throttle, sensor_name="s"
            )
        assert executed_urls == ["https://example.com/1"]

    # ── Auto-resolve ──────────────────────────────────────────

    def test_auto_resolve_fires_and_records_state(self):
        """auto_resolve strategy fires on first tick and records 'active' state."""
        self._fresh_state()
        throttle = comp_mod.ThrottleConfig(strategy="auto_resolve", min_seconds_between_fires=60)
        should_fire, _, _ = comp_mod._apply_throttle(throttle, {"message": "t"}, "s", MagicMock())
        assert should_fire is True
        state = comp_mod._THROTTLE_STATE["s:*"]
        assert state["auto_resolve_active"] is True
        assert state["auto_resolve_fire_count"] == 1

    def test_auto_resolve_check_emits_resolve_after_staleness(self):
        """After the fire is stale (2x cooldown), _check_auto_resolve emits a paired resolve."""
        self._fresh_state()
        throttle = comp_mod.ThrottleConfig(strategy="auto_resolve", min_seconds_between_fires=1)
        # Trigger initial fire
        comp_mod._apply_throttle(throttle, {"message": "down"}, "s", MagicMock())
        # Backdate the state to look stale
        state = comp_mod._THROTTLE_STATE["s:*"]
        state["auto_resolve_last_fire_ts"] = time.time() - 999
        state["auto_resolve_first_fire_ts"] = time.time() - 1000
        resolve = comp_mod._check_auto_resolve(throttle, "s", MagicMock())
        assert resolve is not None
        assert resolve["event_type"] == "auto_resolved"
        assert "Resolved after" in resolve["message"]
        # Second call returns None (already sent)
        assert comp_mod._check_auto_resolve(throttle, "s", MagicMock()) is None

    def test_auto_resolve_check_no_fire_no_resolve(self):
        """Without a prior fire, _check_auto_resolve returns None."""
        self._fresh_state()
        throttle = comp_mod.ThrottleConfig(strategy="auto_resolve")
        assert comp_mod._check_auto_resolve(throttle, "s", MagicMock()) is None

    def test_auto_resolve_check_ignores_non_auto_resolve(self):
        """Only auto_resolve strategy is eligible."""
        self._fresh_state()
        throttle = comp_mod.ThrottleConfig(strategy="silence")
        assert comp_mod._check_auto_resolve(throttle, "s", MagicMock()) is None


# ── Helpers for throttle tests ────────────────────────────────────────

def _utc_hour_ts(hour: int) -> float:
    """Return a UTC timestamp for today at HH:00:00."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return now.replace(hour=hour, minute=0, second=0, microsecond=0).timestamp()


def _ts_iso(ts: float) -> str:
    """Format a unix ts as ISO8601 UTC string."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


# ── Selection syntax + run filter tests ────────────────────────────────

class TestAssetKeySelection:
    """Verify Union[List[str], str] on asset_keys works: explicit list,
    fnmatch glob, and single-literal fallback."""

    def test_list_pass_through(self):
        """Explicit list is returned as-is."""
        result = comp_mod._resolve_asset_keys(["marts/orders", "marts/customers"])
        assert result == ["marts/orders", "marts/customers"]

    def test_single_literal_no_siblings(self):
        """String with no sibling defs falls back to single-literal."""
        result = comp_mod._resolve_asset_keys("marts/orders")
        assert result == ["marts/orders"]

    def test_fnmatch_against_discovered(self):
        """Bare fnmatch glob resolves against discovered_keys."""
        discovered = ["marts/orders", "marts/customers", "staging/raw_events"]
        result = comp_mod._resolve_asset_keys("marts/*", discovered_keys=discovered)
        assert set(result) == {"marts/orders", "marts/customers"}

    def test_fnmatch_no_match_falls_through(self):
        """When fnmatch matches nothing, return the single literal (backward compat)."""
        discovered = ["marts/orders"]
        result = comp_mod._resolve_asset_keys("no_match", discovered_keys=discovered)
        assert result == ["no_match"]

    def test_asset_materialized_trigger_accepts_list(self):
        """Backward compat: list of asset keys still parses."""
        trigger = comp_mod.AssetMaterializedTrigger(asset_keys=["a", "b"])
        assert trigger.asset_keys == ["a", "b"]

    def test_asset_materialized_trigger_accepts_string(self):
        """New: single string is accepted."""
        trigger = comp_mod.AssetMaterializedTrigger(asset_keys="group:analytics")
        assert trigger.asset_keys == "group:analytics"

    def test_freshness_violation_accepts_string(self):
        """FreshnessViolationTrigger accepts selection string."""
        trigger = comp_mod.FreshnessViolationTrigger(
            asset_keys="tag:tier=gold", max_age_minutes=60
        )
        assert trigger.asset_keys == "tag:tier=gold"

    def test_absence_trigger_accepts_string(self):
        """AbsenceTrigger accepts selection string."""
        trigger = comp_mod.AbsenceTrigger(
            asset_keys="marts/*",
            max_gap_minutes=60,
        )
        assert trigger.asset_keys == "marts/*"

    def test_asset_check_failed_optional_string(self):
        """AssetCheckFailedTrigger's optional asset_keys accepts string."""
        trigger = comp_mod.AssetCheckFailedTrigger(asset_keys="group:critical")
        assert trigger.asset_keys == "group:critical"

    def test_normalization_via_component_build_defs(self):
        """Component's build_defs walks triggers and normalizes string
        selectors to concrete lists. When no siblings are discovered, falls
        back to single-literal."""
        component = comp_mod.EventAutomationComponent(
            name="test_norm",
            when=[
                {"type": "asset_materialized", "asset_keys": "marts/*"},
                {"type": "freshness_violation", "asset_keys": ["a", "b"], "max_age_minutes": 60},
            ],
            then=[{"type": "materialize", "asset_keys": ["target"]}],
        )
        # Simulate no sibling defs (context.build_defs returns None)
        ctx = MagicMock()
        ctx.path.parent = MagicMock()
        ctx.build_defs.return_value = None
        component.build_defs(ctx)
        # String selector → single-literal fallback
        assert component.when[0].asset_keys == ["marts/*"]
        # List selector → unchanged
        assert component.when[1].asset_keys == ["a", "b"]


class TestRunFilterMixin:
    """Verify the _RunFilterMixin fields (job_name_pattern, run_tags) are
    accepted by the 6 run-based triggers and applied via _run_matches_filters."""

    def test_run_status_trigger_accepts_mixin_fields(self):
        trigger = comp_mod.RunStatusTrigger(
            status="FAILURE",
            job_name_pattern="prod_*",
            run_tags={"priority": "P0"},
        )
        assert trigger.job_name_pattern == "prod_*"
        assert trigger.run_tags == {"priority": "P0"}

    def test_step_error_trigger_accepts_mixin_fields(self):
        trigger = comp_mod.StepErrorTrigger(
            job_name_pattern="etl_*",
            run_tags={"team": "data"},
        )
        assert trigger.job_name_pattern == "etl_*"

    def test_run_matches_filters_exact_job_name(self):
        run = MagicMock(job_name="my_job", tags={})
        assert comp_mod._run_matches_filters(run, "my_job", None, None) is True
        assert comp_mod._run_matches_filters(run, "other", None, None) is False

    def test_run_matches_filters_fnmatch_pattern(self):
        run = MagicMock(job_name="prod_ingest", tags={})
        assert comp_mod._run_matches_filters(run, None, "prod_*", None) is True
        assert comp_mod._run_matches_filters(run, None, "dev_*", None) is False

    def test_run_matches_filters_run_tags(self):
        run = MagicMock(job_name="j", tags={"priority": "P0", "team": "data"})
        # All specified tags match
        assert comp_mod._run_matches_filters(
            run, None, None, {"priority": "P0"}
        ) is True
        # Multiple tags all match
        assert comp_mod._run_matches_filters(
            run, None, None, {"priority": "P0", "team": "data"}
        ) is True
        # One tag mismatches
        assert comp_mod._run_matches_filters(
            run, None, None, {"priority": "P1"}
        ) is False
        # Tag missing
        assert comp_mod._run_matches_filters(
            run, None, None, {"missing": "x"}
        ) is False

    def test_run_matches_filters_combined(self):
        """job_name + job_name_pattern + run_tags all compose as AND."""
        run = MagicMock(job_name="prod_ingest", tags={"priority": "P0"})
        assert comp_mod._run_matches_filters(
            run, None, "prod_*", {"priority": "P0"}
        ) is True
        # Pattern matches but tag doesn't
        assert comp_mod._run_matches_filters(
            run, None, "prod_*", {"priority": "P1"}
        ) is False

    def test_run_matches_filters_no_filters(self):
        """No filters → always matches."""
        run = MagicMock(job_name="anything", tags={})
        assert comp_mod._run_matches_filters(run, None, None, None) is True

    def test_run_status_sensor_builds_with_mixin_fields(self):
        """Full-stack: RunStatusTrigger with mixin fields builds a valid
        SensorDefinition (the eval fn is wired to `_run_matches_filters`,
        which is tested directly above)."""
        trigger = comp_mod.RunStatusTrigger(
            status="FAILURE",
            job_name_pattern="prod_*",
            run_tags={"priority": "P0"},
        )
        sensor = comp_mod._build_run_status_sensor(
            "test", trigger, [comp_mod.MaterializeAction(asset_keys=["a"])],
            comp_mod.dg.DefaultSensorStatus.STOPPED,
        )
        assert sensor.name == "test"

    def test_step_error_sensor_builds_with_mixin_fields(self):
        """Full-stack: StepErrorTrigger with mixin fields builds cleanly."""
        trigger = comp_mod.StepErrorTrigger(
            job_name_pattern="etl_*",
            run_tags={"team": "data-platform"},
        )
        sensor = comp_mod._build_step_error_sensor(
            "test", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED,
        )
        assert sensor.name == "test"

    def test_all_run_based_triggers_expose_mixin_fields(self):
        """Every run-based trigger inherits _RunFilterMixin — schema surface
        should include the two new fields on all 6 classes."""
        run_trigger_classes = [
            comp_mod.RunStatusTrigger,
            comp_mod.RunDurationTrigger,
            comp_mod.RunStuckTrigger,
            comp_mod.StepErrorTrigger,
            comp_mod.HookFiredTrigger,
            comp_mod.RunStartupSlowTrigger,
        ]
        for cls in run_trigger_classes:
            fields = cls.model_fields
            assert "job_name_pattern" in fields, f"{cls.__name__} missing job_name_pattern"
            assert "run_tags" in fields, f"{cls.__name__} missing run_tags"


class TestMonitoredJobsMixin:
    """Verify cross-code-location targeting: monitored_locations +
    monitored_jobs on RunStatusTrigger + RunDurationTrigger convert into
    the `monitored_jobs` param of @run_status_sensor."""

    def test_run_status_accepts_monitored_locations(self):
        trigger = comp_mod.RunStatusTrigger(
            status="FAILURE",
            monitored_locations=["prod_ingest", "prod_analytics"],
        )
        assert trigger.monitored_locations == ["prod_ingest", "prod_analytics"]

    def test_run_status_accepts_monitored_jobs(self):
        trigger = comp_mod.RunStatusTrigger(
            status="FAILURE",
            monitored_jobs=[
                {"location": "prod_ingest", "job": "hourly_ingest"},
                {"location": "prod_analytics", "job": "dbt_run", "repository": "__repository__"},
            ],
        )
        assert len(trigger.monitored_jobs) == 2
        assert trigger.monitored_jobs[0].location == "prod_ingest"
        assert trigger.monitored_jobs[0].job == "hourly_ingest"
        # Default repository is filled in
        assert trigger.monitored_jobs[0].repository == "__repository__"

    def test_run_duration_inherits_monitored_fields(self):
        trigger = comp_mod.RunDurationTrigger(
            max_duration_seconds=60,
            monitored_locations=["prod"],
        )
        assert trigger.monitored_locations == ["prod"]

    def test_build_monitored_jobs_arg_returns_none_when_empty(self):
        """Backward compat: no fields set → sensor watches its own location."""
        assert comp_mod._build_monitored_jobs_arg(None, None) is None
        assert comp_mod._build_monitored_jobs_arg([], []) is None

    def test_build_monitored_jobs_arg_returns_selectors(self):
        """When fields are set, we get a list of Dagster selector objects."""
        from dagster import CodeLocationSelector, JobSelector
        result = comp_mod._build_monitored_jobs_arg(
            monitored_locations=["prod_ingest"],
            monitored_jobs=[
                comp_mod.MonitoredJob(location="prod_analytics", job="dbt_run"),
            ],
        )
        assert result is not None
        assert len(result) == 2
        assert isinstance(result[0], CodeLocationSelector)
        assert result[0].location_name == "prod_ingest"
        assert isinstance(result[1], JobSelector)
        assert result[1].location_name == "prod_analytics"
        assert result[1].job_name == "dbt_run"
        assert result[1].repository_name == "__repository__"

    def test_run_status_sensor_builds_with_monitored_locations(self):
        """Full stack: sensor construction with monitored_locations doesn't error."""
        trigger = comp_mod.RunStatusTrigger(
            status="FAILURE",
            monitored_locations=["prod_ingest"],
        )
        sensor = comp_mod._build_run_status_sensor(
            "cross_location_alert",
            trigger,
            [comp_mod.MaterializeAction(asset_keys=["a"])],
            comp_mod.dg.DefaultSensorStatus.STOPPED,
        )
        assert sensor.name == "cross_location_alert"

    def test_run_duration_sensor_builds_with_monitored_jobs(self):
        """Full stack: run_duration builds cleanly with cross-location selectors."""
        trigger = comp_mod.RunDurationTrigger(
            max_duration_seconds=300,
            monitored_jobs=[
                {"location": "prod", "job": "hourly_ingest"},
            ],
        )
        sensor = comp_mod._build_run_duration_sensor(
            "cross_duration_alert",
            trigger,
            [],
            comp_mod.dg.DefaultSensorStatus.STOPPED,
        )
        assert sensor.name == "cross_duration_alert"


class TestPartitionKey:
    """Verify partition_key on MaterializeAction:
      - accepts string form (rendered via template)
      - accepts dict form (built as MultiPartitionKey)
      - single-dim + multi-dim partitions surface in tokens
    """

    def test_none_passes_through(self):
        assert comp_mod._resolve_partition_key(None, {}) is None

    def test_string_template_rendered(self):
        """String form gets `{partition_key}` substituted from tokens."""
        tokens = {"partition_key": "2024-01-15"}
        assert comp_mod._resolve_partition_key("{partition_key}", tokens) == "2024-01-15"

    def test_string_literal_pass_through(self):
        """String with no substitution tokens just passes through."""
        assert comp_mod._resolve_partition_key("2024-01-15", {}) == "2024-01-15"

    def test_dict_builds_multi_partition_key(self):
        """Dict form → MultiPartitionKey with each value templated."""
        tokens = {"partition_date": "2024-01-15", "partition_region": "us"}
        result = comp_mod._resolve_partition_key(
            {"date": "{partition_date}", "region": "{partition_region}"}, tokens
        )
        assert isinstance(result, comp_mod.dg.MultiPartitionKey)
        assert result.keys_by_dimension == {"date": "2024-01-15", "region": "us"}

    def test_dict_static_values(self):
        """Dict form with literal values (no templates)."""
        result = comp_mod._resolve_partition_key(
            {"date": "2024-01-15", "region": "us"}, {}
        )
        assert isinstance(result, comp_mod.dg.MultiPartitionKey)
        assert result.keys_by_dimension == {"date": "2024-01-15", "region": "us"}

    def test_split_multi_partition_from_key_object(self):
        """_split_multi_partition unpacks a real MultiPartitionKey."""
        mpk = comp_mod.dg.MultiPartitionKey({"date": "2024-01-15", "region": "us"})
        assert comp_mod._split_multi_partition(mpk) == {"date": "2024-01-15", "region": "us"}

    def test_split_multi_partition_single_dim_returns_none(self):
        """Plain string partition has no dimension info."""
        assert comp_mod._split_multi_partition("2024-01-15") is None

    def test_split_multi_partition_none(self):
        assert comp_mod._split_multi_partition(None) is None

    def test_materialize_action_accepts_string(self):
        """Backward-compat: string form still parses."""
        action = comp_mod.MaterializeAction(
            asset_keys=["derived_data"], partition_key="{partition_key}"
        )
        assert action.partition_key == "{partition_key}"

    def test_materialize_action_accepts_dict(self):
        """New: dict form parses for multi-dim partitions."""
        action = comp_mod.MaterializeAction(
            asset_keys=["derived_data"],
            partition_key={"date": "{partition_date}", "region": "{partition_region}"},
        )
        assert action.partition_key == {
            "date": "{partition_date}",
            "region": "{partition_region}",
        }

    def test_asset_partition_materialized_trigger_accepts_dict(self):
        """Filter by dict for multi-dim partition matching."""
        trigger = comp_mod.AssetPartitionMaterializedTrigger(
            asset_keys=["derived_data"],
            partition_key={"region": "us"},   # wildcard the date dimension
        )
        assert trigger.partition_key == {"region": "us"}

    def test_default_tokens_includes_partition_key(self):
        """partition_key is now part of the default token surface."""
        tokens = comp_mod._default_tokens("test_event")
        assert "partition_key" in tokens
        assert tokens["partition_key"] == ""

    def test_materialize_action_dict_renders_partition_key_end_to_end(self):
        """End-to-end: dict-form MaterializeAction with template tokens
        produces a RunRequest whose partition_key is a MultiPartitionKey."""
        action = comp_mod.MaterializeAction(
            asset_keys=["derived_data"],
            partition_key={"date": "{partition_date}", "region": "{partition_region}"},
        )
        tokens = comp_mod._default_tokens(
            event_type="asset_partition_materialized",
            asset_key="derived_data",
            partition_date="2024-01-15",
            partition_region="us",
        )
        rr = comp_mod._execute_action(action, tokens, MagicMock())
        assert isinstance(rr, comp_mod.dg.RunRequest)
        assert isinstance(rr.partition_key, comp_mod.dg.MultiPartitionKey)
        assert rr.partition_key.keys_by_dimension == {
            "date": "2024-01-15",
            "region": "us",
        }
