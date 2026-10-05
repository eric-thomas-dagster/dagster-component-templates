"""Tests for OpenaiBatchStatusSensorComponent. Mocks `_build_openai_client`
-- the real (paid) OpenAI Batch API is never called."""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
OpenaiBatchStatusSensorComponent = mod.OpenaiBatchStatusSensorComponent


class FakeBatch:
    def __init__(self, id, status):
        self.id = id
        self.status = status


class FakeClient:
    def __init__(self, batch):
        self._batch = batch
        self.retrieved_ids = []

        class _Batches:
            def retrieve(_self, batch_id):
                self.retrieved_ids.append(batch_id)
                return self._batch

        self.batches = _Batches()


def _component(**overrides):
    attrs = dict(
        sensor_name="support_reply_batch_done",
        watch_asset_key="support_reply_batch",
        job_name="fetch_support_reply_results",
        results_asset_key="support_reply_results",
    )
    attrs.update(overrides)
    return OpenaiBatchStatusSensorComponent(**attrs)


def _build_sensor(component):
    defs = component.build_defs(None)
    return list(defs.sensors)[0]


def _skip_text(result: dg.SensorResult) -> str:
    """SensorResult.skip_reason is a SkipReason NamedTuple(skip_message=...),
    not a plain string -- unwrap it for substring assertions."""
    return result.skip_reason.skip_message if result.skip_reason else ""


def _watched_asset_materialized_with(instance, batch_id):
    """Materialize a stand-in for the watched openai_batch_submit asset so
    the sensor has something real to read via get_latest_materialization_event."""
    @dg.asset(key=dg.AssetKey.from_user_string("support_reply_batch"))
    def _watched(context: dg.AssetExecutionContext):
        context.add_output_metadata({
            "batch_id": dg.MetadataValue.text(batch_id),
            "prompts_hash": dg.MetadataValue.text("deadbeef"),
            "status": dg.MetadataValue.text("validating"),
            "request_count": dg.MetadataValue.int(2),
        })
        return 1

    dg.Definitions(assets=[_watched]).get_implicit_global_asset_job_def().execute_in_process(instance=instance)


def test_skips_when_not_terminal(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    _watched_asset_materialized_with(instance, "batch-1")

    client = FakeClient(FakeBatch("batch-1", status="in_progress"))
    monkeypatch.setattr(mod, "_build_openai_client", lambda api_key: client)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")

    sensor_def = _build_sensor(_component())
    context = dg.build_sensor_context(instance=instance)
    result = sensor_def(context)

    assert isinstance(result, dg.SensorResult)
    assert not result.run_requests
    assert "not terminal" in _skip_text(result)


def test_fires_run_request_when_terminal(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    _watched_asset_materialized_with(instance, "batch-1")

    client = FakeClient(FakeBatch("batch-1", status="completed"))
    monkeypatch.setattr(mod, "_build_openai_client", lambda api_key: client)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")

    sensor_def = _build_sensor(_component())
    context = dg.build_sensor_context(instance=instance)
    result = sensor_def(context)

    assert len(result.run_requests) == 1
    rr = result.run_requests[0]
    expected_op_name = dg.AssetKey.from_user_string("support_reply_results").to_python_identifier()
    assert rr.run_config == {"ops": {expected_op_name: {"config": {"batch_id": "batch-1"}}}}
    assert result.cursor == "batch-1|completed"


def test_does_not_double_fire_via_cursor(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    _watched_asset_materialized_with(instance, "batch-1")

    client = FakeClient(FakeBatch("batch-1", status="completed"))
    monkeypatch.setattr(mod, "_build_openai_client", lambda api_key: client)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")

    sensor_def = _build_sensor(_component())

    context1 = dg.build_sensor_context(instance=instance, cursor=None)
    result1 = sensor_def(context1)
    assert len(result1.run_requests) == 1

    context2 = dg.build_sensor_context(instance=instance, cursor=result1.cursor)
    result2 = sensor_def(context2)
    assert not result2.run_requests
    assert "Already processed" in _skip_text(result2)


def test_no_materialization_skips(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    sensor_def = _build_sensor(_component())
    context = dg.build_sensor_context(instance=instance)
    result = sensor_def(context)
    assert not result.run_requests
    assert "No materialization" in _skip_text(result)


def test_missing_api_key_skips(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    _watched_asset_materialized_with(instance, "batch-1")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    sensor_def = _build_sensor(_component())
    context = dg.build_sensor_context(instance=instance)
    result = sensor_def(context)
    assert not result.run_requests
    assert "not set" in _skip_text(result)
