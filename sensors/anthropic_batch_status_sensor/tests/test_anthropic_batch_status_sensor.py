"""Tests for AnthropicBatchStatusSensorComponent. Mocks `anthropic.Anthropic`
-- the real (paid) Anthropic Batches API is never called."""
import types

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
AnthropicBatchStatusSensorComponent = mod.AnthropicBatchStatusSensorComponent


def _unwrap(result):
    if isinstance(result, list):
        return result[0] if result else None
    return result


def _install_fake_anthropic(monkeypatch, status_by_id):
    """status_by_id: dict[batch_id] -> processing_status string."""
    import anthropic

    class FakeBatch:
        def __init__(self, id, processing_status):
            self.id = id
            self.processing_status = processing_status

    class FakeMessagesBatches:
        def retrieve(self, batch_id):
            return FakeBatch(batch_id, status_by_id[batch_id])

    class FakeClient:
        def __init__(self, api_key=None):
            self.messages = types.SimpleNamespace(batches=FakeMessagesBatches())

    monkeypatch.setattr(anthropic, "Anthropic", FakeClient)


def _make_watched_materialization(instance, asset_name, batch_id):
    """Materialize a stand-in 'submit' asset carrying batch_id metadata, so
    the sensor has something real to read via get_latest_materialization_event."""
    @dg.asset(name=asset_name)
    def _submit(context: dg.AssetExecutionContext):
        context.add_output_metadata({"batch_id": dg.MetadataValue.text(batch_id)})
        return 1

    dg.materialize([_submit], instance=instance)


def _sensor_def(component):
    defs = component.build_defs(None)
    return list(defs.sensors)[0]


def _component(**overrides):
    attrs = dict(
        sensor_name="watch_sensor",
        watch_asset_key="submit_asset",
        job_name="results_job",
        results_asset_key="results_asset",
    )
    attrs.update(overrides)
    return AnthropicBatchStatusSensorComponent(**attrs)


def test_fires_run_request_when_ended(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    _install_fake_anthropic(monkeypatch, {"batch_42": "ended"})

    with dg.instance_for_test() as instance:
        _make_watched_materialization(instance, "submit_asset", "batch_42")

        sensor_def = _sensor_def(_component())
        context = dg.build_sensor_context(instance=instance)
        result = _unwrap(sensor_def(context))

        assert isinstance(result, dg.SensorResult)
        assert len(result.run_requests) == 1
        rr = result.run_requests[0]
        assert rr.run_config == {"ops": {"results_asset": {"config": {"batch_id": "batch_42"}}}}
        assert result.cursor == "batch_42|ended"


def test_op_name_derived_from_multi_segment_results_asset_key(monkeypatch):
    """A multi-segment results_asset_key (e.g. 'ai/results_asset') must
    derive the op name via AssetKey.to_python_identifier() (double
    underscore join), not just take the last path segment or the literal
    string 'config' (the bug already found+fixed in precisely_job_sensor)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    _install_fake_anthropic(monkeypatch, {"batch_42": "ended"})

    with dg.instance_for_test() as instance:
        _make_watched_materialization(instance, "submit_asset", "batch_42")

        sensor_def = _sensor_def(_component(results_asset_key="ai/results_asset"))
        context = dg.build_sensor_context(instance=instance)
        result = _unwrap(sensor_def(context))

        rr = result.run_requests[0]
        assert rr.run_config == {"ops": {"ai__results_asset": {"config": {"batch_id": "batch_42"}}}}


def test_skips_when_in_progress(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    _install_fake_anthropic(monkeypatch, {"batch_42": "in_progress"})

    with dg.instance_for_test() as instance:
        _make_watched_materialization(instance, "submit_asset", "batch_42")

        sensor_def = _sensor_def(_component())
        context = dg.build_sensor_context(instance=instance)
        result = _unwrap(sensor_def(context))

        assert isinstance(result, dg.SensorResult)
        assert not result.run_requests
        assert "in_progress" in result.skip_reason.skip_message


def test_does_not_double_fire_via_cursor(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    _install_fake_anthropic(monkeypatch, {"batch_42": "ended"})

    with dg.instance_for_test() as instance:
        _make_watched_materialization(instance, "submit_asset", "batch_42")

        sensor_def = _sensor_def(_component())

        ctx1 = dg.build_sensor_context(instance=instance)
        result1 = _unwrap(sensor_def(ctx1))
        assert len(result1.run_requests) == 1

        # Second evaluation with the cursor carried forward must skip.
        ctx2 = dg.build_sensor_context(instance=instance, cursor=result1.cursor)
        result2 = _unwrap(sensor_def(ctx2))
        assert not result2.run_requests
        assert "Already processed" in result2.skip_reason.skip_message


def test_no_materialization_skips(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    _install_fake_anthropic(monkeypatch, {})

    with dg.instance_for_test() as instance:
        sensor_def = _sensor_def(_component())
        context = dg.build_sensor_context(instance=instance)
        result = _unwrap(sensor_def(context))

        assert isinstance(result, dg.SensorResult)
        assert not result.run_requests
        assert "No materialization" in result.skip_reason.skip_message


def test_uses_live_status_not_stale_metadata(monkeypatch):
    """The sensor must re-check the batch's LIVE processing_status via the
    API rather than trusting whatever status string was last written into
    the submit asset's own materialization metadata."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    _install_fake_anthropic(monkeypatch, {"batch_42": "ended"})

    with dg.instance_for_test() as instance:
        @dg.asset(name="submit_asset")
        def _submit(context: dg.AssetExecutionContext):
            context.add_output_metadata({
                "batch_id": dg.MetadataValue.text("batch_42"),
                # Deliberately stale/wrong -- live retrieve() says "ended".
                "processing_status": dg.MetadataValue.text("in_progress"),
            })
            return 1

        dg.materialize([_submit], instance=instance)

        sensor_def = _sensor_def(_component())
        context = dg.build_sensor_context(instance=instance)
        result = _unwrap(sensor_def(context))

        assert len(result.run_requests) == 1
