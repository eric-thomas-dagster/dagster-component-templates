"""Tests for AnthropicBatchResultsComponent. Mocks `anthropic.Anthropic` --
the real (paid) Anthropic Batches API is never called."""
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
AnthropicBatchResultsComponent = mod.AnthropicBatchResultsComponent


# ── Fake Anthropic result shapes (mirror the real SDK's discriminated union) ──

class FakeTextBlock:
    type = "text"

    def __init__(self, text):
        self.text = text


class FakeMessage:
    def __init__(self, texts):
        self.content = [FakeTextBlock(t) for t in texts]


class FakeSucceededResult:
    type = "succeeded"

    def __init__(self, texts):
        self.message = FakeMessage(texts)


class FakeErrorObject:
    def __init__(self, type_, message):
        self.type = type_
        self.message = message


class FakeErrorResponse:
    def __init__(self, type_, message):
        self.error = FakeErrorObject(type_, message)


class FakeErroredResult:
    type = "errored"

    def __init__(self, type_, message):
        self.error = FakeErrorResponse(type_, message)


class FakeCanceledResult:
    type = "canceled"


class FakeExpiredResult:
    type = "expired"


class FakeResultEntry:
    def __init__(self, custom_id, result):
        self.custom_id = custom_id
        self.result = result


class FakeBatch:
    def __init__(self, id, processing_status):
        self.id = id
        self.processing_status = processing_status


def _install_fake_anthropic(monkeypatch, batch, results):
    import anthropic

    class FakeMessagesBatches:
        def retrieve(self, batch_id):
            return batch

        def results(self, batch_id):
            return iter(results)

    class FakeClient:
        def __init__(self, api_key=None):
            self.messages = types.SimpleNamespace(batches=FakeMessagesBatches())

    monkeypatch.setattr(anthropic, "Anthropic", FakeClient)


def _component(**overrides):
    attrs = dict(asset_name="batch_results")
    attrs.update(overrides)
    return AnthropicBatchResultsComponent(**attrs)


def _materialize(component, monkeypatch, run_config=None):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    defs = component.build_defs(None)
    return dg.materialize(list(defs.assets), run_config=run_config)


def test_happy_path_mixed_result_types(monkeypatch):
    batch = FakeBatch("batch_1", "ended")
    results = [
        FakeResultEntry("0", FakeSucceededResult(["the answer"])),
        FakeResultEntry("1", FakeErroredResult("invalid_request_error", "bad prompt")),
        FakeResultEntry("2", FakeCanceledResult()),
        FakeResultEntry("3", FakeExpiredResult()),
    ]
    _install_fake_anthropic(monkeypatch, batch, results)

    comp = _component(batch_id="batch_1")
    result = _materialize(comp, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("batch_results"))
    assert list(out["custom_id"]) == ["0", "1", "2", "3"]
    assert list(out["result_type"]) == ["succeeded", "errored", "canceled", "expired"]
    assert out["raw_output"].iloc[0] == "the answer"
    assert pd.isna(out["raw_output"].iloc[1])
    assert "bad prompt" in out["error"].iloc[1]
    assert out["error"].iloc[2] == "request canceled"
    assert out["error"].iloc[3] == "request expired"
    assert not out["invalid_output"].any()

    mats = result.asset_materializations_for_node("batch_results")
    md = mats[-1].metadata
    assert md["row_count"].value == 4
    assert md["succeeded_count"].value == 1
    assert md["errored_count"].value == 1
    assert md["invalid_output_count"].value == 0


def test_multiple_text_blocks_joined(monkeypatch):
    batch = FakeBatch("batch_1", "ended")
    results = [FakeResultEntry("0", FakeSucceededResult(["part one", "part two"]))]
    _install_fake_anthropic(monkeypatch, batch, results)

    comp = _component(batch_id="batch_1")
    result = _materialize(comp, monkeypatch)

    out = result.asset_value(dg.AssetKey("batch_results"))
    assert out["raw_output"].iloc[0] == "part one\npart two"


def test_not_ended_raises_clear_error(monkeypatch):
    batch = FakeBatch("batch_1", "in_progress")
    _install_fake_anthropic(monkeypatch, batch, [])

    comp = _component(batch_id="batch_1")
    with pytest.raises(Exception, match="not 'ended'"):
        _materialize(comp, monkeypatch)


def test_no_batch_id_raises_clear_error(monkeypatch):
    _install_fake_anthropic(monkeypatch, FakeBatch("unused", "ended"), [])
    comp = _component(batch_id=None)
    with pytest.raises(Exception, match="no batch_id supplied"):
        _materialize(comp, monkeypatch)


def test_dynamic_batch_id_from_run_config_overrides_static(monkeypatch):
    batch = FakeBatch("batch_dynamic", "ended")
    results = [FakeResultEntry("0", FakeSucceededResult(["hi"]))]
    _install_fake_anthropic(monkeypatch, batch, results)

    comp = _component(batch_id="static_default")
    result = _materialize(
        comp, monkeypatch,
        run_config={"ops": {"batch_results": {"config": {"batch_id": "batch_dynamic"}}}},
    )
    assert result.success
    out = result.asset_value(dg.AssetKey("batch_results"))
    assert list(out["custom_id"]) == ["0"]


# ── output_schema validation ──────────────────────────────────────────────

class _FakeSchemaModule:
    """Stands in for an importable module exposing a Pydantic model, so we
    don't need a real file on sys.path."""


def _install_output_schema_module(monkeypatch):
    import sys
    from pydantic import BaseModel

    class TicketClassification(BaseModel):
        sentiment: str
        confidence: float

    mod_obj = types.ModuleType("fake_schema_module_for_anthropic_batch_results")
    mod_obj.TicketClassification = TicketClassification
    monkeypatch.setitem(sys.modules, "fake_schema_module_for_anthropic_batch_results", mod_obj)
    return "fake_schema_module_for_anthropic_batch_results:TicketClassification"


def test_output_schema_valid_json_populates_typed_columns(monkeypatch):
    schema_ref = _install_output_schema_module(monkeypatch)
    batch = FakeBatch("batch_1", "ended")
    results = [
        FakeResultEntry("0", FakeSucceededResult(['{"sentiment": "positive", "confidence": 0.9}'])),
    ]
    _install_fake_anthropic(monkeypatch, batch, results)

    comp = _component(batch_id="batch_1", output_schema=schema_ref)
    result = _materialize(comp, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("batch_results"))
    assert out["invalid_output"].iloc[0] == False  # noqa: E712
    assert out["sentiment"].iloc[0] == "positive"
    assert out["confidence"].iloc[0] == 0.9


def test_output_schema_invalid_json_marks_invalid_without_failing_asset(monkeypatch):
    schema_ref = _install_output_schema_module(monkeypatch)
    batch = FakeBatch("batch_1", "ended")
    results = [
        FakeResultEntry("0", FakeSucceededResult(["not json at all"])),
        FakeResultEntry("1", FakeSucceededResult(['{"sentiment": "negative", "confidence": 0.4}'])),
    ]
    _install_fake_anthropic(monkeypatch, batch, results)

    comp = _component(batch_id="batch_1", output_schema=schema_ref)
    result = _materialize(comp, monkeypatch)

    assert result.success  # a bad row must not fail the whole asset
    out = result.asset_value(dg.AssetKey("batch_results"))
    assert out["invalid_output"].iloc[0] == True  # noqa: E712
    assert out["raw_output"].iloc[0] == "not json at all"
    assert pd.isna(out["sentiment"].iloc[0])
    assert out["invalid_output"].iloc[1] == False  # noqa: E712
    assert out["sentiment"].iloc[1] == "negative"

    mats = result.asset_materializations_for_node("batch_results")
    md = mats[-1].metadata
    assert md["invalid_output_count"].value == 1


def test_bad_output_schema_dotted_path_raises():
    comp = _component(output_schema="not_a_valid_dotted_path")
    with pytest.raises(ValueError, match="module.path:ClassName"):
        comp.build_defs(None)
