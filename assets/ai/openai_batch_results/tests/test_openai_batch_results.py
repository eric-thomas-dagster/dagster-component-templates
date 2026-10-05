"""Tests for OpenaiBatchResultsComponent. Mocks `_build_openai_client` -- the
real (paid) OpenAI Batch API is never called."""
import json

import dagster as dg
import pandas as pd
import pytest
from pydantic import BaseModel

from .conftest import load_component_module

mod = load_component_module()
OpenaiBatchResultsComponent = mod.OpenaiBatchResultsComponent


class FakeFileContent:
    def __init__(self, text):
        self.text = text


class FakeBatch:
    def __init__(self, id, status, output_file_id=None, error_file_id=None):
        self.id = id
        self.status = status
        self.output_file_id = output_file_id
        self.error_file_id = error_file_id


class FakeClient:
    def __init__(self, batch, file_contents):
        self._batch = batch
        self._file_contents = file_contents
        self.retrieved_ids = []
        self.files = self._Files(self)
        self.batches = self._Batches(self)

    class _Files:
        def __init__(self, outer):
            self.outer = outer

        def content(self, file_id):
            return FakeFileContent(self.outer._file_contents.get(file_id, ""))

    class _Batches:
        def __init__(self, outer):
            self.outer = outer

        def retrieve(self, batch_id):
            self.outer.retrieved_ids.append(batch_id)
            return self.outer._batch


def _component(**overrides):
    attrs = dict(asset_name="support_reply_results", batch_id="batch-1")
    attrs.update(overrides)
    return OpenaiBatchResultsComponent(**attrs)


def _materialize(component, client, monkeypatch, run_config=None):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")
    monkeypatch.setattr(mod, "_build_openai_client", lambda api_key: client)
    defs = component.build_defs(None)
    return defs.get_implicit_global_asset_job_def().execute_in_process(
        run_config=run_config, raise_on_error=False,
    )


def test_happy_path_parse(monkeypatch):
    batch = FakeBatch("batch-1", status="completed", output_file_id="outfile-1")
    output_lines = [
        json.dumps({
            "custom_id": "t1",
            "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "reply one"}}]}},
            "error": None,
        }),
        json.dumps({
            "custom_id": "t2",
            "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "reply two"}}]}},
            "error": None,
        }),
    ]
    client = FakeClient(batch, {"outfile-1": "\n".join(output_lines)})

    result = _materialize(_component(), client, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("support_reply_results"))
    assert list(out["custom_id"]) == ["t1", "t2"]
    assert list(out["raw_output"]) == ["reply one", "reply two"]
    assert out["error"].isna().all()
    assert (~out["invalid_output"]).all()

    mats = result.asset_materializations_for_node("support_reply_results")
    md = mats[-1].metadata
    assert md["row_count"].value == 2
    assert md["succeeded_count"].value == 2
    assert md["errored_count"].value == 0
    assert md["invalid_output_count"].value == 0


def test_per_row_api_error_captured(monkeypatch):
    batch = FakeBatch("batch-1", status="completed", output_file_id="outfile-1")
    output_lines = [
        json.dumps({
            "custom_id": "t1",
            "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "ok"}}]}},
            "error": None,
        }),
        json.dumps({
            "custom_id": "t2",
            "response": {"status_code": 429},
            "error": {"message": "rate limited"},
        }),
    ]
    client = FakeClient(batch, {"outfile-1": "\n".join(output_lines)})

    result = _materialize(_component(), client, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("support_reply_results"))
    assert pd.isna(out["raw_output"].iloc[1])
    assert "rate limited" in out["error"].iloc[1]

    mats = result.asset_materializations_for_node("support_reply_results")
    md = mats[-1].metadata
    assert md["errored_count"].value == 1
    assert md["succeeded_count"].value == 1


def test_error_file_rows_merged_in(monkeypatch):
    batch = FakeBatch("batch-1", status="completed", output_file_id="outfile-1", error_file_id="errfile-1")
    output_lines = [json.dumps({
        "custom_id": "t1",
        "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "ok"}}]}},
        "error": None,
    })]
    error_lines = [json.dumps({
        "custom_id": "t2",
        "error": {"message": "invalid request: malformed line"},
    })]
    client = FakeClient(batch, {
        "outfile-1": "\n".join(output_lines),
        "errfile-1": "\n".join(error_lines),
    })

    result = _materialize(_component(), client, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("support_reply_results"))
    assert len(out) == 2
    assert set(out["custom_id"]) == {"t1", "t2"}
    err_row = out[out["custom_id"] == "t2"].iloc[0]
    assert pd.isna(err_row["raw_output"])
    assert "malformed line" in err_row["error"]


class SupportReply(BaseModel):
    sentiment: str
    confidence: float


def test_typed_output_schema_success_and_invalid_rows(monkeypatch):
    batch = FakeBatch("batch-1", status="completed", output_file_id="outfile-1")
    good_payload = json.dumps({"sentiment": "positive", "confidence": 0.9})
    output_lines = [
        json.dumps({
            "custom_id": "t1",
            "response": {"status_code": 200, "body": {"choices": [{"message": {"content": good_payload}}]}},
            "error": None,
        }),
        json.dumps({
            "custom_id": "t2",
            "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "not json at all"}}]}},
            "error": None,
        }),
    ]
    client = FakeClient(batch, {"outfile-1": "\n".join(output_lines)})

    comp = _component(output_schema=f"{__name__}:SupportReply")
    result = _materialize(comp, client, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("support_reply_results"))

    good_row = out[out["custom_id"] == "t1"].iloc[0]
    assert good_row["invalid_output"] == False  # noqa: E712
    assert good_row["sentiment"] == "positive"
    assert good_row["confidence"] == 0.9

    bad_row = out[out["custom_id"] == "t2"].iloc[0]
    assert bad_row["invalid_output"] == True  # noqa: E712
    assert bad_row["raw_output"] == "not json at all"
    assert pd.isna(bad_row["sentiment"])

    mats = result.asset_materializations_for_node("support_reply_results")
    md = mats[-1].metadata
    assert md["invalid_output_count"].value == 1


def test_manual_run_before_completion_raises(monkeypatch):
    batch = FakeBatch("batch-1", status="in_progress")
    client = FakeClient(batch, {})

    result = _materialize(_component(), client, monkeypatch)
    assert not result.success


def test_dynamic_batch_id_via_run_config_takes_precedence(monkeypatch):
    batch = FakeBatch("batch-dynamic", status="completed", output_file_id="outfile-1")
    output_lines = [json.dumps({
        "custom_id": "t1",
        "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "ok"}}]}},
        "error": None,
    })]
    client = FakeClient(batch, {"outfile-1": "\n".join(output_lines)})

    # Static batch_id is intentionally wrong/absent -- the dynamic run_config
    # value must be what's actually used.
    comp = _component(batch_id=None)
    op_name = dg.AssetKey.from_user_string("support_reply_results").to_python_identifier()
    run_config = {"ops": {op_name: {"config": {"batch_id": "batch-dynamic"}}}}

    result = _materialize(comp, client, monkeypatch, run_config=run_config)

    assert result.success
    assert client.retrieved_ids == ["batch-dynamic"]


def test_missing_batch_id_raises(monkeypatch):
    comp = _component(batch_id=None)
    client = FakeClient(FakeBatch("unused", status="completed"), {})
    result = _materialize(comp, client, monkeypatch)
    assert not result.success
