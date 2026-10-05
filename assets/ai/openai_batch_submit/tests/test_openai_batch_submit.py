"""Tests for OpenaiBatchSubmitComponent. Mocks `_build_openai_client` -- the
real (paid) OpenAI Batch API is never called."""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset

mod = load_component_module()
OpenaiBatchSubmitComponent = mod.OpenaiBatchSubmitComponent


# --- Fakes mirroring the real OpenAI SDK's shapes -------------------------

class FakeFileObj:
    def __init__(self, id):
        self.id = id


class FakeFileContent:
    def __init__(self, text):
        self.text = text


class FakeBatch:
    def __init__(self, id, status, output_file_id=None, error_file_id=None):
        self.id = id
        self.status = status
        self.output_file_id = output_file_id
        self.error_file_id = error_file_id


class StatefulFakeClient:
    """Stateful fake spanning multiple materializations -- tracks call
    counts so tests can assert resubmission did/didn't happen."""

    def __init__(self):
        self.create_calls = 0
        self.retrieve_calls = []
        self.cancel_calls = []
        self.batches_by_id = {}
        self.batch_metadata_by_id = {}
        self.uploaded_files = []
        self.files = self._Files(self)
        self.batches = self._Batches(self)

    class _Files:
        def __init__(self, outer):
            self.outer = outer

        def create(self, file, purpose):
            assert purpose == "batch"
            self.outer.uploaded_files.append(file.read())
            fid = f"file-{len(self.outer.uploaded_files)}"
            return FakeFileObj(fid)

        def content(self, file_id):
            return FakeFileContent("")

    class _Batches:
        def __init__(self, outer):
            self.outer = outer

        def create(self, input_file_id, endpoint, completion_window, metadata):
            self.outer.create_calls += 1
            bid = f"batch-{self.outer.create_calls}"
            batch = FakeBatch(bid, status="validating")
            self.outer.batches_by_id[bid] = batch
            self.outer.batch_metadata_by_id[bid] = metadata
            return batch

        def retrieve(self, batch_id):
            self.outer.retrieve_calls.append(batch_id)
            return self.outer.batches_by_id[batch_id]

        def cancel(self, batch_id):
            self.outer.cancel_calls.append(batch_id)
            b = self.outer.batches_by_id.get(batch_id)
            if b:
                b.status = "cancelled"
            return b


def _component(**overrides):
    attrs = dict(
        asset_name="support_reply_batch",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    attrs.update(overrides)
    return OpenaiBatchSubmitComponent(**attrs)


def _materialize(component, df, monkeypatch, instance, fake_client):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")
    monkeypatch.setattr(mod, "_build_openai_client", lambda api_key: fake_client)
    defs = component.build_defs(None)
    upstream = make_upstream_asset("support_tickets", df)
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    return full_defs.get_implicit_global_asset_job_def().execute_in_process(instance=instance)


def test_fresh_submit(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    client = StatefulFakeClient()
    df = pd.DataFrame({"ticket_id": ["t1", "t2"], "body": ["hello", "world"]})

    result = _materialize(_component(), df, monkeypatch, instance, client)

    assert result.success
    assert client.create_calls == 1
    out = result.asset_value(dg.AssetKey("support_reply_batch"))
    assert out["batch_id"].iloc[0] == "batch-1"
    assert out["request_count"].iloc[0] == 2

    mats = result.asset_materializations_for_node("support_reply_batch")
    md = mats[-1].metadata
    assert md["batch_id"].text == "batch-1"
    assert md["request_count"].value == 2
    assert md["prompts_hash"].text  # non-empty


def test_retry_reattach_same_hash_does_not_resubmit(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    client = StatefulFakeClient()
    df = pd.DataFrame({"ticket_id": ["t1", "t2"], "body": ["hello", "world"]})

    comp = _component()
    result1 = _materialize(comp, df, monkeypatch, instance, client)
    assert result1.success
    assert client.create_calls == 1

    # Simulate the batch progressing on OpenAI's side between runs.
    client.batches_by_id["batch-1"].status = "in_progress"

    result2 = _materialize(comp, df, monkeypatch, instance, client)
    assert result2.success
    # Must NOT have submitted a second batch -- same prompts_hash means reattach.
    assert client.create_calls == 1
    assert "batch-1" in client.retrieve_calls

    out2 = result2.asset_value(dg.AssetKey("support_reply_batch"))
    assert out2["batch_id"].iloc[0] == "batch-1"
    assert out2["status"].iloc[0] == "in_progress"


def test_prompts_changed_cancels_stale_batch_and_resubmits(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    client = StatefulFakeClient()
    df1 = pd.DataFrame({"ticket_id": ["t1"], "body": ["hello"]})

    comp = _component()
    result1 = _materialize(comp, df1, monkeypatch, instance, client)
    assert result1.success
    assert client.create_calls == 1

    df2 = pd.DataFrame({"ticket_id": ["t1"], "body": ["completely different prompt"]})
    result2 = _materialize(comp, df2, monkeypatch, instance, client)
    assert result2.success

    assert client.create_calls == 2
    assert "batch-1" in client.cancel_calls
    out2 = result2.asset_value(dg.AssetKey("support_reply_batch"))
    assert out2["batch_id"].iloc[0] == "batch-2"


def test_wait_for_completion_parses_output_inline(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    client = StatefulFakeClient()

    # Make the freshly-created batch come back already completed, with an
    # output file ready to download -- avoids sleeping in the poll loop.
    orig_create = client.batches.create

    def create_completed(input_file_id, endpoint, completion_window, metadata):
        batch = orig_create(input_file_id, endpoint, completion_window, metadata)
        batch.status = "completed"
        batch.output_file_id = "outfile-1"
        client.batches_by_id[batch.id] = batch
        return batch

    client.batches.create = create_completed

    output_lines = [
        json.dumps({
            "custom_id": "t1",
            "response": {"status_code": 200, "body": {"choices": [{"message": {"content": "reply one"}}]}},
            "error": None,
        }),
        json.dumps({
            "custom_id": "t2",
            "response": None,
            "error": {"message": "rate limited"},
        }),
    ]
    client.files.content = lambda file_id: FakeFileContent("\n".join(output_lines))

    df = pd.DataFrame({"ticket_id": ["t1", "t2"], "body": ["hello", "world"]})
    result = _materialize(_component(wait_for_completion=True, poll_interval_seconds=0), df, monkeypatch, instance, client)

    assert result.success
    out = result.asset_value(dg.AssetKey("support_reply_batch"))
    assert list(out["custom_id"]) == ["t1", "t2"]
    assert out["raw_output"].iloc[0] == "reply one"
    assert pd.isna(out["raw_output"].iloc[1])
    assert pd.isna(out["error"].iloc[0])
    assert "rate limited" in out["error"].iloc[1]


def test_missing_api_key_raises(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    comp = _component()
    defs = comp.build_defs(None)
    df = pd.DataFrame({"ticket_id": ["t1"], "body": ["hello"]})
    upstream = make_upstream_asset("support_tickets", df)
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    result = full_defs.get_implicit_global_asset_job_def().execute_in_process(
        instance=instance, raise_on_error=False,
    )
    assert not result.success


def test_prompt_column_and_template_mutually_exclusive_raise():
    comp = _component(prompt_template="hi {body}")
    with pytest.raises(ValueError, match="OR prompt_template, not both"):
        comp.build_defs(None)


def test_missing_prompt_fields_raise():
    comp = OpenaiBatchSubmitComponent(asset_name="x", upstream_asset_key="y")
    with pytest.raises(ValueError, match="set prompt_column or prompt_template"):
        comp.build_defs(None)


def test_empty_upstream_skips_without_calling_api(monkeypatch):
    instance = dg.DagsterInstance.ephemeral()
    client = StatefulFakeClient()
    df = pd.DataFrame({"ticket_id": [], "body": []})

    result = _materialize(_component(), df, monkeypatch, instance, client)

    assert result.success
    assert client.create_calls == 0
    out = result.asset_value(dg.AssetKey("support_reply_batch"))
    assert out["status"].iloc[0] == "skipped_empty"
    assert out["request_count"].iloc[0] == 0
