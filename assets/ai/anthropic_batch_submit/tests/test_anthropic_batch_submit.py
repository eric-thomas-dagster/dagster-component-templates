"""Tests for AnthropicBatchSubmitComponent. Mocks `anthropic.Anthropic` --
the real (paid) Anthropic Batches API is never called."""
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset

mod = load_component_module()
AnthropicBatchSubmitComponent = mod.AnthropicBatchSubmitComponent


def _counts(processing=0, succeeded=0, errored=0, canceled=0, expired=0):
    return types.SimpleNamespace(
        processing=processing, succeeded=succeeded, errored=errored,
        canceled=canceled, expired=expired,
    )


class FakeBatch:
    def __init__(self, id, processing_status="ended", request_counts=None):
        self.id = id
        self.processing_status = processing_status
        self.request_counts = request_counts or _counts()


class FakeMessagesBatches:
    """Shared across every `anthropic.Anthropic(...)` instantiation within a
    test so retry-reattach / cancel-and-resubmit checks can see prior calls."""

    def __init__(self):
        self.create_calls = []
        self.retrieve_calls = []
        self.cancel_calls = []
        self._batches = {}
        self._next_id = 1

    def create(self, requests):
        self.create_calls.append(list(requests))
        bid = f"batch_{self._next_id}"
        self._next_id += 1
        batch = FakeBatch(bid, processing_status="ended", request_counts=_counts(succeeded=len(requests)))
        self._batches[bid] = batch
        return batch

    def retrieve(self, batch_id):
        self.retrieve_calls.append(batch_id)
        return self._batches[batch_id]

    def cancel(self, batch_id):
        self.cancel_calls.append(batch_id)
        if batch_id in self._batches:
            self._batches[batch_id].processing_status = "canceling"
        return self._batches.get(batch_id)

    def results(self, batch_id):
        return iter([])


def _install_fake_anthropic(monkeypatch, shared_batches: FakeMessagesBatches):
    import anthropic

    class FakeClient:
        def __init__(self, api_key=None):
            self.api_key = api_key
            self.messages = types.SimpleNamespace(batches=shared_batches)

    monkeypatch.setattr(anthropic, "Anthropic", FakeClient)


def _component(**overrides):
    attrs = dict(
        asset_name="batch_out",
        upstream_asset_key="prompts_in",
        prompt_column="text",
        id_column="row_id",
    )
    attrs.update(overrides)
    return AnthropicBatchSubmitComponent(**attrs)


def _materialize(component, df, instance, monkeypatch, upstream_name="prompts_in"):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
    defs = component.build_defs(None)
    upstream = make_upstream_asset(upstream_name, df)
    return dg.materialize([*defs.assets, upstream], instance=instance)


def test_fresh_submit_path(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        df = pd.DataFrame({"row_id": ["a", "b"], "text": ["hello", "world"]})
        result = _materialize(_component(), df, instance, monkeypatch)

        assert result.success
        assert len(shared.create_calls) == 1
        assert len(shared.retrieve_calls) == 0
        assert len(shared.cancel_calls) == 0

        out = result.asset_value(dg.AssetKey("batch_out"))
        assert out["request_count"].iloc[0] == 2
        assert out["processing_status"].iloc[0] == "ended"
        assert out["batch_id"].iloc[0] == "batch_1"

        mats = result.asset_materializations_for_node("batch_out")
        md = mats[-1].metadata
        assert md["batch_id"].text == "batch_1"
        assert "prompts_hash" in md


def test_retry_reattach_same_prompts_reuses_batch(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        df = pd.DataFrame({"row_id": ["a", "b"], "text": ["hello", "world"]})

        r1 = _materialize(_component(), df, instance, monkeypatch)
        assert r1.success
        assert len(shared.create_calls) == 1
        first_batch_id = shared.create_calls and list(shared._batches.keys())[0]

        # Re-materialize with the SAME content -- must reattach, not resubmit.
        r2 = _materialize(_component(), df, instance, monkeypatch)
        assert r2.success
        assert len(shared.create_calls) == 1, "same prompts must not trigger a second create()"
        assert first_batch_id in shared.retrieve_calls
        assert len(shared.cancel_calls) == 0

        out = r2.asset_value(dg.AssetKey("batch_out"))
        assert out["batch_id"].iloc[0] == first_batch_id


def test_prompts_changed_cancels_stale_and_submits_fresh(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        df1 = pd.DataFrame({"row_id": ["a", "b"], "text": ["hello", "world"]})
        r1 = _materialize(_component(), df1, instance, monkeypatch)
        assert r1.success
        first_batch_id = list(shared._batches.keys())[0]
        assert len(shared.create_calls) == 1

        # Different content -> different prompts_hash.
        df2 = pd.DataFrame({"row_id": ["a", "b"], "text": ["totally", "different"]})
        r2 = _materialize(_component(), df2, instance, monkeypatch)
        assert r2.success
        assert len(shared.create_calls) == 2, "changed prompts must submit a fresh batch"
        assert shared.cancel_calls == [first_batch_id]

        out = r2.asset_value(dg.AssetKey("batch_out"))
        second_batch_id = list(shared._batches.keys())[1]
        assert out["batch_id"].iloc[0] == second_batch_id
        assert second_batch_id != first_batch_id


def test_bad_custom_id_raises_clear_error(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        # id_column value contains characters outside ^[a-zA-Z0-9_-]{1,64}$
        df = pd.DataFrame({"row_id": ["this id has spaces!"], "text": ["hi"]})
        with pytest.raises(Exception, match="custom_id"):
            _materialize(_component(), df, instance, monkeypatch)

        assert len(shared.create_calls) == 0


def test_no_id_column_uses_positional_index(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        df = pd.DataFrame({"text": ["hello", "world"]})
        comp = _component(id_column=None)
        result = _materialize(comp, df, instance, monkeypatch)

        assert result.success
        requests = shared.create_calls[0]
        assert [r["custom_id"] for r in requests] == ["0", "1"]


def test_prompt_template_renders_row_values(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        df = pd.DataFrame({"row_id": ["a"], "body": ["hello"]})
        comp = _component(prompt_column=None, prompt_template="Summarize: {body}")
        result = _materialize(comp, df, instance, monkeypatch)

        assert result.success
        requests = shared.create_calls[0]
        assert requests[0]["params"]["messages"][0]["content"] == "Summarize: hello"


def test_mutually_exclusive_prompt_fields_raise():
    comp = _component(prompt_template="hi {text}")
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_missing_prompt_fields_raise():
    comp = AnthropicBatchSubmitComponent(asset_name="x", upstream_asset_key="y")
    with pytest.raises(ValueError, match="prompt_column or prompt_template"):
        comp.build_defs(None)


def test_wait_for_completion_returns_parsed_results(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    class FakeTextBlock:
        type = "text"

        def __init__(self, text):
            self.text = text

    class FakeMessage:
        def __init__(self, text):
            self.content = [FakeTextBlock(text)]

    class FakeSucceededResult:
        type = "succeeded"

        def __init__(self, text):
            self.message = FakeMessage(text)

    class FakeResultEntry:
        def __init__(self, custom_id, text):
            self.custom_id = custom_id
            self.result = FakeSucceededResult(text)

    def fake_results(batch_id):
        return iter([FakeResultEntry("0", "ok response")])

    shared.results = fake_results

    with dg.instance_for_test() as instance:
        df = pd.DataFrame({"text": ["hello"]})
        comp = _component(id_column=None, wait_for_completion=True, poll_interval_seconds=0)
        result = _materialize(comp, df, instance, monkeypatch)

        assert result.success
        out = result.asset_value(dg.AssetKey("batch_out"))
        assert list(out["custom_id"]) == ["0"]
        assert list(out["raw_output"]) == ["ok response"]
        assert list(out["result_type"]) == ["succeeded"]


def test_partition_bridge_concats_dict_of_frames(monkeypatch):
    shared = FakeMessagesBatches()
    _install_fake_anthropic(monkeypatch, shared)

    with dg.instance_for_test() as instance:
        comp = _component(id_column=None)
        defs = comp.build_defs(None)
        asset_fn = list(defs.assets)[0]

        upstream_dict = {
            "p1": pd.DataFrame({"text": ["a"]}),
            "p2": pd.DataFrame({"text": ["b"]}),
        }
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-123")
        ctx = dg.build_asset_context(instance=instance)
        out_df = asset_fn(ctx, upstream_dict)
        assert out_df["request_count"].iloc[0] == 2
