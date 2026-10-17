"""Partitioned LLM batch runs keep their batch state per partition.

Both components are exercised with a fake client in the shape of the real SDK (only the paid calls are faked). A
partition submits only its own rows, never reattaches to, cancels or hides another partition's batch, and the status
sensor requests one results run per finished partition."""
import importlib.util
import json
import os
import re
import sys
import types
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
D3, D4 = "2026-01-03", "2026-01-04"
PART = dict(partition_type="daily", partition_start="2026-01-01", partition_date_column="ts")


def _frame(rows_d3=2, rows_d4=3, tag=""):
    rows = [(f"a{i}", f"{D3}T0{i}:00:00", f"prompt a{i}{tag}") for i in range(rows_d3)]
    rows += [(f"b{i}", f"{D4}T0{i}:00:00", f"prompt b{i}{tag}") for i in range(rows_d4)]
    return pd.DataFrame(rows, columns=["id", "ts", "body"])


def _upstream(df):
    @dg.asset(name="tickets")
    def tickets():
        return df
    return tickets


class OpenAIFake:
    """The `batches` / `files` surface of the openai SDK."""
    name, folder, cls = "openai", "assets/ai/openai_llm_batch", "OpenaiLlmBatchComponent"
    env, builder, done = "OPENAI_API_KEY", "_build_openai_client", "completed"

    def __init__(self):
        self.files_created, self.create_calls, self.cancel_calls, self.retrieve_calls, self.batches = [], [], [], [], {}
        outer = self

        class Files:
            def create(self, file, purpose):
                outer.files_created.append(file.read())
                return types.SimpleNamespace(id=f"file-{len(outer.files_created)}")

            def content(self, file_id):
                return types.SimpleNamespace(text="")

        class Batches:
            def create(self, **kw):
                outer.create_calls.append(kw)
                b = types.SimpleNamespace(id=f"batch_{len(outer.create_calls)}", status="validating", output_file_id=None,
                                          error_file_id=None, metadata=kw.get("metadata") or {})
                outer.batches[b.id] = b
                return b

            def retrieve(self, bid):
                outer.retrieve_calls.append(bid)
                return outer.batches[bid]

            def cancel(self, bid):
                outer.cancel_calls.append(bid)

            def list(self, limit=20):
                return types.SimpleNamespace(data=list(reversed(list(outer.batches.values())))[:limit])

        self.files, self.batches_api = Files(), Batches()

    @property
    def client(self):
        return types.SimpleNamespace(files=self.files, batches=self.batches_api)

    def rows_in_last_submission(self):
        return len([l for l in self.files_created[-1].decode().splitlines() if l.strip()])

    def finish(self, batch_id):
        self.batches[batch_id].status = self.done


class AnthropicFake:
    """The `messages.batches` surface of the anthropic SDK."""
    name, folder, cls = "anthropic", "assets/ai/anthropic_llm_batch", "AnthropicLlmBatchComponent"
    env, builder, done = "ANTHROPIC_API_KEY", "_build_anthropic_client", "ended"

    def __init__(self):
        self.create_calls, self.cancel_calls, self.retrieve_calls, self.batches = [], [], [], {}
        outer = self

        class Batches:
            def create(self, requests):
                outer.create_calls.append(requests)
                b = types.SimpleNamespace(id=f"msgbatch_{len(outer.create_calls)}", processing_status="in_progress")
                outer.batches[b.id] = b
                return b

            def retrieve(self, bid):
                outer.retrieve_calls.append(bid)
                return outer.batches[bid]

            def cancel(self, bid):
                outer.cancel_calls.append(bid)

            def results(self, bid):
                return []

        self.client = types.SimpleNamespace(messages=types.SimpleNamespace(batches=Batches()))

    def rows_in_last_submission(self):
        return len(self.create_calls[-1])

    def finish(self, batch_id):
        self.batches[batch_id].processing_status = self.done


PROVIDERS = [OpenAIFake, AnthropicFake]


def _setup(provider_cls, monkeypatch, **extra):
    fake = provider_cls()
    folder = ROOT / fake.folder
    spec = importlib.util.spec_from_file_location("lb_" + fake.name, folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    monkeypatch.setenv(fake.env, "k")
    monkeypatch.setattr(mod, fake.builder, lambda api_key: fake.client)
    attrs = dict(asset_name="out", upstream_asset_key="tickets", prompt_column="body", id_column="id", **PART)
    attrs.update(extra)
    defs = getattr(mod, fake.cls)(**attrs).build_defs(context=None)
    return fake, defs


def _submit(defs, instance, pk, df=None, extra_assets=()):
    return dg.materialize([_upstream(df if df is not None else _frame()), *defs.assets], selection=["tickets", "out__submit"],
                          partition_key=pk, instance=instance, raise_on_error=False)


def _batch_id(instance, key, pk):
    recs = instance.fetch_materializations(dg.AssetRecordsFilter(asset_key=dg.AssetKey(key), asset_partitions=[pk]), limit=1).records
    return recs[0].asset_materialization.metadata["batch_id"].text if recs else None


@pytest.mark.parametrize("provider", PROVIDERS, ids=lambda p: p.name)
def test_each_partition_submits_only_its_own_rows_and_records_its_own_batch(provider, monkeypatch):
    fake, defs = _setup(provider, monkeypatch)
    instance = dg.DagsterInstance.ephemeral()
    assert _submit(defs, instance, D3).success and fake.rows_in_last_submission() == 2
    assert _submit(defs, instance, D4).success and fake.rows_in_last_submission() == 3
    assert _batch_id(instance, "out__submit", D3) == "batch_1" or _batch_id(instance, "out__submit", D3) == "msgbatch_1"
    assert _batch_id(instance, "out__submit", D3) != _batch_id(instance, "out__submit", D4)
    assert all(a.partitions_def == defs.assets[0].partitions_def for a in defs.assets)


@pytest.mark.parametrize("provider", PROVIDERS, ids=lambda p: p.name)
def test_a_partition_never_cancels_or_reattaches_to_another_partitions_batch(provider, monkeypatch):
    fake, defs = _setup(provider, monkeypatch)
    instance = dg.DagsterInstance.ephemeral()
    _submit(defs, instance, D3)
    _submit(defs, instance, D4)
    first_d3, first_d4 = _batch_id(instance, "out__submit", D3), _batch_id(instance, "out__submit", D4)
    assert fake.cancel_calls == [] and len(fake.create_calls) == 2   # D4 did not cancel D3's batch
    _submit(defs, instance, D3)                                       # same rows: reattach, no new paid batch
    assert len(fake.create_calls) == 2 and fake.cancel_calls == [] and _batch_id(instance, "out__submit", D3) == first_d3
    _submit(defs, instance, D3, df=_frame(tag=" changed"))            # D3's prompts changed: only D3's old batch is cancelled
    assert fake.cancel_calls == [first_d3] and len(fake.create_calls) == 3
    assert _batch_id(instance, "out__submit", D4) == first_d4


@pytest.mark.parametrize("provider", PROVIDERS, ids=lambda p: p.name)
def test_blocking_mode_returns_only_the_partitions_rows_submitted(provider, monkeypatch):
    fake, defs = _setup(provider, monkeypatch, wait_for_completion=True, poll_interval_seconds=0, timeout_seconds=5)
    orig = fake.batches_api.create if provider is OpenAIFake else fake.client.messages.batches.create

    def finished_create(*a, **k):
        b = orig(*a, **k)
        fake.finish(b.id)
        return b

    if provider is OpenAIFake:
        fake.batches_api.create = finished_create
    else:
        fake.client.messages.batches.create = finished_create
    r = dg.materialize([_upstream(_frame()), *defs.assets], partition_key=D4, raise_on_error=False)
    assert r.success, r
    assert fake.rows_in_last_submission() == 3


@pytest.mark.parametrize("provider", PROVIDERS, ids=lambda p: p.name)
def test_sensor_requests_one_results_run_per_finished_partition_and_remembers_them(provider, monkeypatch):
    fake, defs = _setup(provider, monkeypatch)
    instance = dg.DagsterInstance.ephemeral()
    _submit(defs, instance, D3)
    _submit(defs, instance, D4)
    b3, b4 = _batch_id(instance, "out__submit", D3), _batch_id(instance, "out__submit", D4)
    full = dg.Definitions(assets=[_upstream(_frame()), *defs.assets], sensors=defs.sensors)
    sensor = defs.sensors[0]
    op = dg.AssetKey("out").to_python_identifier()

    def tick(cursor=None):
        return sensor(dg.build_sensor_context(instance=instance, definitions=full, cursor=cursor))

    assert tick().run_requests == []                       # nothing finished yet
    fake.finish(b3)
    r1 = tick()
    assert [(rr.partition_key, rr.asset_selection) for rr in r1.run_requests] == [(D3, [dg.AssetKey("out")])]
    assert r1.run_requests[0].run_config == {"ops": {op: {"config": {"batch_id": b3}}}}
    fake.retrieve_calls.clear()
    fake.finish(b4)
    r2 = tick(r1.cursor)
    assert [rr.partition_key for rr in r2.run_requests] == [D4]
    assert b3 not in fake.retrieve_calls                   # a handled partition costs no API call
    assert tick(r2.cursor).run_requests == []
    assert json.loads(r2.cursor) == {D3: f"{b3}|{provider.done}", D4: f"{b4}|{provider.done}"}


def test_an_unconfirmed_submission_in_one_partition_is_not_hidden_by_another_partitions_newer_marker(monkeypatch):
    """Anthropic has no server-side lookup, so a crash between the paid create() and its recorded batch id leaves only an
    intent marker. A newer marker from a different partition must not mask it, or the retry would pay twice."""
    fake, defs = _setup(AnthropicFake, monkeypatch)

    def crashing_create(requests):
        fake.create_calls.append(requests)
        raise RuntimeError("simulated crash right after the paid call succeeded")

    fake.client.messages.batches.create = crashing_create
    instance = dg.DagsterInstance.ephemeral()
    assert not _submit(defs, instance, D3).success and len(fake.create_calls) == 1
    assert not _submit(defs, instance, D4).success and len(fake.create_calls) == 2   # a newer marker, for a different partition
    r = _submit(defs, instance, D3)
    assert not r.success
    assert len(fake.create_calls) == 2                                               # no second paid call for D3
    assert "recorded intent to submit" in str(r.all_events)


@pytest.mark.parametrize("provider", PROVIDERS, ids=lambda p: p.name)
def test_partitioning_without_a_column_to_slice_by_is_refused(provider, monkeypatch):
    with pytest.raises(ValueError, match="partition_date_column"):
        _setup(provider, monkeypatch, partition_date_column=None)
    with pytest.raises(ValueError, match="multi"):
        _setup(provider, monkeypatch, partition_type="multi")
    with pytest.raises(ValueError, match="partition_static_column"):
        _setup(provider, monkeypatch, partition_type="static", partition_values="a,b", partition_date_column=None)


def test_helper_blocks_are_identical_in_both_components():
    blocks = {re.search(r"# BEGIN batch partition helpers.*?# END batch partition helpers", (ROOT / p / "component.py").read_text(), re.S).group(0)
              for p in ("assets/ai/openai_llm_batch", "assets/ai/anthropic_llm_batch")}
    assert len(blocks) == 1
