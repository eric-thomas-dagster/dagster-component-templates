"""Tests for AnthropicLlmBatchComponent -- the single-component bundle that
replaced anthropic_batch_submit / anthropic_batch_status_sensor / anthropic_batch_results.

Only the real (paid) Anthropic API calls are mocked -- a fake client with
the same shape as the real anthropic SDK's `messages.batches` surface.
"""
import os
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset, requires_duckdb


@pytest.fixture
def mod():
    m = load_component_module()
    os.environ["ANTHROPIC_API_KEY"] = "sk-ant-test-fake"
    return m


class FakeAnthropicBatch:
    def __init__(self, id, processing_status):
        self.id = id
        self.processing_status = processing_status


def _fake_result(custom_id, result_type, text=None, error_type=None, error_message=None):
    if result_type == "succeeded":
        block = types.SimpleNamespace(type="text", text=text)
        message = types.SimpleNamespace(content=[block])
        result = types.SimpleNamespace(type="succeeded", message=message)
    elif result_type == "errored":
        err_obj = types.SimpleNamespace(type=error_type, message=error_message)
        err = types.SimpleNamespace(error=err_obj)
        result = types.SimpleNamespace(type="errored", error=err)
    else:
        result = types.SimpleNamespace(type=result_type)
    return types.SimpleNamespace(custom_id=custom_id, result=result)


class FakeBatchesAPI:
    def __init__(self):
        self.create_calls = []
        self.cancel_calls = []
        self.retrieve_calls = []
        self._batches = {}
        self._results_by_id = {}

    def create(self, requests):
        self.create_calls.append(requests)
        bid = f"msgbatch_{len(self.create_calls)}"
        b = FakeAnthropicBatch(bid, "in_progress")
        self._batches[bid] = b
        return b

    def retrieve(self, batch_id):
        self.retrieve_calls.append(batch_id)
        return self._batches[batch_id]

    def cancel(self, batch_id):
        self.cancel_calls.append(batch_id)
        if batch_id in self._batches:
            self._batches[batch_id].processing_status = "canceling"

    def results(self, batch_id):
        return self._results_by_id.get(batch_id, [])


class FakeMessagesAPI:
    def __init__(self):
        self.batches = FakeBatchesAPI()


class FakeClient:
    def __init__(self):
        self.messages = FakeMessagesAPI()


def _install_fake_client(mod, client):
    mod._build_anthropic_client = lambda api_key: client


def _upstream_df():
    return pd.DataFrame({"ticket_id": ["t1", "t2"], "body": ["help me", "also help"]})


def test_fresh_submit_creates_new_batch(mod):
    client = FakeClient()
    _install_fake_client(mod, client)

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    assert {a.key.to_user_string() for a in defs.assets} == {"support_results__submit", "support_results"}
    assert {s.name for s in defs.sensors} == {"support_results__batch_status_sensor"}

    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize(
        [upstream, *defs.assets],
        selection=["support_tickets", "support_results__submit"],
        instance=instance,
    )
    assert result.success
    assert len(client.messages.batches.create_calls) == 1
    assert client.messages.batches.create_calls[0][0]["custom_id"] == "t1"

    md = result.asset_materializations_for_node("support_results__submit")[0].metadata
    assert md["batch_id"].text == "msgbatch_1"
    assert md["processing_status"].text == "in_progress"


def test_retry_reattach_same_prompts_no_duplicate_submit(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()

    r1 = dg.materialize([upstream, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    assert r1.success
    assert len(client.messages.batches.create_calls) == 1

    r2 = dg.materialize([upstream, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    assert r2.success
    assert len(client.messages.batches.create_calls) == 1
    assert client.messages.batches.retrieve_calls[-1] == "msgbatch_1"


def test_prompts_changed_cancels_stale_and_submits_fresh(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    instance = dg.DagsterInstance.ephemeral()

    upstream_v1 = make_upstream_asset("support_tickets", _upstream_df())
    r1 = dg.materialize([upstream_v1, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    assert r1.success

    upstream_v2 = make_upstream_asset("support_tickets", pd.DataFrame({"ticket_id": ["t1"], "body": ["totally different"]}))
    r2 = dg.materialize([upstream_v2, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    assert r2.success
    assert len(client.messages.batches.create_calls) == 2
    assert client.messages.batches.cancel_calls == ["msgbatch_1"]


def test_orphaned_intent_raises_by_default(mod):
    """Anthropic's batch API has no metadata field, so unlike
    OpenaiLlmBatchComponent there's no server-side way to recover a batch
    whose id never made it into this asset's own materialization metadata.
    Simulates: run 1's process crashes right after the paid create() call
    succeeds (an AssetObservation intent marker was already recorded, but
    no materialization ever confirms a batch_id). Run 2, same instance,
    same prompts -- should detect the unresolved intent and raise rather
    than silently submitting (and paying for) a second batch."""
    client = FakeClient()

    class CrashingBatchesAPI(FakeBatchesAPI):
        def create(self, requests):
            self.create_calls.append(requests)
            raise RuntimeError("simulated crash right after the paid call succeeded")

    client.messages.batches = CrashingBatchesAPI()
    _install_fake_client(mod, client)

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()

    r1 = dg.materialize(
        [upstream, *defs.assets], selection=["support_tickets", "support_results__submit"],
        instance=instance, raise_on_error=False,
    )
    assert not r1.success
    assert len(client.messages.batches.create_calls) == 1

    r2 = dg.materialize(
        [upstream, *defs.assets], selection=["support_tickets", "support_results__submit"],
        instance=instance, raise_on_error=False,
    )
    assert not r2.success
    assert len(client.messages.batches.create_calls) == 1  # no duplicate paid create() call
    failure_text = str(r2.all_events)
    assert "recorded intent to submit" in failure_text
    assert "on_orphaned_intent: resubmit" in failure_text


def test_orphaned_intent_resubmit_override_proceeds(mod):
    """Same crash scenario as above, but on_orphaned_intent='resubmit'
    accepts the small risk and proceeds instead of blocking."""
    client = FakeClient()

    class CrashingBatchesAPI(FakeBatchesAPI):
        def create(self, requests):
            self.create_calls.append(requests)
            raise RuntimeError("simulated crash right after the paid call succeeded")

    client.messages.batches = CrashingBatchesAPI()
    _install_fake_client(mod, client)

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
        on_orphaned_intent="resubmit",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()

    r1 = dg.materialize(
        [upstream, *defs.assets], selection=["support_tickets", "support_results__submit"],
        instance=instance, raise_on_error=False,
    )
    assert not r1.success
    assert len(client.messages.batches.create_calls) == 1

    # Swap in a non-crashing batches API for the retry, same instance.
    client.messages.batches = FakeBatchesAPI()
    r2 = dg.materialize(
        [upstream, *defs.assets], selection=["support_tickets", "support_results__submit"],
        instance=instance,
    )
    assert r2.success
    assert len(client.messages.batches.create_calls) == 1  # proceeded despite the unresolved prior intent


def test_custom_id_validation_rejects_bad_characters(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset(
        "support_tickets", pd.DataFrame({"ticket_id": ["bad id with spaces!"], "body": ["x"]})
    )
    result = dg.materialize(
        [upstream, *defs.assets], selection=["support_tickets", "support_results__submit"], raise_on_error=False
    )
    assert not result.success


def test_wait_for_completion_inline_parses_and_returns_full_dataframe(mod):
    client = FakeClient()
    orig_create = client.messages.batches.create
    def create(requests):
        b = orig_create(requests)
        return b
    client.messages.batches.create = create

    orig_retrieve = client.messages.batches.retrieve
    def retrieve(batch_id):
        b = orig_retrieve(batch_id)
        b.processing_status = "ended"
        return b
    client.messages.batches.retrieve = retrieve
    _install_fake_client(mod, client)

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
        wait_for_completion=True,
        poll_interval_seconds=0,
    )
    defs = comp.build_defs(context=None)
    assert {a.key.to_user_string() for a in defs.assets} == {"support_results"}
    assert not defs.sensors

    upstream = make_upstream_asset("support_tickets", _upstream_df())
    # Pre-seed results for the batch id that will be created ("msgbatch_1")
    client.messages.batches._results_by_id["msgbatch_1"] = [
        _fake_result("t1", "succeeded", text="reply one"),
        _fake_result("t2", "succeeded", text="reply two"),
    ]
    result = dg.materialize([upstream, *defs.assets])
    assert result.success
    df = result.output_for_node("support_results")
    assert list(df["raw_output"]) == ["reply one", "reply two"]
    assert list(df["invalid_output"]) == [False, False]


def test_results_asset_covers_all_result_types(mod):
    client = FakeClient()
    b = FakeAnthropicBatch("msgbatch_x", "ended")
    client.messages.batches._batches["msgbatch_x"] = b
    client.messages.batches._results_by_id["msgbatch_x"] = [
        _fake_result("a", "succeeded", text="ok"),
        _fake_result("b", "errored", error_type="invalid_request", error_message="bad input"),
        _fake_result("c", "canceled"),
        _fake_result("d", "expired"),
    ]
    _install_fake_client(mod, client)

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    results_asset = [a for a in defs.assets if a.key.to_user_string() == "support_results"][0]
    result = dg.materialize(
        [results_asset],
        run_config={"ops": {"support_results": {"config": {"batch_id": "msgbatch_x"}}}},
    )
    assert result.success
    df = result.output_for_node("support_results")
    assert list(df["result_type"]) == ["succeeded", "errored", "canceled", "expired"]
    assert df.loc[df["custom_id"] == "a", "raw_output"].iloc[0] == "ok"
    assert "invalid_request" in df.loc[df["custom_id"] == "b", "error"].iloc[0]


def test_results_asset_marks_invalid_json_without_failing(mod):
    client = FakeClient()
    b = FakeAnthropicBatch("msgbatch_y", "ended")
    client.messages.batches._batches["msgbatch_y"] = b
    client.messages.batches._results_by_id["msgbatch_y"] = [
        _fake_result("a", "succeeded", text="not json"),
    ]
    _install_fake_client(mod, client)

    import sys, types as _types
    from pydantic import BaseModel
    fake_mod = _types.ModuleType("tests_fixtures_anthropic")
    schemas_mod = _types.ModuleType("tests_fixtures_anthropic.schemas")
    class Dummy(BaseModel):
        foo: str
    schemas_mod.Dummy = Dummy
    sys.modules["tests_fixtures_anthropic"] = fake_mod
    sys.modules["tests_fixtures_anthropic.schemas"] = schemas_mod

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="typed_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        output_schema="tests_fixtures_anthropic.schemas:Dummy",
    )
    defs = comp.build_defs(context=None)
    results_asset = [a for a in defs.assets if a.key.to_user_string() == "typed_results"][0]
    result = dg.materialize(
        [results_asset],
        run_config={"ops": {"typed_results": {"config": {"batch_id": "msgbatch_y"}}}},
    )
    assert result.success
    df = result.output_for_node("typed_results")
    assert df["invalid_output"].iloc[0] == True  # noqa: E712
    assert df["raw_output"].iloc[0] == "not json"


def test_sensor_fires_on_ended_and_dedupes_via_cursor(mod):
    client = FakeClient()
    orig_retrieve = client.messages.batches.retrieve
    def retrieve(batch_id):
        b = orig_retrieve(batch_id)
        b.processing_status = "ended"
        return b
    client.messages.batches.retrieve = retrieve
    _install_fake_client(mod, client)

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([upstream, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)

    full_defs = dg.Definitions(assets=[upstream, *defs.assets], sensors=defs.sensors)
    ctx = dg.build_sensor_context(instance=instance, definitions=full_defs)
    sensor = defs.sensors[0]

    r1 = sensor(ctx)
    assert len(r1.run_requests) == 1
    rr = r1.run_requests[0]
    assert rr.asset_selection == [dg.AssetKey("support_results")]
    assert rr.run_config == {"ops": {"support_results": {"config": {"batch_id": "msgbatch_1"}}}}

    ctx2 = dg.build_sensor_context(instance=instance, definitions=full_defs, cursor=r1.cursor)
    r2 = sensor(ctx2)
    assert r2.run_requests == []


def test_source_dispatches_to_bigquery_style_get_client(mod):
    client = FakeClient()
    _install_fake_client(mod, client)

    class FakeQueryJob:
        def to_dataframe(self):
            return pd.DataFrame({"ticket_id": ["t1", "t2"], "body": ["help me", "also help"]})

    class FakeBigQueryClient:
        def __init__(self):
            self.queries = []
        def query(self, sql):
            self.queries.append(sql)
            return FakeQueryJob()

    fake_bq_client = FakeBigQueryClient()

    class FakeBigQueryResource:
        def get_client(self):
            return fake_bq_client

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT * FROM tickets"},
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    submit_asset = [a for a in defs.assets if a.key.to_user_string() == "support_results__submit"][0]
    result = dg.materialize([submit_asset], resources={"bq": FakeBigQueryResource()})
    assert result.success
    assert fake_bq_client.queries == ["SELECT * FROM tickets"]
    assert len(client.messages.batches.create_calls) == 1


def test_source_dispatches_to_redshift_style_get_client(mod):
    client = FakeClient()
    _install_fake_client(mod, client)

    class FakeRedshiftClient:
        def __init__(self):
            self.calls = []
        def execute_query(self, sql, fetch_results=False, cursor_factory=None):
            self.calls.append((sql, fetch_results, cursor_factory))
            return [{"ticket_id": "t1", "body": "help me"}, {"ticket_id": "t2", "body": "also help"}]

    fake_rs_client = FakeRedshiftClient()

    class FakeRedshiftResource:
        def get_client(self):
            return fake_rs_client

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        source={"kind": "warehouse_query", "resource_key": "rs", "sql": "SELECT * FROM tickets"},
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    submit_asset = [a for a in defs.assets if a.key.to_user_string() == "support_results__submit"][0]
    result = dg.materialize([submit_asset], resources={"rs": FakeRedshiftResource()})
    assert result.success
    assert fake_rs_client.calls[0][0] == "SELECT * FROM tickets"
    assert fake_rs_client.calls[0][1] is True
    assert len(client.messages.batches.create_calls) == 1


def test_upstream_asset_key_and_source_mutually_exclusive():
    import importlib.util, pathlib
    spec = importlib.util.spec_from_file_location(
        "anthropic_llm_batch_component_validate2",
        pathlib.Path(__file__).resolve().parent.parent / "component.py",
    )
    mod2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod2)

    with pytest.raises(ValueError, match="set exactly one"):
        mod2.AnthropicLlmBatchComponent(asset_name="x", prompt_column="body").build_defs(context=None)

    with pytest.raises(ValueError, match="set exactly one"):
        mod2.AnthropicLlmBatchComponent(
            asset_name="x", prompt_column="body",
            upstream_asset_key="y",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
        ).build_defs(context=None)


@requires_duckdb
def test_source_warehouse_query_against_real_duckdb(mod):
    import duckdb
    import tempfile, os as _os
    from dagster_duckdb import DuckDBResource

    client = FakeClient()
    _install_fake_client(mod, client)

    tmp_dir = tempfile.mkdtemp()
    db_path = _os.path.join(tmp_dir, "tickets.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE tickets AS SELECT * FROM (VALUES ('t1','help me'), ('t2','also help')) AS t(ticket_id, body)")
    conn.close()

    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM tickets"},
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    submit_asset = [a for a in defs.assets if a.key.to_user_string() == "support_results__submit"][0]
    result = dg.materialize(
        [submit_asset],
        resources={"duckdb_resource": DuckDBResource(database=db_path)},
    )
    assert result.success
    assert len(client.messages.batches.create_calls) == 1


def test_sensor_skips_when_not_ended(mod):
    client = FakeClient()  # retrieve() returns default "in_progress"
    _install_fake_client(mod, client)
    comp = mod.AnthropicLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([upstream, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    full_defs = dg.Definitions(assets=[upstream, *defs.assets], sensors=defs.sensors)
    ctx = dg.build_sensor_context(instance=instance, definitions=full_defs)
    result = defs.sensors[0](ctx)
    assert result.run_requests == []
