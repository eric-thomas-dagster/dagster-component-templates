"""Tests for OpenaiLlmBatchComponent -- the single-component bundle that
replaced openai_batch_submit / openai_batch_status_sensor / openai_batch_results.

Only the real (paid) OpenAI API calls are mocked -- a fake client with the
same shape as the real openai SDK's `batches`/`files` surface.
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
    os.environ["OPENAI_API_KEY"] = "sk-test-fake"
    return m


class FakeBatch:
    def __init__(self, id, status, output_file_id=None, error_file_id=None):
        self.id = id
        self.status = status
        self.output_file_id = output_file_id
        self.error_file_id = error_file_id


class FakeFilesAPI:
    def __init__(self):
        self.created = []
        self._content_by_id = {}

    def create(self, file, purpose):
        assert purpose == "batch"
        self.created.append(file.read())
        return types.SimpleNamespace(id=f"file-{len(self.created)}")

    def content(self, file_id):
        return types.SimpleNamespace(text=self._content_by_id.get(file_id, ""))


class FakeBatchesAPI:
    def __init__(self):
        self.create_calls = []
        self.cancel_calls = []
        self.retrieve_calls = []
        self._batches = {}

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        bid = f"batch_{len(self.create_calls)}"
        b = FakeBatch(bid, "validating")
        self._batches[bid] = b
        return b

    def retrieve(self, batch_id):
        self.retrieve_calls.append(batch_id)
        return self._batches[batch_id]

    def cancel(self, batch_id):
        self.cancel_calls.append(batch_id)
        if batch_id in self._batches:
            self._batches[batch_id].status = "cancelled"


class FakeClient:
    def __init__(self):
        self.files = FakeFilesAPI()
        self.batches = FakeBatchesAPI()


def _install_fake_client(mod, client):
    mod._build_openai_client = lambda api_key: client


def _upstream_df():
    return pd.DataFrame({"ticket_id": ["t1", "t2"], "body": ["help me", "also help"]})


def test_fresh_submit_creates_new_batch(mod):
    client = FakeClient()
    _install_fake_client(mod, client)

    comp = mod.OpenaiLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        id_column="ticket_id",
    )
    defs = comp.build_defs(context=None)
    asset_names = {a.key.to_user_string() for a in defs.assets}
    assert asset_names == {"support_results__submit", "support_results"}
    assert {s.name for s in defs.sensors} == {"support_results__batch_status_sensor"}

    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize(
        [upstream, *defs.assets],
        selection=["support_tickets", "support_results__submit"],
        instance=instance,
    )
    assert result.success
    assert len(client.batches.create_calls) == 1
    assert len(client.files.created) == 1

    md = result.asset_materializations_for_node("support_results__submit")[0].metadata
    assert md["batch_id"].text == "batch_1"
    assert md["status"].text == "validating"
    assert md["request_count"].value == 2


def test_retry_reattach_same_prompts_no_duplicate_submit(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.OpenaiLlmBatchComponent(
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
    assert len(client.batches.create_calls) == 1

    r2 = dg.materialize([upstream, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    assert r2.success
    assert len(client.batches.create_calls) == 1  # unchanged -- reattached, not resubmitted
    assert client.batches.retrieve_calls[-1] == "batch_1"


def test_prompts_changed_cancels_stale_and_submits_fresh(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.OpenaiLlmBatchComponent(
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

    upstream_v2 = make_upstream_asset("support_tickets", pd.DataFrame({"ticket_id": ["t1"], "body": ["totally different text"]}))
    r2 = dg.materialize([upstream_v2, *defs.assets], selection=["support_tickets", "support_results__submit"], instance=instance)
    assert r2.success
    assert len(client.batches.create_calls) == 2
    assert client.batches.cancel_calls == ["batch_1"]


def test_wait_for_completion_inline_parses_and_returns_full_dataframe(mod):
    client = FakeClient()
    client.files._content_by_id["file-out"] = (
        '{"custom_id":"t1","response":{"status_code":200,"body":{"choices":[{"message":{"content":"reply one"}}]}},"error":null}\n'
        '{"custom_id":"t2","response":{"status_code":200,"body":{"choices":[{"message":{"content":"reply two"}}]}},"error":null}\n'
    )

    # Make retrieve() report "completed" immediately with an output file.
    orig_retrieve = client.batches.retrieve
    def retrieve(batch_id):
        b = orig_retrieve(batch_id)
        b.status = "completed"
        b.output_file_id = "file-out"
        return b
    client.batches.retrieve = retrieve
    _install_fake_client(mod, client)

    comp = mod.OpenaiLlmBatchComponent(
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
    result = dg.materialize([upstream, *defs.assets])
    assert result.success
    df = result.output_for_node("support_results")
    assert list(df["raw_output"]) == ["reply one", "reply two"]
    assert list(df["invalid_output"]) == [False, False]


def test_results_asset_marks_invalid_json_without_failing(mod):
    client = FakeClient()
    client.files._content_by_id["file-out"] = (
        '{"custom_id":"t1","response":{"status_code":200,"body":{"choices":[{"message":{"content":"not json"}}]}},"error":null}\n'
    )
    b = FakeBatch("batch_x", "completed", output_file_id="file-out")
    client.batches._batches["batch_x"] = b
    _install_fake_client(mod, client)

    comp = mod.OpenaiLlmBatchComponent(
        asset_name="typed_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
        output_schema="tests_fixtures.schemas:Dummy",
    )
    # output_schema resolution happens lazily at asset-run time; provide a
    # real importable dummy module/class via sys.modules injection.
    import sys, types as _types
    fake_mod = _types.ModuleType("tests_fixtures")
    schemas_mod = _types.ModuleType("tests_fixtures.schemas")
    from pydantic import BaseModel
    class Dummy(BaseModel):
        foo: str
    schemas_mod.Dummy = Dummy
    sys.modules["tests_fixtures"] = fake_mod
    sys.modules["tests_fixtures.schemas"] = schemas_mod

    defs = comp.build_defs(context=None)
    results_asset = [a for a in defs.assets if a.key.to_user_string() == "typed_results"][0]
    result = dg.materialize(
        [results_asset],
        run_config={"ops": {"typed_results": {"config": {"batch_id": "batch_x"}}}},
    )
    assert result.success
    df = result.output_for_node("typed_results")
    assert df["invalid_output"].iloc[0] == True  # noqa: E712 -- "not json" fails Dummy validation
    assert df["raw_output"].iloc[0] == "not json"


def test_results_asset_requires_batch_id(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.OpenaiLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    results_asset = [a for a in defs.assets if a.key.to_user_string() == "support_results"][0]
    result = dg.materialize([results_asset], raise_on_error=False)
    assert not result.success


def test_sensor_fires_on_terminal_status_and_dedupes_via_cursor(mod):
    client = FakeClient()
    orig_retrieve = client.batches.retrieve
    def retrieve(batch_id):
        b = orig_retrieve(batch_id)
        b.status = "completed"
        return b
    client.batches.retrieve = retrieve
    _install_fake_client(mod, client)

    comp = mod.OpenaiLlmBatchComponent(
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
    assert rr.run_config == {"ops": {"support_results": {"config": {"batch_id": "batch_1"}}}}

    ctx2 = dg.build_sensor_context(instance=instance, definitions=full_defs, cursor=r1.cursor)
    r2 = sensor(ctx2)
    assert r2.run_requests == []  # same fingerprint -- deduped


def test_sensor_skips_when_not_terminal(mod):
    client = FakeClient()  # retrieve() returns default "validating" status
    _install_fake_client(mod, client)

    comp = mod.OpenaiLlmBatchComponent(
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


def test_sensor_skips_with_no_prior_materialization(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.OpenaiLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", _upstream_df())
    instance = dg.DagsterInstance.ephemeral()
    full_defs = dg.Definitions(assets=[upstream, *defs.assets], sensors=defs.sensors)
    ctx = dg.build_sensor_context(instance=instance, definitions=full_defs)
    result = defs.sensors[0](ctx)
    assert result.run_requests == []


def test_empty_upstream_short_circuits(mod):
    client = FakeClient()
    _install_fake_client(mod, client)
    comp = mod.OpenaiLlmBatchComponent(
        asset_name="support_results",
        upstream_asset_key="support_tickets",
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    upstream = make_upstream_asset("support_tickets", pd.DataFrame({"body": []}))
    result = dg.materialize([upstream, *defs.assets], selection=["support_tickets", "support_results__submit"])
    assert result.success
    assert len(client.batches.create_calls) == 0


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

    comp = mod.OpenaiLlmBatchComponent(
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
    assert len(client.batches.create_calls) == 1


def test_source_dispatches_to_redshift_style_get_client(mod):
    client = FakeClient()
    _install_fake_client(mod, client)

    class FakeRedshiftClient:
        def __init__(self):
            self.calls = []
        def execute_query(self, sql, fetch_results=False, cursor_factory=None):
            self.calls.append((sql, fetch_results, cursor_factory))
            # Simulate RealDictCursor-style dict rows (what cursor_factory=RealDictCursor yields)
            return [{"ticket_id": "t1", "body": "help me"}, {"ticket_id": "t2", "body": "also help"}]

    fake_rs_client = FakeRedshiftClient()

    class FakeRedshiftResource:
        def get_client(self):
            return fake_rs_client

    comp = mod.OpenaiLlmBatchComponent(
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
    assert fake_rs_client.calls[0][1] is True  # fetch_results=True
    assert len(client.batches.create_calls) == 1


def test_source_unknown_client_shape_raises_clear_error(mod):
    client = FakeClient()
    _install_fake_client(mod, client)

    class FakeUnknownClient:
        pass

    class FakeUnknownResource:
        def get_client(self):
            return FakeUnknownClient()

    comp = mod.OpenaiLlmBatchComponent(
        asset_name="support_results",
        source={"kind": "warehouse_query", "resource_key": "unk", "sql": "SELECT 1"},
        prompt_column="body",
    )
    defs = comp.build_defs(context=None)
    submit_asset = [a for a in defs.assets if a.key.to_user_string() == "support_results__submit"][0]
    result = dg.materialize([submit_asset], resources={"unk": FakeUnknownResource()}, raise_on_error=False)
    assert not result.success


def test_upstream_asset_key_and_source_mutually_exclusive():
    import importlib.util, pathlib
    spec = importlib.util.spec_from_file_location(
        "openai_llm_batch_component_validate2",
        pathlib.Path(__file__).resolve().parent.parent / "component.py",
    )
    mod2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod2)

    with pytest.raises(ValueError, match="set exactly one"):
        mod2.OpenaiLlmBatchComponent(
            asset_name="x", prompt_column="body",
        ).build_defs(context=None)  # neither set

    with pytest.raises(ValueError, match="set exactly one"):
        mod2.OpenaiLlmBatchComponent(
            asset_name="x", prompt_column="body",
            upstream_asset_key="y",
            source={"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"},
        ).build_defs(context=None)  # both set


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

    comp = mod.OpenaiLlmBatchComponent(
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
    assert len(client.batches.create_calls) == 1
    md = result.asset_materializations_for_node("support_results__submit")[0].metadata
    assert md["request_count"].value == 2


def test_mutually_exclusive_prompt_fields_raise():
    import importlib.util, pathlib
    spec = importlib.util.spec_from_file_location(
        "openai_llm_batch_component_validate",
        pathlib.Path(__file__).resolve().parent.parent / "component.py",
    )
    mod2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod2)

    comp = mod2.OpenaiLlmBatchComponent(
        asset_name="x", upstream_asset_key="y", prompt_column="body", prompt_template="{body}",
    )
    with pytest.raises(ValueError):
        comp.build_defs(context=None)

    comp2 = mod2.OpenaiLlmBatchComponent(asset_name="x", upstream_asset_key="y")
    with pytest.raises(ValueError):
        comp2.build_defs(context=None)
