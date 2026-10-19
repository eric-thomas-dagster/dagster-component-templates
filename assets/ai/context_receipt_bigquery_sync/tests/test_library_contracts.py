"""The sync component against the REAL google-cloud-bigquery classes and method signatures (see real_bq.py): every query() /
load_table_from_json() is bound against ``inspect.signature`` of the real Client method, every QueryJobConfig / LoadJobConfig /
ScalarQueryParameter / ArrayQueryParameter is the real class serialized with ``to_api_repr()`` and checked against the REST API's rules,
and the statement is then executed by the DuckDB fake. Skips when google-cloud-bigquery is not installed. No network, no credentials;
this does not prove behaviour on real BigQuery."""
import pytest

pytest.importorskip("google.cloud.bigquery")
import dagster as dg  # noqa: E402
from .real_bq import ContractClient, REAL_BIGQUERY, install_real_bigquery  # noqa: E402

DS = "proj.ctx"


@pytest.fixture(autouse=True)
def real_library(monkeypatch):
    install_real_bigquery(monkeypatch)


def seed(store, n=3, prefix="r"):
    ids = []
    for i in range(n):
        ids.append(store.record_retrieval(
            context_name="kb", items=[{"item_id": f"{prefix}{i}-a", "source_asset_key": "docs", "source_version": "v1", "rank": 0, "score": 0.9},
                                      {"item_id": f"{prefix}{i}-b", "source_asset_key": "docs", "source_partition": "p1", "rank": 1, "score": 0.5}],
            excluded=[{"item_id": f"{prefix}{i}-x", "reason": "pii", "source_asset_key": "docs"}],
            caller_id="u1", purpose="support", query_hash=f"h{i}", eligible_count=3, dagster_asset_key="docs", trace_id=f"t{i}"))
    return ids


def counts(client):
    return {t: client._backend.con.execute(f"SELECT COUNT(*) FROM ctx.{t}").fetchone()[0]
            for t in ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events")}


@pytest.fixture()
def client():
    c = ContractClient(project="proj")
    c.create_dataset("proj.ctx", exists_ok=True)
    return c


def test_sync_receipts_drives_the_real_client_surface(comp, store, client):
    ids = seed(store, 3)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u1")
    out = comp.sync_receipts(store, client, REAL_BIGQUERY, dataset=DS, batch_size=2)
    assert out["drained"] and out["batches"] >= 2
    assert counts(client) == {"context_receipts": 3, "context_receipt_items": 6, "context_receipt_excluded": 3, "context_receipt_events": 1}
    # second run: nothing new, cursors read back through a real ScalarQueryParameter("n", "STRING", ...)
    again = comp.sync_receipts(store, client, REAL_BIGQUERY, dataset=DS, batch_size=2)
    assert again["cursors_before"] == out["cursors_after"] and sum(again["rows_shipped"].values()) == 0


def test_parameter_and_load_shapes_on_the_wire(comp, store, client):
    seed(store, 2)
    comp.sync_receipts(store, client, REAL_BIGQUERY, dataset=DS)
    params = [p for call in client.calls if call[0] == "query" for p in call[2]]
    kinds = {(p["parameterType"]["type"], p["parameterType"].get("arrayType", {}).get("type")) for p in params}
    assert {("STRING", None), ("ARRAY", "STRING")} <= kinds                # @n / @now, and IN UNNEST(@ids)
    assert all(t in {"STRING", "ARRAY", "INT64", "BOOL"} for t, _ in kinds)
    for _, table, rows, cfg in client.loads():
        assert table.count(".") == 2 and "`" not in table
        assert cfg == {"load": {"writeDisposition": "WRITE_APPEND"}}      # existing tables keep their own schema (REQUIRED columns)
        assert rows and all(isinstance(r, dict) for r in rows)
    # the cursor row is a load of ints, not strings
    state = [c for c in client.loads() if c[1].endswith("context_sync_state")]
    assert state and isinstance(state[0][2][0]["receipt_cursor"], int)


def test_state_cleanup_uses_a_string_timestamp_parameter(comp, store, client):
    seed(store, 1)
    comp.sync_receipts(store, client, REAL_BIGQUERY, dataset=DS)
    deletes = [c for c in client.calls if c[0] == "query" and c[1].lstrip().upper().startswith("DELETE FROM") and "context_sync_state" in c[1]]
    assert deletes and "TIMESTAMP(@now)" in deletes[0][1]
    assert {p["name"]: p["parameterType"]["type"] for p in deletes[0][2]} == {"n": "STRING", "now": "STRING"}


def _gcp_resource(monkeypatch, backend):
    import dagster_gcp

    monkeypatch.setattr(REAL_BIGQUERY, "Client", lambda **kw: backend)
    return dagster_gcp.BigQueryResource(project="proj")


def test_sync_through_the_real_dagster_gcp_get_client(comp, store, monkeypatch):
    # dagster-gcp's BigQueryResource.get_client() is a @contextmanager around bigquery.Client(...): entering it yields the client sync_receipts needs
    pytest.importorskip("dagster_gcp")
    backend = ContractClient(project="proj")
    backend.create_dataset("proj.ctx", exists_ok=True)
    resource = _gcp_resource(monkeypatch, backend)
    seed(store, 2)
    with resource.get_client() as client:
        comp.sync_receipts(store, client, REAL_BIGQUERY, dataset=DS)
    assert counts(backend)["context_receipts"] == 2 and backend.calls


def test_component_with_the_real_dagster_gcp_resource_via_resource_key(comp, store, monkeypatch):
    pytest.importorskip("dagster_gcp")
    backend = ContractClient(project="proj")
    backend.create_dataset("proj.ctx", exists_ok=True)
    resource = _gcp_resource(monkeypatch, backend)
    seed(store, 2)
    defs = comp.ContextReceiptBigQuerySyncComponent(store_resource_key="receipts", bigquery_resource_key="bigquery", dataset=DS).build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"receipts": store, "bigquery": resource}, raise_on_error=False)
    failures = " ".join(str(e.event_specific_data) for e in result.all_events if e.is_failure)
    assert result.success, failures
    assert counts(backend)["context_receipts"] == 2


def test_a_bare_context_manager_is_not_a_client():
    # the harness would catch the original bug: calling .query on what get_client() returns without entering it
    import contextlib

    @contextlib.contextmanager
    def get_client():
        yield ContractClient()

    assert not hasattr(get_client(), "query")
