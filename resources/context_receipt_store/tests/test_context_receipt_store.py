import importlib.util
import threading
from pathlib import Path

import dagster as dg
import pytest


def _load_module():
    component_path = Path(__file__).parents[1] / "component.py"
    spec = importlib.util.spec_from_file_location("context_receipt_store_component", component_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def store(tmp_path):
    module = _load_module()
    return module.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def test_receipt_round_trip_and_append_only_output_link(store):
    receipt_id = store.record_retrieval(
        context_name="support_context",
        context_version="v17",
        policy_version="policy-v3",
        caller_id="support-agent",
        purpose="answer_question",
        query_hash="sha256:abc",
        eligible_count=4,
        excluded_count=1,
        dagster_asset_key="support_tickets",
        items=[
            {
                "item_id": "ticket-12:chunk-0",
                "source_asset_key": "support_tickets",
                "source_version": "v9",
                "rank": 0,
                "score": 0.92,
            }
        ],
    )
    before = store.get_receipt(receipt_id)
    store.attach_output(
        receipt_id,
        output_id="answer-1",
        model="test-model",
        model_version="2026-10",
        output_hash="sha256:def",
        prefect_run_id="prefect-run-1",
    )
    store.add_evidence(
        receipt_id,
        level="cited",
        details={"claim_ids": ["claim-1"], "item_ids": ["ticket-12:chunk-0"]},
    )
    after = store.get_receipt(receipt_id)

    assert before is not None
    assert after is not None
    assert before["created_at"] == after["created_at"]
    assert before["context_version"] == after["context_version"] == "v17"
    assert after["items"][0]["item_id"] == "ticket-12:chunk-0"
    assert [event["event_type"] for event in after["events"]] == [
        "output_attached",
        "evidence",
    ]
    assert after["events"][1]["payload"]["level"] == "cited"
    assert store.find_by_item("ticket-12:chunk-0") == [receipt_id]
    assert store.find_by_source_asset("support_tickets") == [receipt_id]


def test_store_reopens_and_does_not_accept_raw_context_fields(store):
    receipt_id = store.record_retrieval(
        context_name="account_context",
        items=[{"item_id": "account-1:chunk-0"}],
    )
    reopened = type(store)(store.database_path)

    assert reopened.get_receipt(receipt_id)["context_name"] == "account_context"
    with pytest.raises(ValueError, match="unsupported retrieved item field"):
        reopened.record_retrieval(
            context_name="account_context",
            items=[{"item_id": "account-1:chunk-0", "text": "raw customer data"}],
        )


def test_unknown_receipt_and_invalid_evidence_fail_explicitly(store):
    with pytest.raises(KeyError, match="unknown context receipt"):
        store.attach_output(
            "missing",
            output_id="answer-1",
            model="test-model",
            output_hash="sha256:abc",
        )

    receipt_id = store.record_retrieval(context_name="context", items=[])
    with pytest.raises(ValueError, match="level must be one of"):
        store.add_evidence(receipt_id, level="used", details={})


def test_component_registers_store_resource_for_dagster(tmp_path):
    module = _load_module()
    definitions = module.ContextReceiptStoreComponent(
        database_path=str(tmp_path / "component.sqlite3")
    ).build_defs(context=None)

    @dg.asset(required_resource_keys={"context_receipts"})
    def records_retrieval(context):
        return context.resources.context_receipts.record_retrieval(
            context_name="dagster_context",
            items=[{"item_id": "source-1"}],
        )

    result = dg.materialize([records_retrieval], resources=definitions.resources)
    assert result.success
    assert result.output_for_node("records_retrieval")


def test_concurrent_first_open_loses_no_receipts(tmp_path):
    module = _load_module()
    path = str(tmp_path / "shared.sqlite3")
    ids, errors = [], []

    def worker():
        try:
            local = module.ContextReceiptStore(path)
            for _ in range(10):
                ids.append(local.record_retrieval(context_name="c", items=[{"item_id": "a"}]))
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(set(ids)) == 60
    assert len(module.ContextReceiptStore(path).find_by_item("a")) == 60


def test_duplicate_items_rejected_and_missing_rank_defaults_to_position(store):
    with pytest.raises(ValueError, match="duplicate item_id"):
        store.record_retrieval(context_name="c", items=[{"item_id": "a"}, {"item_id": "a"}])
    rid = store.record_retrieval(context_name="c", items=[{"item_id": "a"}, {"item_id": "b"}])
    assert [i["rank"] for i in store.get_receipt(rid)["items"]] == [0, 1]
