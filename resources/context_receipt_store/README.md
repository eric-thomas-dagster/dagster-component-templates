# Context Receipt Store

An append-only, SQLite-backed prototype for recording context retrievals and
linking later agent outputs to them. It is designed to sit beside the existing
[`context_engineering_pipeline`](../../assets/ai/context_engineering_pipeline/README.md):
Dagster builds and versions context; a serving gateway records what it actually
retrieved.

This component does **not** change Dagster internals or put online retrieval
traffic in Dagster's event log. The receipt store is an ordinary Dagster
resource and a standalone Python API that a FastMCP tool or Prefect flow can
also call.

## Configure the Dagster resource

```yaml
type: dagster_community_components.ContextReceiptStoreComponent
attributes:
  resource_key: context_receipts
  database_path: ./var/context-receipts.sqlite3
```

The default backend is appropriate for local development and single-host
prototypes. SQLite is **not** a shared multi-host production service; use a
shared database or receipt API when the gateway and agent workers run on
different hosts.

## Record a retrieval at the gateway

The public `ContextReceiptStore` API can be used outside a Dagster run:

```python
import hashlib
from dagster_community_components import ContextReceiptStore

receipts = ContextReceiptStore("./var/context-receipts.sqlite3")

query = "What renewal risks were reported?"
hits = vector_store.search(query)
receipt_id = receipts.record_retrieval(
    context_name="account_support_context",
    context_version="materialization:abc123",
    policy_version="support-policy-v4",
    caller_id="support-agent",
    purpose="answer_customer_question",
    query_hash=hashlib.sha256(query.encode()).hexdigest(),
    eligible_count=12,
    excluded_count=2,
    items=[
        {
            "item_id": hit["chunk_id"],
            "source_asset_key": "support_tickets",
            "source_version": hit["source_version"],
            "rank": rank,
            "score": hit["score"],
        }
        for rank, hit in enumerate(hits)
    ],
)

# Return the ID with the retrieved context so the agent run can carry it.
return {"receipt_id": receipt_id, "context": hits}
```

FastMCP and Prefect are intentionally not dependencies of this template. Call
`record_retrieval` in the gateway after policy evaluation and retrieval, then
pass the returned ID through the agent run. After producing an output, append
an output reference:

```python
receipts.attach_output(
    receipt_id,
    output_id="answer-0194",
    model="example-model",
    model_version="2026-09",
    output_hash=hashlib.sha256(answer.encode()).hexdigest(),
    prefect_run_id=prefect_flow_run_id,
)
```

The retrieval record is not rewritten. Output links and later evidence are
separate appended events. Evidence levels supported are `cited`, `verified`,
and `counterfactual_tested`; they describe the evidence recorded, not certainty
that a black-box model causally relied on a particular item.

## Record what was withheld

Pass `excluded=[{"item_id": ..., "reason": "pii"}]` (suggested reasons:
`pii`, `stale`, `out_of_scope`, `over_budget`, `policy`) to `record_retrieval`.
An item can't be both retrieved and excluded, and `excluded_count` defaults to
the number listed. `get_receipt()` returns them under `"excluded"`.

## FastMCP examples

Both run against stock FastMCP (tested with 4.1.0); no fork is needed. They import
`ContextReceiptStore` from the installed `dagster-community-components` package.

- [`examples/fastmcp_gateway.py`](examples/fastmcp_gateway.py): a gateway with
  `search_context` (policy filter → receipt with excluded reasons),
  `attach_output`, `explain(receipt_id)` and `impacted_by(asset)` tools.
- [`examples/fastmcp_receipt_middleware.py`](examples/fastmcp_receipt_middleware.py):
  a `Middleware` that receipts any tool returning a `_receipt` dict, so tools
  stay receipt-agnostic.

## Look up impact

```python
receipt_ids = receipts.find_by_item("ticket-184:chunk-2")
receipt = receipts.get_receipt(receipt_ids[0])
```

`find_by_source_asset("support_tickets")` finds receipts that referenced any
item attributed to that Dagster asset. This is a first prototype of recall and
impact analysis; it does not yet query Dagster's asset graph automatically.

## Privacy and integrity

- Receipt item fields are restricted to stable item IDs, source asset/version,
  rank, and score. Raw context is never stored by this API.
- Pass a one-way `query_hash`, not the raw query. Attach output hashes or
  references, not output text.
- Treat caller IDs, purpose labels, asset keys, and item IDs as potentially
  sensitive metadata and apply your organization's retention/access controls.
- The SQLite file is append-only through this API, but is not tamper-evident,
  encrypted, or a compliance-grade audit ledger.
- Unknown receipt IDs and invalid fields raise errors; they are never silently
  ignored.
