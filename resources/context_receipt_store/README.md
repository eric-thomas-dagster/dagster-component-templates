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

`context_engineering_pipeline` is the build side of this loop: its `emit_data_version` and `stamp_provenance`
settings (on by default) write the asset key, content-derived version, partition, chunk id and content hash onto
every indexed chunk, so a gateway can build receipt items straight from index hits and a no-op rebuild does not
make receipts look stale. Any other pipeline works too if it stamps the same fields.

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

The wire contract is [`SPEC.md`](SPEC.md) / [`receipt-spec.v1.json`](receipt-spec.v1.json); `ingest()` and `export_events()` speak it, and tests validate everything the store emits against the schema.

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

## Declare context once: `@context_asset`

The decorator is the contract between the asset that builds context and the gateway that serves it.

```python
from dagster_community_components import context_asset, context_policies

@context_asset(scope="account_id", sensitivity="confidential", max_age_hours=24, token_budget=4000,
               access_tags=["support"])
@dg.asset(partitions_def=daily)
def account_context(): ...   # DataFrame with chunk_id, chunk_text, account_id, ...
```

Apply it after `@dg.asset`. It does three things:
1. **Declares policy** as `context/policy` metadata plus `context/*` tags (the pipeline component's
   `context_policy:` writes the same declaration). `context_policies(defs)` reads it back, which is how a
   gateway discovers which assets are governed context and what applies to each.
2. **Validates the output**: a DataFrame with unique, non-empty `chunk_id` and non-empty `chunk_text`
   (names configurable) or the materialization fails. Non-DataFrame results pass through untouched.
3. **Emits a content-derived data version**, so a no-op rebuild does not make receipts look stale.

The gateway enforces the same declaration and the result lands on the receipt:

```python
policy = context_policies(defs)["account_context"]
caller = CallerContext(clearance="internal", access_tags=("support",), scope_value="acme")  # from auth
kept, excluded = policy.apply(candidates, caller)   # excluded is receipt-ready: reasons below
store.record_retrieval(context_name="account_context", items=kept_items, excluded=excluded)
```

`evaluate` returns `out_of_scope` (tenant mismatch, or the caller has no `scope_value`: fail closed), `policy`
(missing access tag, or asset sensitivity above the caller's clearance), `pii` (chunk tagged `sensitivity: pii`
by the pipeline's `sensitivity` op, caller lacks `allow_pii`), or `stale` (`valid_until` passed; a bare date is valid
through that day; or older than `max_age_hours`). `apply` also enforces `token_budget` greedily in rank order
(`over_budget`). Caller identity must come from your auth layer, never from what the model says about itself.
`retention_days` is declared metadata only: nothing purges receipts or chunks on it yet.

## Where thumbs up/down and scoring come in

MCP has no standard feedback channel, so there are four ways an outcome reaches a receipt. All of them end as an
`outcome` event in the store; Dagster never sees the signal at write time, it reads and scores later.

1. **Carry the receipt id with the answer.** The gateway returns `receipt_id` in the tool result and in `_meta`
   (`context_receipt_id`; the middleware example does both), so the client or UI that renders the answer can post
   feedback against it.
2. **A feedback tool on the gateway** (`record_feedback` in the gateway example): the agent calls it with the
   receipt id when its user reacts.
3. **Write straight to the store from your app** (a support UI, CRM or ticketing webhook) using `record_outcome` or
   a spec `outcome` event. No MCP involved.
4. **Implicit signals** (`implicit_signals_enabled`, off by default): the sensor marks a retrieval `reformulated`
   (-0.2) when the same caller re-asks in the same MCP session within `reformulation_window_seconds` with a
   different query. A heuristic, so the weight is small; it needs `mcp_session_id` and `query_hash` on receipts.

Scores then flow as described below: outcome, weighted by the evidence that an item was used, becomes a prior that
re-ranks future retrievals and feeds the outcome-quality asset check.

## Close the loop: outcomes → scores → better context

Gateway writes receipts; Dagster reads them. Dagster is never in the agent's request path.

**Hot path (gateway, no Dagster):** `new_receipt_id()` assigns a time-sortable ID
client-side; `AsyncReceiptWriter` queues writes with retries so the agent never waits
on the store (failures land in `dead_letters`; it is in-process, so use a durable
outbox if losing receipts on process death is unacceptable).

```python
writer = AsyncReceiptWriter(ContextReceiptStore("./receipts.sqlite3"))
receipt_id = writer.record_retrieval(context_name="support", items=[...], excluded=[...])
writer.attach_output(receipt_id, output_id="a1", model="m", output_hash="...")
writer.record_outcome(receipt_id, signal="thumbs_down", outcome_ref="user-42")
```

**Outcomes:** `record_outcome` takes `thumbs_up`, `thumbs_down`, `accepted`, `edited`,
`escalated`, `reopened`, `closed_won`, `closed_lost` (or any signal with an explicit
value in [-1, 1]). A later outcome with the same `outcome_ref` supersedes the earlier
one, so a flipped thumb isn't double-counted. Outcomes can arrive weeks later.

**Scoring:** `refresh_item_scores()` credits/blames each retrieved item with the
receipt's outcome, weighted by the strongest evidence it was actually used
(retrieved 0.5 < cited 1.0 < verified 1.5 < counterfactual 2.0; evidence may name
`details.item_ids` so blame lands on the chunk that was relied on), shrunk toward 0
until there is enough evidence. `get_item_scores()` + `rerank_with_priors()` let the
gateway use the priors, and `rerank_with_priors(..., explore_fraction=0.1)` reserves
a slice of traffic for exploration (recorded as `selection="explore"` on the
receipt) so early winners don't lock in.

**Dagster sensor** (`usage_sensor_enabled: true`): tails receipts and outcomes by cursor,
emits **one aggregated `AssetObservation` per source asset per tick** (retrievals,
exclusions by reason, stale reads, outcome counts, mean outcome), refreshes the item
priors, and, only for assets listed in `rebuild_asset_keys`, requests a rebuild once
`stale_reads_threshold` receipt items were served from an out-of-date version.
"Out of date" means the receipt's `source_version` differs from the asset's latest
`dagster/data_version`, so the gateway should stamp `source_version` with that value
(assets without a data version are never judged stale).

This is a feedback loop that optimizes the context layer with the model held fixed,
not reinforcement learning. Limits: priors are only as good as the outcome signal;
credit is per item (not a causal estimate); no inverse-propensity correction for
exploration yet; scoring recomputes from scratch each tick (fine for prototype scale).

## Context health as asset checks

`check_asset_keys: [support/tickets]` attaches three checks to each asset, evaluated over a
rolling window (`check_window_hours`): **`context_staleness`** (ERROR: share of retrieved
items served from an out-of-date data version, per partition), **`context_outcome_quality`**
(WARN: mean outcome of answers built from the asset; needs `min_checked_receipts`
outcomes before it judges), and **`context_verified_share`** (WARN: share of outputs whose
receipt reached `verified` or `counterfactual_tested` evidence; `cited` does not count,
it is the model's own claim). They turn context debt into red/green on the asset. There is
no ungrounded-claim-rate check yet because nothing computes claim-level grounding.

## Trace an MCP call to its context

Retrieval events accept `trace_id` (e.g. an OpenTelemetry trace id), `mcp_session_id` and
`mcp_request_id`; the FastMCP middleware example fills the MCP ids from the request
context. Given a tool call you can find its receipt, the exact versioned, partitioned
context it was served, what was withheld and why, and the lineage behind it.

## Recall across lineage and partitions

Stamp each retrieved item with `source_asset_key`, `source_version` (the asset's
`dagster/data_version`) and, for partitioned assets, `source_partition`. Then:

```python
from dagster_community_components import receipts_impacted_by

# "ticket partition 2026-10-08 was wrong: which answers used it, or anything built from it?"
out = receipts_impacted_by(store, defs.resolve_asset_graph(), "tickets", ["2026-10-08"])
out["affected"]          # {"tickets": [...], "ticket_chunks": [...], "account_summary": None, ...}
out["receipt_ids"]       # receipts that used an affected asset/partition
out["partition_unknown"] # used an affected asset but recorded no partition: can't be ruled out
```

It walks child assets through the graph and translates partitions across each edge's real
partition mapping (identity, daily→weekly, all-to-one for an unpartitioned child, and so on).
The usage sensor judges staleness and requests rebuilds per `(asset, partition)`.
Limits: it follows asset dependencies only, not what a downstream *context* was
built from outside Dagster, and it reports affected receipts rather than notifying anyone.

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
