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

## Closing the loop: reading receipts back into the build

Receipts are only useful if something learns from them. `rag_pipeline`'s `receipts:` block records every retrieval (and reranks with the learned item
priors); `context_engineering_pipeline`'s `receipts:` block reads them back at build time. The read side it uses is
`store.item_feedback(asset_key, since="", code_version=None)`:

```python
store.item_feedback("support_kb", since="2026-09-17T00:00:00+00:00", code_version="recipe-7")
# {"support_kb_billing_c_0": {"retrievals": 6, "rated": 6, "mean_outcome": -1.0, "cited": 0,
#                             "excluded": {"stale": 2}, "last_retrieved_at": "..."}, ...}
```

Per chunk: how often it was retrieved, how many of those receipts have an outcome and their mean (a receipt's effective outcome is the mean of its outcomes; a later
outcome with the same `outcome_ref` supersedes the earlier -- the same counting as `refresh_item_scores`), how often it was cited, and why the gateway withheld it.
`code_version` scopes the answer to chunks built by one recipe, so a recipe change does not inherit ratings from different chunks. See
[`context_engineering_pipeline`'s "Learning from receipts"](../../assets/ai/context_engineering_pipeline/README.md#learning-from-receipts).

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

## Receipts in BigQuery

SQLite is one file on one host. If your agents and gateways run on several hosts, or your warehouse is BigQuery and you want receipts next to your data (and readable by a SQL-mode `context_engineering_pipeline`), keep them in BigQuery:

```yaml
type: dagster_community_components.ContextReceiptStoreComponent
attributes:
  resource_key: context_receipts
  backend: bigquery
  bigquery_dataset: my-project.context        # five tables are created on first use
  bigquery_table_prefix: prod_                # optional: several environments in one dataset
  gcp_credentials_env_var: GCP_SA_JSON_B64    # omit for Application Default Credentials
  async_writes: true                          # default
```

The resource has the same API as the SQLite store (`record_retrieval`, `record_outcome`, `item_feedback`, `refresh_item_scores`, `outcomes_by_code_version`, `ingest`, `enforce_retention`, ...), so `rag_pipeline`, the gateway helpers and the pipeline's `receipts:` work unchanged. A parity test runs one scenario through both backends and requires identical results.

- **Writes are load jobs, not streaming inserts**, so rows stay deletable/updatable (retention, score refresh). A load job takes seconds, hence `async_writes: true`: writes are queued on a background thread and return at once; argument errors still raise at the call site, storage failures that survive retries land in `store.dead_letters`. Reads see finished writes only; call `store.flush()` for read-your-writes. Queued writes are lost if the process dies.
- **Receipt row is written last** (it is the commit marker); a retry with the same `receipt_id` cleans up orphan items from a crashed attempt.
- `read_since` cursors are microsecond timestamps instead of row positions.
- `refresh_item_scores` reads all items/events and recomputes in Python (then one `MERGE`): fine for hundreds of thousands of receipts, not billions.
- Already have receipts in SQLite? Keep them local and ship them with `ContextReceiptBigQuerySyncComponent` instead of switching backends.

Verification: exercised against a fake BigQuery client that parses every statement as BigQuery (sqlglot) and runs it in DuckDB. Not run against real BigQuery.

## Receipts in Postgres, MySQL, Snowflake, Databricks, DuckDB

`backend: sql` keeps receipts in any database SQLAlchemy can reach, with the same API and the same results as the other backends:

```yaml
type: dagster_community_components.ContextReceiptStoreComponent
attributes:
  backend: sql
  sql_url_env_var: RECEIPTS_DB_URL      # postgresql://..., mysql+pymysql://..., snowflake://..., databricks://..., duckdb:///receipts.duckdb
  sql_schema: ANALYTICS.CONTEXT         # database.schema (Snowflake), catalog.schema (Databricks), schema (Postgres), database (MySQL)
  sql_table_prefix: prod_
  async_writes: false                   # true for slow warehouses
```

Install the database's SQLAlchemy driver yourself (`psycopg2-binary`, `snowflake-sqlalchemy`, `databricks-sqlalchemy`, ...). The engine is built on first use, so an unset URL variable does not break loading the code location. A receipt is written in one transaction (receipt row last) where the database has transactions. Cursors for `read_since` are microsecond timestamps, as in the BigQuery backend.

### Without SQLAlchemy: the vendor's own connector

If you already have `dagster-snowflake` (`snowflake-connector-python`), `databricks-sql-connector`, `psycopg2` or `pymysql`, skip SQLAlchemy and its dialect packages and give `sql_connection` instead of `sql_url_env_var` (exactly one of the two is required). Secrets are read from the named environment variables when the first connection is opened; the connector is imported then too, so a missing one raises an `ImportError` naming the package to `pip install`.

```yaml
attributes:
  backend: sql
  sql_schema: ANALYTICS.CONTEXT
  sql_connection:
    kind: snowflake                     # pip install snowflake-connector-python (already there with dagster-snowflake)
    account: acme-xy1
    user: RECEIPTS
    warehouse: WH
    role: RECEIPTS_RW
    password_env_var: SNOWFLAKE_PASSWORD        # or private_key_env_var (PEM) [+ private_key_passphrase_env_var]
```

| `kind` | keys | pip package |
|---|---|---|
| `snowflake` | `account`, `user`, `warehouse`, `database`, `schema`, `role`, `password_env_var` or `private_key_env_var` (+ `private_key_passphrase_env_var`), `authenticator`, `connect_args` | `snowflake-connector-python` |
| `databricks` | `server_hostname`, `http_path`, `access_token_env_var`, `catalog`, `schema`, `connect_args` | `databricks-sql-connector` |
| `postgres` | `host`, `port`, `database`, `user`, `password_env_var`, `connect_args` | `psycopg2-binary` |
| `mysql` | `host`, `port`, `database`, `user`, `password_env_var`, `connect_args` | `pymysql` |

In Python: `SqlContextReceiptStore(connection_factory=lambda: my_connection(), dialect="snowflake", schema="DB.SCHEMA")` accepts any zero-argument callable that returns a PEP-249 connection.

How it runs on a plain connection:

- **Parameters.** The store's SQL uses `:name` and `IN :ids` binds. One converter rewrites them for the driver's `paramstyle` (qmark, numeric, named, format, pyformat), expanding lists to `(%(ids_0)s, %(ids_1)s, ...)` and leaving quoted strings, quoted identifiers, comments, `::` casts and `col:field` paths alone. `paramstyle` is read from the driver module (`snowflake.connector` is pyformat, psycopg2 and pymysql pyformat, sqlite3 and duckdb qmark) or pass `paramstyle=`. For Databricks, `databricks-sql-connector` 3.x declares `named` (native `:name` parameters) and 2.x declared `pyformat` (client-side inlining); the store follows whatever the installed module declares.
- **Transactions.** Snowflake and DuckDB connections autocommit, so a multi-statement write is wrapped in `BEGIN` ... `COMMIT` (`ROLLBACK` on error); a single statement is not wrapped. Postgres, MySQL and SQLite use the driver's implicit transaction with `commit()`/`rollback()`. Databricks has no multi-statement transactions at all: each statement stands alone, and the receipt row is written last as the commit marker, so a crashed write leaves only orphan item rows that the next attempt deletes. Override with `transaction_mode='dbapi'|'sql'|'none'`.
- **Column names** from `cursor.description` are lowercased (Snowflake returns UPPERCASE). One connection is kept per thread and reconnected after a failed rollback.

Verification of this path: the converter is unit-tested on all five paramstyles, and the whole store (every parametrized backend suite, including the self-test) runs on real `sqlite3` and real `duckdb` connections. Snowflake, Databricks, Postgres and MySQL have no real server behind them here: a Snowflake-shaped connection (pyformat, UPPERCASE names, autocommit) wrapping sqlite and stand-in vendor modules prove the conversion, name handling and connection arguments, not the services. Run `python3 tools/verify_receipts_backend.py dbapi --kind snowflake ...` against yours.

Verification: SQLite and DuckDB run for real through SQLAlchemy, with a parity test against the native SQLite store. For Postgres, MySQL, Snowflake and Databricks the generated statements are only parsed with sqlglot -- no such servers were available, so treat those four as unverified until you run them against yours.

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

## Data version and code version

Dagster gives every materialization two versions, and they answer different questions:

- **Data version** (`dagster/data_version`): what the output *is*. Receipts record it as `source_version`; an item whose
  version differs from the asset's current one is a stale read. For context, make it content-derived (the pipeline and
  `@context_asset` do). Dagster's own default derives from the asset's code version and its inputs' data versions, so it
  is stable across no-op rebuilds only if you set `code_version` *and* every input is explicitly versioned; an
  upstream asset with no explicit version (a typical raw table) changes it on every run, and every receipt looks stale.
- **Code version** (`dagster/code_version`): the *recipe* that built it (chunking, filters, embedding model). Set it with
  `@dg.asset(code_version=...)`, `@context_asset(code_version=...)`, or let the pipeline fingerprint its steps. Dagster
  marks the asset stale when it changes (`AutomationCondition.code_version_changed()`), and if the gateway stamps it as
  `source_code_version`, `store.outcomes_by_code_version("support_kb")` answers "did the new chunking help?" from what users
  actually thought. The usage sensor reports the same split as `context_receipts/outcomes_by_code_version`.

## Governance: consumption, retention, health

**Downstream consumption.** `@context_consumer(clearance="internal", access_tags=[...])` (after `@dg.asset`) declares who an
asset's audience is. `check_context_consumption(defs)` raises, at load and before any run, if such an asset has a context
ancestor whose policy it is not cleared for (sensitivity above its clearance, or required access tags it lacks), looking
through intermediate assets. `validate_context_consumption(defs)` returns the violations instead. Per-tenant `scope` and
chunk-level PII are enforced at retrieval, not from the graph.

**Retention.** Set `retention_days` in a policy and `enforce_retention: true` on the component: the usage sensor deletes
receipts past it, using the shortest retention among the assets a receipt touched; receipts that touched no asset with a
retention are kept unless you set `default_retention_days` (see Privacy). `store.enforce_retention({...}, default_days=...)` does the same on demand. It is the only deletion the store performs;
aggregated item priors are statistics and are kept. (Expired chunks in the index: `purge_expired` on the pipeline's
`write_vector_store` step.)

**Health.** `context_health(store, instance, ["support_kb"])` returns `{status: healthy | degraded | unhealthy, failing,
checks, current data and code version}` from the same numbers the asset checks use, so a gateway can put it in a result or
an agent runtime can degrade on it. It is a library call; serving it over a network API is a deployment choice.

## Context health as asset checks

`check_asset_keys: [support/tickets]` attaches four checks to each asset, evaluated over a
rolling window (`check_window_hours`): **`context_staleness`** (ERROR: share of retrieved
items served from an out-of-date data version, per partition), **`context_outcome_quality`**
(WARN: mean outcome of answers built from the asset; needs `min_checked_receipts`
outcomes before it judges), and **`context_verified_share`** (WARN: share of outputs whose
receipt reached `verified` or `counterfactual_tested` evidence; `cited` does not count,
it is the model's own claim), and **`context_grounding`** (WARN: share of claims a verifier found ungrounded, read from
evidence `details.claims_total` / `details.claims_ungrounded`; passes when no verifier has reported claims, and the
verifier that produces those counts is not part of this module). They turn context debt into red/green on the asset.

## Trace an MCP call to its context

Retrieval events accept `trace_id` (e.g. an OpenTelemetry trace id), `mcp_session_id` and
`mcp_request_id`; the FastMCP middleware example fills the MCP ids from the request
context. Given a tool call you can find its receipt, the exact versioned, partitioned
context it was served, what was withheld and why, and the lineage behind it.

## Recall across lineage and partitions

Stamp each retrieved item with `source_asset_key`, `source_version` (the asset's
`dagster/data_version`) and, for partitioned assets, `source_partition`. Then:
(`source_partition` is where a chunk came from. Separately, a receipt's `dagster_partition_key` is the partition of the
*producing* asset's run that answered; query it with `store.receipts_for_partition(asset_key, partition_key)`.)

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

## Verify your backend

Without Dagster, from a checkout of this repo, run `python3 tools/verify_receipts_backend.py bigquery --dataset my-project.context --prefix verify_` (or `sql --url-env RECEIPTS_URL --schema ...`, or `dbapi --kind snowflake|databricks|postgres|mysql ...` for the vendor connector, or `sqlite --path ...`). It runs the same self-test, prints a PASS/FAIL line per step and exits non-zero on any failure -- a one-command check against your own warehouse.

Pointed this at a real Snowflake, BigQuery or Postgres? Check it in one step:

```yaml
attributes:
  selftest_asset: true      # adds the asset "<resource_key>_selftest"
```

Materialize `context_receipts_selftest` (or call `store.selftest()`). It writes synthetic receipts with fresh uuid ids (concurrent
runs never collide), then exercises create-tables, `record_retrieval`, duplicate detection, outcome / evidence / output events,
`get_receipt`, `read_since` (receipts and every event type), `item_feedback` numbers, `refresh_item_scores` + `get_item_scores`,
`outcomes_by_code_version`, `asset_usage_since`, the `find_by_*` lookups and `maintenance_report`, and finally deletes exactly the
rows it wrote and verifies they are gone (`delete_receipts` / `delete_item_scores`). Each step is reported as `{step, ok, detail}`;
one failure does not stop the others, and cleanup runs in a `finally`. The asset shows the step table as metadata and fails the
run if any step failed. The refresh step is the normal store-wide score recompute (a full read; rows get a new `updated_at`), so
the asset uses your `score_prior_strength` to leave real priors unchanged in value.

## Privacy: pseudonyms and retention

```yaml
attributes:
  pseudonymize_callers: true
  pseudonym_salt_env_var: RECEIPT_PSEUDONYM_SALT   # a secret; read at write time via dagster EnvVar
  enforce_retention: true
  usage_sensor_enabled: true
  default_retention_days: 90                       # receipts of assets whose policy declares no retention
```

- `pseudonymize_callers` stores `caller_id` and `mcp_session_id` as `h:` plus the first 32 hex chars of HMAC-SHA256(salt, id),
  in every backend (it is applied inside the shared validation, and by the async wrapper before queueing). The hash is
  deterministic, so a caller or session still groups and implicit-outcome derivation keeps working. Losing or changing the salt breaks
  continuity with earlier receipts; rows written before you turned it on are not rewritten. With the salt unset, writes fail rather than
  fall back to raw ids. Other fields (`purpose`, `trace_id`, `mcp_request_id`) are not hashed.
- `default_retention_days` is applied by the usage sensor's `enforce_retention` path to receipts of assets whose context policy
  declares no `retention_days` (and receipts that touched no asset). An asset's own policy always wins, and the shortest applicable
  limit still decides for a receipt that touched several assets. `None` (default) keeps everything: nothing is deleted unless you set it.
- On a shared-warehouse backend (`bigquery`, `sql`) the component logs a load-time WARNING when neither retention nor
  pseudonymization is configured, because those tables hold caller ids.

## Maintenance report: what to rewrite, delete or keep

`store.maintenance_report("support/tickets", since=..., known_item_ids=[...])` turns `item_feedback` into a to-do list; it is one pure
function (`fold_maintenance_report`) over the feedback, so it is identical on every backend.

- `rewrite_candidates`: chunks retrieved >= `min_retrievals` (5) and rated >= `min_outcomes` (3) whose mean outcome is <= `bad_below`
  (-0.3), worst first, each with `retrievals`, `rated`, `mean_outcome`, `cited` and `excluded` reasons.
- `dead_weight`: ids in `known_item_ids` never retrieved. Only judged when the asset had >= `dead_after_receipts` (50) receipts in the
  window; otherwise `None` and `dead_weight_reason` says why. You supply `known_item_ids` (the current index's chunk ids).
- `top_performers`: mean >= `good_above` (0.3), best first. `noisy_exclusions`: chunks withheld >= `min_exclusions` (5) times.
- `summary`: the counts.

`maintenance_check_asset_keys: [support/tickets]` adds a WARN-severity asset check `context_maintenance` per asset (window
`maintenance_window_days`, thresholds `maintenance_min_retrievals` / `maintenance_min_outcomes` / `maintenance_bad_below`) that
reports the counts and the worst chunk ids as metadata. It is WARN, so it never fails a run. The component form does not judge dead
weight (it has no list of known ids); use `build_maintenance_check(store, key, known_item_ids=...)` for that.

## Privacy and integrity

- Receipt item fields are restricted to stable item IDs, source asset/version,
  rank, and score. Raw context is never stored by this API.
- Pass a one-way `query_hash`, not the raw query. Attach output hashes or
  references, not output text.
- Treat caller IDs, purpose labels, asset keys, and item IDs as potentially
  sensitive metadata and apply your organization's retention/access controls
  (`pseudonymize_callers` and `default_retention_days` above).
- The SQLite file is append-only through this API, but is not tamper-evident,
  encrypted, or a compliance-grade audit ledger.
- Unknown receipt IDs and invalid fields raise errors; they are never silently
  ignored.
