# Context Receipt Spec v1

The receipt is the join key between the products in a context-engineering stack. This
document is the contract; [`receipt-spec.v1.json`](receipt-spec.v1.json) is its machine
form (JSON Schema, draft 2020-12). `ContextReceiptStore` is one implementation of it and
`ContextReceiptStore.ingest()` / `export_events()` speak it directly.

## Who writes, who reads

| Role | Writes | Reads |
|---|---|---|
| **MCP gateway** (e.g. FastMCP) | `retrieval` (what was retrieved, what was withheld and why), usually `output` | priors, to rerank |
| **Agent runtime** (e.g. Prefect flow) | `output`, `evidence` (cited / verified / counterfactual) | |
| **Apps** (support UI, CRM, ticketing) | `outcome` (thumbs, edits, escalations, deal results) | |
| **Dagster** | nothing on the hot path | receipts and outcomes by cursor: aggregated asset observations, stale-read detection, item priors, rebuild requests |

Dagster is never in the request path and never needs to be told what a thumbs-down is;
`signal` values are part of this schema, not Dagster concepts.

## Events

A receipt is one `retrieval` event followed by any number of appended events that
reference its `receipt_id`. Appended events never rewrite the retrieval.

- `retrieval`: `context_name`, `items[]` (`item_id`, `source_asset_key`, `source_version`,
  `source_partition`, `source_code_version`, `rank`, `score`, `selection`), `excluded[]` (`item_id`, `reason`), plus caller, purpose,
  policy and context versions, `query_hash`, counts.
- `output`: `output_id`, `model`, `model_version`, `output_hash`, `prefect_run_id`.
- `evidence`: `level` in `cited | verified | counterfactual_tested`, `details`
  (`details.item_ids` scopes it to specific items).
- `outcome`: `signal`, `value` in [-1, 1], `source`, `outcome_ref`.

Every event carries `spec_version: "1"` and `type`.

## Rules

1. **No content.** Identifiers, versions, hashes and scores only. Raw queries, chunks and
   output text are never part of a receipt; schemas set `additionalProperties: false`.
2. **Writer-assigned IDs.** The writer picks `receipt_id` (a ULID is recommended: sortable,
   no round trip). Re-sending a `retrieval` with an existing id is a no-op for consumers,
   so delivery can be at-least-once.
3. **`source_version` = the source asset's `dagster/data_version`** at retrieval time, and
   **`source_partition` = its partition key** when the asset is partitioned. **`source_code_version` = its `dagster/code_version`** (the recipe that built it) is optional and lets outcomes be compared across recipe versions. Together they make
   stale reads and recall exact per partition. If `source_partition` is absent on a partitioned
   asset, impact analysis must assume the item may be from any partition.
4. **Retrieved is fact, used is evidence.** `evidence.level` says what was done, not how
   certain the conclusion is. Treat "used" as an estimate.
5. **`selection: "explore"`** marks reserved exploration traffic so scoring can discount it.
6. **Judges have identities.** Outcomes with the same `outcome_ref` supersede one another
   when scoring (a user changing a thumb counts once).
7. **Timestamps (v1).** `created_at` / `occurred_at` are informational; consumers stamp
   their own write time.

## Versioning

`spec_version` is a major version. Consumers reject versions they don't know. Adding an
optional field is a minor change within v1 only if existing writers stay valid; anything
else is v2. Transport and storage (queue, database, HTTP) are deliberately out of scope.
