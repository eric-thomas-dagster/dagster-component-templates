# Route a batch of tickets one at a time -- `route`'s `per_item` mode

A `route` step with `per_item: true`: each ticket in a batch gets its OWN router decision and its own specialist's answer, instead of one router decision for the whole batch. The sibling `route_to_specialist` example shows `route`/`delegate` on a single question -- this one shows the list case.

```
tickets.json (3 tickets: shipping, damaged order, login)
        │
        ▼
┌─────────────────────┐
│       triage          │   per_item: true -- ONE router call + ONE specialist
│    op: route           │   answer PER TICKET: T-1001 -> shipping, T-1002 ->
│  (3 specialists,       │   returns, T-1003 -> account. output_join: records
│   per_item: true)      │   joins the 3 results into one JSON array.
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│       digest           │   synthesize reads triage's records (same shape
│    op: synthesize      │   it already reads after a `map` step's
└─────────────────────┘   output_join: records) and summarizes all 3.
```

## Why this exists

Without `per_item`, a `route` step reads its whole source as one piece of text: the router picks **one** specialist for the entire batch, and that one specialist answers *every* ticket in it. Given `tickets.json`'s three unrelated tickets (a shipping question, a damaged order, a login problem), batch-level `route` would pick a single specialist -- say, shipping, because the router only sees the question-shaped parts at the top of the combined text -- and that shipping specialist would answer the damaged-order and login tickets too, which is wrong for all but the one it actually matched.

`per_item: true` parses the source as a list (the same rule `map`/`invoke_component` use: a JSON array, a dict with a `data`/`items`/`results`/`values` list, or non-empty lines) and runs the router + specialist once per item. `output_join: records` joins the per-ticket results -- `{id, text, selected_specialist, router_reasoning, routing_source, text: <answer>}` per ticket -- into one JSON array, so `digest`'s `synthesize` step reads it the same way it would read a `map` step's `output_join: records` output.

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent` -- `route` (`per_item: true`, 3 inline specialists) into `synthesize`. |
| `tickets.json` | 3 tickets: a shipping question, a damaged order, a login problem -- deliberately three different specialists' territory. |

## Cost note

`per_item` mode makes **two model calls per item** (one router call to pick a specialist, one specialist call to answer) -- 6 calls total for these 3 tickets, where the batch-level default would make 2. Use a cheap model for `router` (it only needs to pick a name from a short list); spend the stronger model, if any, on the specialists.

## Running it

Set `OPENAI_API_KEY`, then materialize `ticket_routing_triage` and `ticket_routing_digest`. Check `ticket_routing_triage`'s materialization metadata: `routing_counts` (`{"shipping": 1, "returns": 1, "account": 1}` if each ticket lands where expected), `n_items` (3), `n_fallbacks` (0 unless the router ever fails to pick a valid name).
