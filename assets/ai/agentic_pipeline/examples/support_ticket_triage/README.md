# Support ticket triage — agent + deterministic + agent, mid-pipeline

A full, real, end-to-end example tying together everything in this component set: dynamic agent discovery, capability-based routing, MCP schema validation, and reusing an existing deterministic transform mid-pipeline.

**The scenario:** categorize a batch of support tickets by severity (an agent's judgment call), deterministically keep only the severe ones (no LLM needed — just a filter condition), then hand the survivors to whichever registered agent is actually tagged for triage (dynamically discovered, not hardcoded).

```
support_tickets (synthetic data)
        │
        ▼
┌───────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   categorize   │ --> │  filter_severe   │ --> │     triaged      │
│  op: map       │     │ op: invoke_      │     │  op: delegate    │
│  (agent judges │     │     component    │     │ (dynamically     │
│   severity)    │     │ (real Filter-    │     │  picks the       │
│                │     │  Component, no   │     │  `triage`-       │
│                │     │  LLM, no new     │     │  capable agent)  │
│                │     │  lineage node)   │     │                  │
└───────────────┘     └──────────────────┘     └─────────────────┘
```

Every step here is real and was actually run, not hand-typed — see "What you'll actually see" below for the genuine output.

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent` — the 3 steps above. |
| `triage_agent.yaml` | An `AgentCardComponent` — the agent `delegate` is supposed to find and pick. `capabilities: [triage]`. |
| `billing_agent.yaml` | A SECOND registered agent, `capabilities: [lookup]` — present specifically to prove `required_capabilities` excludes it before the picker LLM even runs. Never actually called. |
| `triage_mcp_server.py` | The triage agent's real implementation — see "The agent is just an LLM with a persona" below. |
| `tickets.json` | Sample data — a frozen snapshot from `SyntheticDataGeneratorComponent`'s built-in `support_tickets` schema (see `ticket_source.yaml`). |
| `ticket_source.yaml` | Reference config showing how `tickets.json` was generated (and how to regenerate it with different `row_count`/`random_state`). |

## The agent is just an LLM with a persona

If you don't have a real external agent service to call (most people don't, starting out) — that's fine, and it's not a gap in this example. `triage_agent.yaml`'s "agent" is `triage_mcp_server.py`: a ~30-line MCP server whose one tool, instead of running fixed logic, makes a real LLM call with its own system prompt:

```python
TRIAGE_SYSTEM_PROMPT = """You are a senior support triage specialist. Given a \
severe support ticket's context, decide which engineering team should own it \
and how urgent it is. ..."""
```

That's it — that's the whole "agent." Any team can stand one of these up in an afternoon: a small MCP (or HTTP) wrapper around an LLM call with a specific persona, registered as an `AgentCardComponent`. It's then callable by `delegate` exactly like a hosted third-party agent would be, with zero infrastructure beyond an LLM API key.

## A real gotcha this example hit (and you will too): `{prompt}` is not your data

`triage_agent.yaml`'s `tool_args_template` looks like this:

```yaml
tool_args_template: {ticket_context: "{extra.src_text}"}
```

Not `{prompt}`. This tripped up an earlier version of this exact example: `{prompt}` substitutes the `delegate` step's `task:` text — the *instruction* ("Triage this batch of severe support tickets: assign owning team + urgency"), not the actual ticket data. Template it as `{ticket_context: "{prompt}"}` and the agent receives only the instruction, with no real tickets — the LLM will still confidently return a plausible-looking `{team, urgency, reasoning}`, just fabricated from nothing. `{extra.src_text}` is the real upstream data (here, the filtered severe tickets). If your card needs both the instruction and the data, template something like `"Task: {prompt}\n\nData:\n{extra.src_text}"`. This only matters when `delegate` has upstream data flowing in via `source:`/a prior step — a pure task-only call (no separate source) doesn't have this trap.

## Why `required_capabilities` matters here

Both `triage_agent` and `billing_lookup_agent` are registered (same defs folder, both discovered by `delegate`'s sibling scan). `pipeline.yaml`'s `triaged` step sets `required_capabilities: [triage]` — so `billing_lookup_agent` (tagged `[lookup]`) is dropped *before* the picker LLM is even called. With 2 registered agents this is a nice-to-have; with the hundreds this mechanism is actually designed for, it's the difference between a usable system and an expensive, unreliable game of "guess which of 300 descriptions fits."

## Why `tickets.json` is a frozen fixture, not a live upstream asset

You might expect `pipeline.yaml`'s `source:` to be `{kind: upstream_asset, upstream_asset_key: support_tickets}`, pointing straight at a live `SyntheticDataGeneratorComponent` asset. That doesn't quite work today: `upstream_asset` sourcing expects the upstream asset's value to be a string or a `{text: ...}` dict, and `SyntheticDataGeneratorComponent` emits a DataFrame — which falls through to `str(df)` (a printed table, not JSON), not something `map`'s `_parse_items` can parse. Rather than paper over that, this example generates `tickets.json` once from the real component (`ticket_source.yaml` shows exactly how) and ships it as a fixture. If you want live composition, insert your own small text-rendering step between the two (e.g. `.to_dict(orient="records")` + `json.dumps`) — nothing in `invoke_component` stops you from using the exact same direct-call mechanism for that.

## What you'll actually see

This was run for real — `gpt-4o-mini` for both categorization and the triage agent, a real local MCP server — not simulated. From the 12 generated tickets, the categorizer correctly separated genuine outages/security alerts/damaged-product refunds/cancellation-risk ("severe") from routine quote requests and feature requests ("minor") and from "important but not urgent" bugs ("moderate"):

```json
{"ticket_id": "T00002", "ticket_text": "Site is down for me — getting 502 errors since 9am EST.", "category": "severe", ...}
{"ticket_id": "T00001", "ticket_text": "Can I get an enterprise quote? Contact: ...", "category": "minor", ...}
{"ticket_id": "T00004", "ticket_text": "Bug report: search results show duplicates when filtering by date range.", "category": "moderate", ...}
```

6 of 12 survived the deterministic filter. `delegate` then considered only `triage_agent` (`billing_lookup_agent` excluded by `required_capabilities`), picked it with the reasoning:

> "The ticket is about a site being down, which is a critical issue that requires immediate attention."

...and the real MCP-backed triage agent, given the actual filtered ticket batch (not just the instruction — see the gotcha above), returned:

```json
{"team": "infra", "urgency": "P0", "reasoning": "The site is down and customers are experiencing 502 errors, which is critical and needs immediate attention."}
```

Note this is one aggregate decision for the whole severe batch, not a separate decision per ticket — the pipeline's `task:` asks for a batch-level call. For a decision per individual ticket, you'd fan out with `map` over the filtered list instead of a single `delegate` call.

## Running it for real

Drop `pipeline.yaml`, `triage_agent.yaml`, `billing_agent.yaml`, `triage_mcp_server.py`, and `tickets.json` into the same defs folder of a real `dg` project, set `OPENAI_API_KEY`, and materialize `ticket_triage_triaged` (pulls in `ticket_triage_categorize` and `ticket_triage_filter_severe` as dependencies automatically).
