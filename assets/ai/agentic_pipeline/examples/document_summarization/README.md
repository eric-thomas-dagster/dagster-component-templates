# Summarize documents — one agent, structured output

The simplest of these examples: one registered agent, one `delegate` step. Matches the real Designer example task verbatim: *"Summarize incoming documents into a short executive summary and extract key action items."*

```
document.txt
     │
     ▼
┌─────────────┐
│  summarized  │   delegate picks summarizer_agent (capabilities: [summarize]),
│ op: delegate │   which returns {summary, action_items} -- forced structured
└─────────────┘   output, not free text, so action_items is a real list
```

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent` — one `delegate` step. |
| `summarizer_agent.yaml` | The registered `AgentCardComponent`, `capabilities: [summarize]`. |
| `summarizer_agent_server.py` | Its real implementation — an MCP tool wrapping an LLM call with forced tool-calling (`summary` + `action_items`), same pattern as the other examples' agents. |
| `document.txt` | A sample document — a realistic incident postmortem, picked because it has a clear narrative *and* a clear action-items list, so you can sanity-check the agent's output against the source by eye. |

## A real gotcha this example hit (and fixed): `{prompt}` is not your data

`summarizer_agent.yaml`'s `tool_args_template` is `{document_text: "{extra.src_text}"}`, not `{document_text: "{prompt}"}`. An earlier version of this example used `{prompt}` — which substitutes the `delegate` step's `task:` *instruction* text ("Summarize this document into..."), not the actual document. The agent received only the instruction, no real content, and the LLM still confidently returned a plausible-looking (but completely fabricated) executive summary. `{extra.src_text}` is the real upstream document text. See [`../support_ticket_triage/README.md`](../support_ticket_triage/README.md) for the full explanation — this is an easy trap to fall into and this example fell into it first.

## What you'll actually see

Run for real against `gpt-4o-mini`, on `document.txt` (a checkout-latency incident postmortem):

```json
{
  "summary": "On October 2, 2026, the checkout service experienced a significant latency increase due to a configuration change that improperly adjusted the connection pool size without updating the database's connection limits. This led to a temporary slowdown, causing an estimated 3% of checkout sessions to be abandoned. The issue was resolved by rolling back the change, which restored normal latency within 90 seconds.",
  "action_items": [
    "Require performance review sign-off for any change that touches connection pool sizing, not just schema or code changes.",
    "Reduce the checkout-latency alerting window from 10 minutes to 3 minutes, with a secondary fast-trip alert on p99 > 1s for 60 seconds.",
    "Add a runbook step that cross-references ALL deploys in the preceding 15 minutes across every team, not just the on-call engineer's own team.",
    "Add a circuit breaker to the inventory-reservation client so pool saturation fails fast instead of queuing indefinitely.",
    "Schedule a load test of the new pool size against the downstream database's actual max_connections before the next capacity change."
  ]
}
```

The action items are extracted verbatim from the real document's own numbered list — not reworded, not hallucinated.

## When you'd reach for `reduce` instead

This example's document is short enough to fit in one call. For a document (or set of documents) too long for one context window, see the native `reduce` op (chunk + fold) instead of `delegate` — `delegate` picks *who* handles something, `reduce` handles *how* to process something too big for one shot. The two compose: `reduce` to get a document down to a manageable summary, then `delegate` to a reviewer-persona agent for a final pass.

## Running it for real

Drop `pipeline.yaml`, `summarizer_agent.yaml`, `summarizer_agent_server.py`, and `document.txt` into the same defs folder of a real `dg` project, set `OPENAI_API_KEY`, and materialize `doc_summary_summarized`.
