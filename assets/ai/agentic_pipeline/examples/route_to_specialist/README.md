# Route to a specialist — semantic picking, not just tag matching

A `delegate` step with **no `required_capabilities`** at all — the point of this example is to prove the picker LLM does real semantic matching: it reads each registered specialist's `skills`/`description` and judges which one actually fits the incoming question, rather than relying on a pre-set tag.

```
source (a customer question)
        │
        ▼
┌─────────────────┐
│     routed       │   picker reads 3 candidates' skills/descriptions,
│  op: delegate    │   picks the one whose stated purpose matches the
│  (3 specialists  │   question's actual content, then that agent
│   registered)    │   drafts the response
└─────────────────┘
```

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent` — one `delegate` step, `source.text` set to a billing question by default. |
| `billing_specialist_agent.yaml` / `technical_specialist_agent.yaml` / `general_specialist_agent.yaml` | Three registered `AgentCardComponent`s, same shape, different persona + `skills` description. |
| `specialist_agent_server.py` | One script, three personas — selected by `argv[1]` (`billing`/`technical`/`general`), each card's `command:` passes a different one. |

## What you'll actually see — three real questions, three real picks

Same pipeline, same three registered agents, only `source.text` changes. All three were run for real against `gpt-4o-mini`:

**"I was charged twice for my subscription this month, can you help me get a refund for the extra charge?"**
→ picked `billing_specialist` — *"This agent specializes in billing issues, including refunds and subscription charges."*
→ response: *"I can help you with that! Please provide me with the details of your subscription, including the date of the charges and any relevant transaction IDs..."*

**"Your API keeps returning a 500 error when I POST to /v2/orders with more than 10 items in the payload."**
→ picked `technical_specialist` — *"This question involves a technical error related to the API, making the technical specialist the best fit to address it."*
→ response: *"A 500 error indicates an internal server issue, which could be related to how the server is handling larger payloads. Please check if there are any size limits set for the payload..."*

**"What's the difference between the free plan and the pro plan?"**
→ picked `general_specialist` — *"The question is about general product information regarding the differences between plans."*
→ response: *"The free plan typically offers basic features and limited usage... the pro plan usually includes advanced features, increased usage limits, priority support..."*

Notice each response is actually grounded in the specific question's content (the technical one references the real `500 error`/`/v2/orders`/payload details) — not generic boilerplate, because `tool_args_template` correctly passes `{extra.src_text}` (the real question) to the picked agent, not just the task instruction.

## `route` vs `delegate` — when to use which

The native `route` op does this exact pattern too — a router LLM picks from an inline `specialists:` list, each with its own `system_prompt`. Use `route` when your specialist roster is small, fixed, and known at pipeline-write time (simpler: one file, no separate agent cards). Use `delegate` + `AgentCardComponent` when the roster is shared across multiple pipelines, or maintained/grown by someone other than whoever writes this particular pipeline YAML — add a fourth specialist by dropping in one more `AgentCardComponent`, with zero edits to `pipeline.yaml`.

## Running it for real

**Each component needs its own directory.** Dagster's component scanner only loads a component from a file literally named `defs.yaml`, one per directory — the three specialist YAMLs as extra files sitting next to `pipeline.yaml` are never actually loaded as components. In your project's `defs/` folder, lay these out as four sibling directories, each containing one file renamed to `defs.yaml`:

```
defs/
  specialist_routing/
    defs.yaml                  # pipeline.yaml, renamed
  billing_specialist/
    defs.yaml                   # billing_specialist_agent.yaml, renamed
    specialist_agent_server.py
  technical_specialist/
    defs.yaml                   # technical_specialist_agent.yaml, renamed
    specialist_agent_server.py  # (a copy, or point command: at a shared location)
  general_specialist/
    defs.yaml                   # general_specialist_agent.yaml, renamed
    specialist_agent_server.py
```

Set `OPENAI_API_KEY`, then materialize `specialist_routing_routed`. Edit `defs.yaml`'s `source.text` to try a different question. `delegate`'s sibling-discovery scans the shared *parent* directory (`defs/` above), so all three specialists need to be true siblings of `specialist_routing`, not nested inside it.
