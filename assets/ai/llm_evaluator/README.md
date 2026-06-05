# LLM Evaluator (LLM-as-judge)

> **🔑 API key required.** Calls a judge LLM via LiteLLM. Cheap models work well — `gpt-4o-mini` runs ~$0.0001 per evaluation.

Pass-through asset that takes an upstream agent / LLM output and scores it on one or more **feedback metrics** — same conceptual model as [TruLens](https://www.trulens.org/) (groundedness, answer relevance, harmfulness, …) — but as a direct LiteLLM call with a curated prompt per metric. **No TruLens framework dependency**, no `langchain`-version hell, no eval database to manage.

Pairs naturally with `litellm_agent` / `openai_agent` / `anthropic_agent` / `gemini_agent`: point `upstream_asset_key` at one of those, pick your feedbacks, downstream gets the agent's run dict enriched with `evaluations: { groundedness: {score, reason}, ... }`.

---

## Available evaluations

| Name | What it measures | Inputs |
|---|---|---|
| `answer_relevance` | Is the answer relevant to the question? | question + answer |
| `groundedness` | Is every factual claim in the answer supported by the context? | context + answer |
| `context_relevance` | Is the retrieved context relevant to the question? | question + context |
| `harmfulness` | Is anything in the response harmful? | answer |
| `helpfulness` | Does the response actually help the user? | question + answer |
| `coherence` | Is the response logically consistent? | answer |

Each runs one LLM-judge call with a curated prompt that returns `SCORE: 0-10` + `REASON: <one sentence>`. We normalize the score to `0.0–1.0` and surface it in materialization metadata as `score.<name>` so the Dagster UI can sort/filter by quality.

## Input mapping

By default the component reads from a standard `*_agent` output dict:

- `answer_jsonpath` = `final_answer` — the agent's last text response.
- `question_jsonpath` unset → auto-picks the first user message from `transcript`.

For RAG-shaped upstream (e.g. `rag_pipeline`), set:

```yaml
answer_jsonpath: response
question_jsonpath: query
context_jsonpath: retrieved_docs
```

For deeply nested keys, use dot-paths: `output.choices.0.message.content` (numeric indices into lists are not supported in v1 — flatten upstream if needed).

## Output

```python
{
    **upstream,        # original payload passed through
    "evaluations": {
        "answer_relevance": {"score": 0.9, "reason": "Directly answers the question with specifics."},
        "harmfulness":      {"score": 0.0, "reason": "No harmful content detected."},
        "helpfulness":      {"score": 0.8, "reason": "Practical and actionable."},
    },
}
```

## Example: evaluate a Dagster+ agent's daily summary

```yaml
type: dagster_component_templates.LLMEvaluatorComponent
attributes:
  asset_name: dagster_plus_agent_eval
  upstream_asset_key: dagster_plus_agent_run
  evaluations:
    - answer_relevance
    - helpfulness
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  group_name: ai
```

## When NOT to use this

| Need | Right tool |
|---|---|
| Full LLM-app tracing + dashboard | Use TruLens directly (outside Dagster) |
| Per-row enrichment via chat completion | `litellm_inference_asset` |
| Structured-output extraction | `litellm_structured_output` |
| Agent loop with tool calls | `litellm_agent` / `openai_agent` / etc. |

## Cost

One judge call per evaluation per materialization. With `gpt-4o-mini` (~$0.15 / 1M input + ~$0.60 / 1M output) and the default 256-token answer:
  - 1 evaluation ≈ $0.00005
  - 3 evaluations (recommended starter set) ≈ $0.00015
  - Daily at 1 run/day × 3 evals × 365 days ≈ $0.05/year per asset
