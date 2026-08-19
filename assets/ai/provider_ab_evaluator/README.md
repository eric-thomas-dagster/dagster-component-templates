# Provider A/B Evaluator (LLM-as-judge)

LLM-as-judge that scores N candidate outputs against a rubric in ONE pass. Pairs with `InferenceProviderABTestComponent` (upstream) to close the "should we go local" loop with a quality signal — cost + latency without quality is half the picture.

## Why one pass, not N

Separate calls per candidate suffer from **judge-drift**: the same rubric criterion applied to the same output in two separate calls can yield noticeably different scores. Running all candidates through one judge call keeps their scores directly comparable because they share the same judgment context.

## When to use

- Downstream of `InferenceProviderABTestComponent` when you need a quality dimension alongside cost + latency.
- As a branch-deploy merge gate (set `min_winner_score: 70` → the emitted `winner_meets_threshold` asset check gates promotion).
- For any set of N candidate LLM outputs where cross-candidate quality comparison matters (single-candidate quality analysis → use `LLMEvaluatorComponent` instead).

## Fields

- **`asset_name`** *(required, string)* — Name of the emitted asset.
- **`candidates`** *(required, list[string])* — Upstream asset keys to score. Each candidate's text is loaded from the standard `{text, content, ...}` shape.
- **`rubric`** *(required, object)* — Scoring rubric. `{kind: literal, text: '...'}` | `{kind: file, path: '...'}` | `{kind: url, url: '...'}`. Include specific criteria + point weights.
- **`judge`** *(required, object)* — Judge model config: `{model, api_key_env_var, [api_base_env_var, system_prompt, temperature, max_tokens]}`. Use a strong model (gpt-4o, claude-3-5-sonnet) — the judge sets the ceiling on evaluation quality.
- **`min_winner_score`** *(optional, int, 0-100)* — Emits an asset check `winner_meets_threshold`; fails ERROR when the winner's score drops below this. Branch-deploy merge gate.
- **`baseline_alias`** *(optional, string)* — Reference candidate alias for delta metrics. Deltas surface how much a swap moved quality vs baseline.
- **Partitioning** — Full `partition_type` / `partition_start` / etc. support. Must match upstream candidates' partitions_def.

## Emitted payload

```json
{
  "scores": {
    "gpt_4o_mini":  {"score": 82, "reasoning": "correctly classified..."},
    "claude_haiku": {"score": 89, "reasoning": "cites specific..."},
    "qwen_local":   {"score": 76, "reasoning": "missed docs-gap..."}
  },
  "winner": "claude_haiku",
  "winner_score": 89,
  "baseline_alias": "gpt_4o_mini",
  "delta_vs_baseline": {"claude_haiku": 7, "qwen_local": -6},
  "judge_model": "gpt-4o",
  "judge_cost_usd": 0.008,
  "judge_latency_ms": 3200
}
```

## Merge-gate pattern

With `min_winner_score: 70`:

```
InferenceProviderABTestComponent (partitioned per prompt)
     └── ProviderABEvaluatorComponent (min_winner_score: 70)
           └── asset_check: winner_meets_threshold
                 └── downstream job (only fires when check passes)
```

Wire the asset check into a branch-deploy CI step (`dagster asset materialize --select ...` in the PR runner) — the check FAILS ERROR when quality drops, blocking the merge automatically.

## Example

```yaml
type: dagster_community_components.ProviderABEvaluatorComponent
attributes:
  asset_name: triage_ab_scored

  candidates:
    - triage_ab_gpt_4o_mini
    - triage_ab_claude_haiku
    - triage_ab_qwen_local

  rubric:
    kind: literal
    text: |
      Score each candidate on 0-100:
      1. Classification accuracy (40 pts)
      2. Rationale grounding (30 pts)
      3. Next-action concreteness (30 pts)

  judge:
    model: gpt-4o
    api_key_env_var: OPENAI_API_KEY

  baseline_alias: gpt_4o_mini
  min_winner_score: 70
```
