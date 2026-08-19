# Inference Cost Report

Aggregate per-provider cost + latency + quality (from an
`InferenceProviderABTestComponent` and optional
`ProviderABEvaluatorComponent`) into ONE report asset — the "should we go
local" answer that lives with the assets, not in a slide deck.

Materialize on a daily schedule and the report becomes a **time-series in
Dagster+ Insights** — cost curves per provider, quality curves per
provider, projected daily savings — all queryable + PR-linkable.

## Fields

- **`asset_name`** *(required, string)* — Report asset name.
- **`candidates`** *(required, list[string])* — Upstream A/B candidate asset keys. Each provides `cost_usd + latency_ms + tokens_*` in its payload.
- **`evaluator`** *(optional, string)* — Upstream evaluator asset key. Provides quality scores. Without it, the report has cost + latency only.
- **`baseline_alias`** *(optional, string)* — Reference provider alias. Deltas (cost, quality) are computed relative to this.
- **`projected_daily_volume`** *(optional, int)* — Enables `projected_daily_savings_usd` per alternative. "At our current 10k calls/day this swap saves $X/mo."
- **`quality_weight`** *(optional, number, default 0.7)* — Composite value_score weighting. 0.7 = quality matters more than cost. 0.3 = cost matters more.
- **Partitioning** — Full support. Must match upstream candidate + evaluator partitions_def.

## Emitted payload

```json
{
  "per_provider": {
    "gpt_4o_mini": {
      "model": "gpt-4o-mini",
      "cost_usd": 0.0012,
      "latency_ms": 850,
      "tokens_total": 340,
      "quality_score": 82,
      "cost_vs_baseline_pct": 0.0,
      "quality_delta_vs_baseline": 0
    },
    "claude_haiku": {
      "cost_usd": 0.0038,
      "latency_ms": 1100,
      "quality_score": 89,
      "cost_vs_baseline_pct": 216.7,
      "quality_delta_vs_baseline": 7
    },
    "qwen_local": {
      "cost_usd": 0.0,
      "latency_ms": 2300,
      "quality_score": 76,
      "cost_vs_baseline_pct": -100.0,
      "quality_delta_vs_baseline": -6
    }
  },
  "baseline_alias": "gpt_4o_mini",
  "winner_by_cost": "qwen_local",
  "winner_by_quality": "claude_haiku",
  "winner_by_value": "qwen_local",
  "quality_weight": 0.7,
  "recommendation": "cheapest: qwen_local; highest quality: claude_haiku; best value (70% quality/30% cost): qwen_local",
  "projected_daily_volume": 10000,
  "projected_daily_savings_usd": {"claude_haiku": -26.0, "qwen_local": 12.0}
}
```

## Emitted metadata (Insights-friendly)

- Markdown comparison table (provider | cost | latency | tokens | quality | Δ cost | Δ quality) — rendered in the asset's Materializations tab
- `winner_by_cost`, `winner_by_quality`, `winner_by_value`
- `projected_daily_savings_usd` (JSON)
- `recommendation` (string)

## Value scoring

`value_score = quality_weight × (quality/100) + (1 - quality_weight) × (1 - cost/max_cost)`

Both dimensions normalized to 0-1; higher = better. Winner-by-value is the provider that maximizes this composite. Tune `quality_weight` to bias toward one dimension: 0.9 = quality-first, 0.3 = aggressive cost-first.

## Example

```yaml
type: dagster_community_components.InferenceCostReportComponent
attributes:
  asset_name: triage_ab_report
  group_name: local_ai_ab

  candidates:
    - triage_ab_gpt_4o_mini
    - triage_ab_claude_haiku
    - triage_ab_qwen_local

  evaluator: triage_ab_scored

  baseline_alias: gpt_4o_mini
  projected_daily_volume: 10000
  quality_weight: 0.7
```

Materialize daily, browse the asset in Insights, get your "should we go local" curve without ever running a benchmark by hand.
