"""InferenceCostReportComponent.

Aggregates per-provider cost / latency / tokens (from an
`InferenceProviderABTestComponent`) + quality scores (from an optional
`ProviderABEvaluatorComponent`) into ONE report asset — the "should we
go local" answer that lives with the assets.

Emits a structured payload:

    {
      "per_provider": {
        alias: {"cost_usd", "latency_ms", "tokens_total", "quality_score",
                "cost_vs_baseline_pct", "quality_delta_vs_baseline"}
      },
      "baseline_alias",
      "winner_by_cost", "winner_by_quality", "winner_by_value" (quality/cost),
      "recommendation": "one-line finding",
      "projected_daily_savings_usd": <float | null>,
      "generated_at": "<ISO>",
    }

Materialize the A/B + evaluator + this report together on a schedule
(daily is right for most workloads) and the report becomes a time-series
in Insights — "cost curve for gpt-4o-mini triage over the last month"
alongside "quality curve for qwen-local", queryable + PR-linkable.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Same field surface as sibling components — the report co-partitions
    with the A/B + evaluator so per-partition reports become a time-series
    in Insights."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _pull_provider_metrics(payload: Any) -> Dict[str, Any]:
    """Extract cost/latency/token metrics from an A/B candidate asset payload."""
    if not isinstance(payload, dict):
        return {}
    return {
        "alias": payload.get("provider_alias") or payload.get("alias"),
        "model": payload.get("model"),
        "cost_usd": payload.get("cost_usd"),
        "latency_ms": payload.get("latency_ms"),
        "tokens_in": payload.get("tokens_in"),
        "tokens_out": payload.get("tokens_out"),
        "tokens_total": payload.get("tokens_total"),
        "error": payload.get("error"),
    }


def _pull_evaluator_scores(payload: Any) -> Dict[str, int]:
    """From an evaluator asset payload → {alias: score_0_100}."""
    if not isinstance(payload, dict):
        return {}
    scores = payload.get("scores") or {}
    out: Dict[str, int] = {}
    for alias, entry in scores.items():
        if isinstance(entry, dict):
            try:
                out[alias] = int(entry.get("score", 0))
            except (TypeError, ValueError):
                pass
    return out


def _format_currency(v: Optional[float]) -> str:
    if v is None:
        return "n/a"
    if v < 0.01:
        return f"${v * 1000:.3f}m"  # milli-dollars for micro-costs
    return f"${v:.4f}"


class InferenceCostReportComponent(dg.Component, dg.Model, dg.Resolvable):
    """Aggregate per-provider cost + latency + quality into a report asset.

    Standard shape for the "should we go local" story — pair with
    `InferenceProviderABTestComponent` (produces the candidate assets) and
    optionally `ProviderABEvaluatorComponent` (produces quality scores).
    Materialize on a daily schedule and the report becomes a time-series
    in Dagster+ Insights.
    """

    asset_name: str = Field(description="Report asset name.")
    candidates: List[str] = Field(
        description=(
            "Upstream A/B candidate asset keys. Each provides "
            "cost_usd + latency_ms + tokens_* in its payload."
        )
    )
    evaluator: Optional[str] = Field(
        default=None,
        description=(
            "Optional upstream evaluator asset key (from "
            "ProviderABEvaluatorComponent). Provides quality scores per "
            "provider — without it, the report has cost + latency only."
        )
    )
    baseline_alias: Optional[str] = Field(
        default=None,
        description=(
            "Alias of the reference provider. Deltas (cost, quality) are "
            "computed relative to this baseline. Typical: whichever "
            "provider is running in production today (e.g. gpt_4o_mini)."
        )
    )

    projected_daily_volume: Optional[int] = Field(
        default=None,
        description=(
            "Optional projected daily call volume. When set, the report "
            "computes `projected_daily_savings_usd` per non-baseline "
            "provider vs baseline. Useful for 'at our current rate this "
            "swap saves $N/mo' back-of-envelope math."
        )
    )

    quality_weight: float = Field(
        default=0.7,
        description=(
            "Weight (0-1) applied to quality in the value_score composite "
            "(cost gets 1 - quality_weight). Higher = quality matters more. "
            "The winner_by_value tie-breaks cost + quality."
        )
    )

    group_name: Optional[str] = Field(default="ab_test", description="Asset group.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['report', 'ab-test']."
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra tags.")
    description: Optional[str] = Field(default=None, description="Asset description.")

    # Partitioning — must match upstream candidate + evaluator partitions_def.
    partition_type: Optional[str] = Field(default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None.")
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partition types.")
    partition_values: Optional[Any] = Field(default=None, description="Comma-separated string OR list.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None,
        description="Multi-axis partition spec.")

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab."""
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Inference Cost Report", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        candidate_keys = list(self.candidates)
        evaluator_key = self.evaluator
        baseline_alias = self.baseline_alias
        projected_daily_volume = self.projected_daily_volume
        quality_weight = max(0.0, min(1.0, float(self.quality_weight)))
        description = self.description

        if not candidate_keys:
            raise ValueError("candidates must list at least one asset key.")

        kinds = self.kinds or ["report", "ab-test"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        ins: Dict[str, Any] = {
            f"c_{i}": dg.AssetIn(key=dg.AssetKey.from_user_string(k))
            for i, k in enumerate(candidate_keys)
        }
        if evaluator_key:
            ins["evaluator"] = dg.AssetIn(
                key=dg.AssetKey.from_user_string(evaluator_key)
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=description
            or f"Cost + quality report for providers: {candidate_keys}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            ins=ins,
            partitions_def=partitions_def,
        )
        def _report(context: dg.AssetExecutionContext, **inputs):
            # Extract per-provider metrics from candidate assets.
            per_provider: Dict[str, Dict[str, Any]] = {}
            for i, key in enumerate(candidate_keys):
                m = _pull_provider_metrics(inputs[f"c_{i}"])
                alias = m.get("alias") or key.split("/")[-1]
                per_provider[alias] = {
                    "model": m.get("model"),
                    "cost_usd": m.get("cost_usd"),
                    "latency_ms": m.get("latency_ms"),
                    "tokens_total": m.get("tokens_total"),
                    "tokens_in": m.get("tokens_in"),
                    "tokens_out": m.get("tokens_out"),
                    "quality_score": None,
                    "error": m.get("error"),
                }

            # Merge in evaluator quality scores if present.
            if "evaluator" in inputs:
                scores = _pull_evaluator_scores(inputs["evaluator"])
                for alias, s in scores.items():
                    if alias in per_provider:
                        per_provider[alias]["quality_score"] = s

            # Compute baseline-relative deltas.
            baseline_cost: Optional[float] = None
            baseline_quality: Optional[int] = None
            if baseline_alias and baseline_alias in per_provider:
                baseline_cost = per_provider[baseline_alias].get("cost_usd")
                baseline_quality = per_provider[baseline_alias].get("quality_score")

            for alias, entry in per_provider.items():
                if baseline_cost is not None and entry["cost_usd"] is not None and baseline_cost > 0:
                    pct = (entry["cost_usd"] - baseline_cost) / baseline_cost * 100.0
                    entry["cost_vs_baseline_pct"] = round(pct, 1)
                if baseline_quality is not None and entry["quality_score"] is not None:
                    entry["quality_delta_vs_baseline"] = (
                        entry["quality_score"] - baseline_quality
                    )

            # Pick winners.
            def _cost_or_inf(alias_entry):
                cost = alias_entry[1].get("cost_usd")
                return cost if cost is not None else float("inf")

            def _quality_or_neg(alias_entry):
                q = alias_entry[1].get("quality_score")
                return q if q is not None else -1

            live_items = [
                (a, e) for a, e in per_provider.items() if e.get("error") is None
            ]
            winner_by_cost = (
                min(live_items, key=_cost_or_inf)[0] if live_items else None
            )
            winner_by_quality = (
                max(live_items, key=_quality_or_neg)[0]
                if any(
                    e.get("quality_score") is not None for _, e in live_items
                )
                else None
            )

            # Composite value_score = quality_weight * q_norm + (1 - w) * cost_savings_norm.
            # Compute against `live_items` that have both quality and cost;
            # separate helper so Pyright doesn't gag on Optional-returning key.
            max_cost = max(
                (e.get("cost_usd") or 0.0) for _, e in live_items
            ) or 1.0
            valued: List[tuple] = []
            for alias, entry in live_items:
                q = entry.get("quality_score")
                cost = entry.get("cost_usd")
                if q is None or cost is None:
                    continue
                q_norm = q / 100.0
                cost_norm = 1.0 - (cost / max_cost) if max_cost > 0 else 1.0
                value_score = quality_weight * q_norm + (1 - quality_weight) * cost_norm
                valued.append((alias, value_score))
            winner_by_value = (
                max(valued, key=lambda p: p[1])[0] if valued else None
            )

            # Projected daily savings (per provider vs baseline).
            projected_savings: Dict[str, float] = {}
            if projected_daily_volume and baseline_cost is not None:
                for alias, entry in per_provider.items():
                    if alias == baseline_alias:
                        continue
                    cost = entry.get("cost_usd")
                    if cost is None:
                        continue
                    savings = (baseline_cost - cost) * projected_daily_volume
                    projected_savings[alias] = round(savings, 2)

            # Recommendation string.
            rec_parts: List[str] = []
            if winner_by_cost:
                rec_parts.append(f"cheapest: {winner_by_cost}")
            if winner_by_quality:
                rec_parts.append(f"highest quality: {winner_by_quality}")
            if winner_by_value and winner_by_value != winner_by_cost and winner_by_value != winner_by_quality:
                rec_parts.append(f"best value ({int(quality_weight*100)}% quality/{int((1-quality_weight)*100)}% cost): {winner_by_value}")
            recommendation = "; ".join(rec_parts) or "(no live providers to compare)"

            output_value = {
                "per_provider": per_provider,
                "baseline_alias": baseline_alias,
                "winner_by_cost": winner_by_cost,
                "winner_by_quality": winner_by_quality,
                "winner_by_value": winner_by_value,
                "quality_weight": quality_weight,
                "recommendation": recommendation,
                "projected_daily_volume": projected_daily_volume,
                "projected_daily_savings_usd": projected_savings or None,
                "generated_at": _now_iso(),
                "op": "inference_cost_report",
            }

            # Metadata: comparison table + savings summary.
            header = "| provider | cost | latency | tokens | quality | Δ cost | Δ quality |"
            sep = "|---|---|---|---|---|---|---|"
            rows: List[str] = [header, sep]
            for alias, entry in sorted(per_provider.items()):
                rows.append(
                    "| {a} | {c} | {l}ms | {tk} | {q} | {dc} | {dq} |".format(
                        a=alias + (" ⭐" if alias == baseline_alias else ""),
                        c=_format_currency(entry.get("cost_usd")),
                        l=entry.get("latency_ms") if entry.get("latency_ms") is not None else "n/a",
                        tk=entry.get("tokens_total") or "n/a",
                        q=entry.get("quality_score") if entry.get("quality_score") is not None else "n/a",
                        dc=(
                            f"{entry['cost_vs_baseline_pct']:+.1f}%"
                            if "cost_vs_baseline_pct" in entry
                            else "—"
                        ),
                        dq=(
                            f"{entry['quality_delta_vs_baseline']:+d}"
                            if "quality_delta_vs_baseline" in entry
                            else "—"
                        ),
                    )
                )

            md: Dict[str, Any] = {
                "recommendation": recommendation,
                "comparison_table": dg.MetadataValue.md("\n".join(rows)),
            }
            if winner_by_cost:
                md["winner_by_cost"] = dg.MetadataValue.text(winner_by_cost)
            if winner_by_quality:
                md["winner_by_quality"] = dg.MetadataValue.text(winner_by_quality)
            if winner_by_value:
                md["winner_by_value"] = dg.MetadataValue.text(winner_by_value)
            if projected_savings:
                md["projected_daily_savings_usd"] = dg.MetadataValue.json(projected_savings)
                if projected_daily_volume:
                    md["projected_daily_volume"] = dg.MetadataValue.int(
                        projected_daily_volume
                    )

            return dg.Output(output_value, metadata=md)

        return dg.Definitions(assets=[_report])
