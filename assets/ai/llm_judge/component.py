"""LLM Judge Component.

Evaluate and score LLM outputs against a rubric using another LLM as a judge.
Useful for automated quality assessment and regression testing of AI pipelines.
"""
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class LlmJudgeComponent(Component, Model, Resolvable):
    """Evaluate and score LLM outputs against a rubric using another LLM as a judge.

    Builds a structured evaluation prompt presenting the response (and optional reference),
    asks the judge model to score each criterion, and returns JSON with score (0-10) and reason.

    Example:
        ```yaml
        type: dagster_component_templates.LlmJudgeComponent
        attributes:
          asset_name: evaluated_responses
          upstream_asset_key: llm_responses
          response_column: response
          reference_column: ground_truth
          criteria:
            - accuracy
            - clarity
            - completeness
          model: gpt-4o
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    response_column: str = Field(description="Column containing LLM responses to evaluate")
    reference_column: Optional[str] = Field(default=None, description="Column containing reference/ground truth (if available)")
    criteria: List[str] = Field(description='Evaluation criteria e.g. ["accuracy", "clarity", "completeness"]')
    score_column: str = Field(default="judge_score", description="Column to write numeric score (0-10)")
    reason_column: str = Field(default="judge_reason", description="Column to write explanation")
    model: str = Field(default="gpt-4o", description="Judge model (use a strong model for reliable evaluation)")
    rubric: Optional[str] = Field(default=None, description="Custom scoring rubric to guide the judge")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var name for API key")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        response_column = self.response_column
        reference_column = self.reference_column
        criteria = self.criteria
        score_column = self.score_column
        reason_column = self.reason_column
        model = self.model
        rubric = self.rubric
        api_key_env_var = self.api_key_env_var
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            kinds={"ai", "evaluation"},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                import litellm
            except ImportError:
                raise ImportError("pip install litellm>=1.0.0")

            import os
            import json

            df = upstream.copy()
            context.log.info(f"Evaluating {len(df)} responses with judge model={model}")

            kwargs: dict = {
                "model": model,
                "response_format": {"type": "json_object"},
            }
            if api_key_env_var:
                kwargs["api_key"] = os.environ[api_key_env_var]

            criteria_str = "\n".join(f"- {c}" for c in criteria)

            scores = []
            reasons = []

            for _, row in df.iterrows():
                response_text = str(row[response_column])

                prompt_parts = [
                    "You are an objective evaluator. Score the following response on each criterion and provide an overall score.",
                    "",
                    f"Criteria to evaluate:\n{criteria_str}",
                ]

                if rubric:
                    prompt_parts.extend(["", f"Scoring rubric:\n{rubric}"])

                if reference_column and reference_column in row.index:
                    reference_text = str(row[reference_column])
                    prompt_parts.extend([
                        "",
                        f"Reference/Ground Truth:\n{reference_text}",
                    ])

                prompt_parts.extend([
                    "",
                    f"Response to evaluate:\n{response_text}",
                    "",
                    "Respond with a JSON object containing:",
                    '  "score": overall numeric score from 0-10 (average across all criteria)',
                    '  "criterion_scores": object with each criterion name as key and score (0-10) as value',
                    '  "reason": one or two sentence explanation of the scores',
                ])

                user_content = "\n".join(prompt_parts)
                messages = [
                    {"role": "system", "content": "You are an expert evaluator. Always respond with valid JSON."},
                    {"role": "user", "content": user_content},
                ]

                try:
                    response = litellm.completion(messages=messages, **kwargs                api_key=api_key,
            )
                    content = response.choices[0].message.content or "{}"
                    parsed = json.loads(content)
                    scores.append(float(parsed.get("score", 0)))
                    reasons.append(parsed.get("reason", ""))
                except Exception as e:
                    context.log.warning(f"Judge evaluation failed for row: {e}")
                    scores.append(None)
                    reasons.append(f"Error: {e}")

            df[score_column] = scores
            df[reason_column] = reasons

            valid_scores = [s for s in scores if s is not None]
            avg_score = sum(valid_scores) / len(valid_scores) if valid_scores else 0.0

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "average_score": MetadataValue.float(avg_score),
                "model": MetadataValue.text(model),
                "criteria": MetadataValue.text(", ".join(criteria)),
            })
            return df

        return Definitions(assets=[_asset])
