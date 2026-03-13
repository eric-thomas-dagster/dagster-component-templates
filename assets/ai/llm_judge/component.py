"""LLM Judge Component.

Evaluate and score LLM outputs against a rubric using another LLM as a judge.
Useful for automated quality assessment and regression testing of AI pipelines.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
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

        # Infer kinds from component name if not explicitly set
        _comp_name = "llm_judge"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
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


            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
