"""Precision Match.

LLM-assisted fuzzy matching to standardize varied string representations to canonical forms.
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
class PrecisionMatchComponent(Component, Model, Resolvable):
    """LLM-assisted fuzzy matching to standardize varied string representations to canonical forms."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with values to standardize")
    column: str = Field(description="Column with varied strings to standardize")
    reference_asset_key: Optional[str] = Field(default=None, description="Optional upstream asset key for a reference DataFrame containing canonical values")
    reference_column: Optional[str] = Field(default=None, description="Column in the reference DataFrame containing canonical values")
    reference_values: Optional[List[str]] = Field(default=None, description="Explicit list of canonical values (alternative to reference_asset_key)")
    model: str = Field(default="gpt-4o-mini", description="LLM model identifier (passed to litellm)")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable name holding the API key")
    output_column: str = Field(default="matched_value", description="Column name for the matched canonical value")
    confidence_column: Optional[str] = Field(default="match_confidence", description="Column name for match confidence score (None to skip)")
    batch_size: int = Field(default=20, description="Number of unique values to match per LLM call")
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

    @classmethod
    def get_description(cls) -> str:
        return "LLM-assisted fuzzy matching to standardize varied string representations to canonical forms."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        column = self.column
        reference_asset_key = self.reference_asset_key
        reference_column = self.reference_column
        reference_values = self.reference_values
        llm_model = self.model
        api_key_env_var = self.api_key_env_var
        output_column = self.output_column
        confidence_column = self.confidence_column
        batch_size = self.batch_size
                owners=owners,
        tags=_all_tags,
        freshness_policy=_freshness_policy,
group_name = self.group_name

        if reference_asset_key:
            ins = {
                "upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key)),
                "reference": AssetIn(key=AssetKey.from_user_string(reference_asset_key)),
            }

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

        partitions_def=partitions_def,
        # Infer kinds from component name if not explicitly set
        _comp_name = "precision_match"  # component directory name
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


        @asset(name=asset_name, ins=ins, group_name=group_name)
            def _asset(
                context: AssetExecutionContext,
                upstream: pd.DataFrame,
                reference: pd.DataFrame,
            ) -> pd.DataFrame:
                return _run_matching(
                    context=context,
                    df=upstream,
                    column=column,
                    canonical=reference[reference_column].dropna().unique().tolist(),
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    output_column=output_column,
                    confidence_column=confidence_column,
                    batch_size=batch_size,
                )

        else:
            ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

            @asset(name=asset_name, ins=ins, group_name=group_name)
            def _asset(  # type: ignore[no-redef]
                context: AssetExecutionContext,
                upstream: pd.DataFrame,
            ) -> pd.DataFrame:
                canonical = reference_values or []
                return _run_matching(
                    context=context,
                    df=upstream,
                    column=column,
                    canonical=canonical,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    output_column=output_column,
                    confidence_column=confidence_column,
                    batch_size=batch_size,
                )

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


def _run_matching(
    context: AssetExecutionContext,
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
    df: pd.DataFrame,
    column: str,
    canonical: list,
    llm_model: str,
    api_key_env_var: str,
    output_column: str,
    confidence_column: Optional[str],
    batch_size: int,
) -> pd.DataFrame:
    import json
    import os

    try:
        from litellm import completion
    except ImportError as e:
        raise ImportError("litellm is required: pip install litellm") from e

    df = df.copy()
    unique_vals = df[column].dropna().unique().tolist()
    api_key = os.environ.get(api_key_env_var)

    mapping: dict = {}
    confidence_map: dict = {}

    for i in range(0, len(unique_vals), batch_size):
        batch = unique_vals[i : i + batch_size]
        prompt = (
            f"Match each input string to the closest canonical value. "
            f"Return a JSON object mapping each input to its matched canonical value "
            f"and a confidence score between 0 and 1.\n"
            f"Format: {{\"input_value\": {{\"match\": \"canonical_value\", \"confidence\": 0.95}}, ...}}\n"
            f"Canonical values: {json.dumps(canonical)}\n"
            f"Inputs to match: {json.dumps(batch)}"
        )
        resp = completion(
            model=llm_model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
        )
        raw = resp.choices[0].message.content
        # Strip markdown code fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
        result = json.loads(raw)
        for val, info in result.items():
            if isinstance(info, dict):
                mapping[val] = info.get("match", val)
                confidence_map[val] = info.get("confidence", None)
            else:
                mapping[val] = info

    df[output_column] = df[column].map(mapping)
    if confidence_column:
        df[confidence_column] = df[column].map(confidence_map)

    matched = df[output_column].notna().sum()

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

    # Add column lineage if defined

    if column_lineage:

    _upstream_key = AssetKey.from_user_string(upstream_asset_key) if 'upstream_asset_key' in dir() else None

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
