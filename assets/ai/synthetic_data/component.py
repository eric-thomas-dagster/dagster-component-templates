"""Synthetic Data.

Generate synthetic data rows using an LLM based on a schema definition.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

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
class SyntheticDataComponent(Component, Model, Resolvable):
    """Generate synthetic data rows using an LLM based on a schema definition."""

    asset_name: str = Field(description="Output Dagster asset name")
    schema: Dict[str, Any] = Field(description="Column definitions e.g. {name: 'full name', age: 'integer 18-80'}")
    n_rows: int = Field(default=100, description="Total number of synthetic rows to generate")
    model: str = Field(default="gpt-4o-mini", description="LLM model identifier (passed to litellm)")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable name holding the API key")
    context: Optional[str] = Field(default=None, description="Business context to guide generation e.g. 'e-commerce customers'")
    batch_size: int = Field(default=25, description="Number of rows per LLM call")
    seed_data_asset_key: Optional[str] = Field(default=None, description="Optional upstream asset key for few-shot seed rows")
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
        return "Generate synthetic data rows using an LLM based on a schema definition."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        schema = self.schema
        n_rows = self.n_rows
        llm_model = self.model
        api_key_env_var = self.api_key_env_var
        context_str = self.context
        batch_size = self.batch_size
        seed_data_asset_key = self.seed_data_asset_key
                owners=owners,
        tags=_all_tags,
        freshness_policy=_freshness_policy,
group_name = self.group_name

        if seed_data_asset_key:
            ins = {"seed_data": AssetIn(key=AssetKey.from_user_string(seed_data_asset_key))}

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
        _comp_name = "synthetic_data"  # component directory name
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
                seed_data: pd.DataFrame,
            ) -> pd.DataFrame:
                return _generate_data(
                    context=context,
                    schema=schema,
                    n_rows=n_rows,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    context_str=context_str,
                    batch_size=batch_size,
                    seed_df=seed_data,
                )

        else:

            @asset(name=asset_name, deps=[], group_name=group_name)
            def _asset(context: AssetExecutionContext) -> pd.DataFrame:  # type: ignore[no-redef]
                return _generate_data(
                    context=context,
                    schema=schema,
                    n_rows=n_rows,
                    llm_model=llm_model,
                    api_key_env_var=api_key_env_var,
                    context_str=context_str,
                    batch_size=batch_size,
                    seed_df=None,
                )

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


def _generate_data(
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
    schema: dict,
    n_rows: int,
    llm_model: str,
    api_key_env_var: str,
    context_str: Optional[str],
    batch_size: int,
    seed_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    import json
    import os

    try:
        from litellm import completion
    except ImportError as e:
        raise ImportError("litellm is required: pip install litellm") from e

    api_key = os.environ.get(api_key_env_var)
    all_rows = []

    seed_example = ""
    if seed_df is not None and len(seed_df) > 0:
        sample = seed_df.head(3).to_dict(orient="records")
        seed_example = f"\nHere are example rows for reference:\n{json.dumps(sample, default=str)}\n"

    for batch_start in range(0, n_rows, batch_size):
        batch_n = min(batch_size, n_rows - batch_start)
        ctx_prefix = f"Context: {context_str}\n" if context_str else ""
        prompt = (
            f"{ctx_prefix}"
            f"Generate {batch_n} rows of synthetic data as a JSON array.{seed_example}"
            f"Schema: {json.dumps(schema)}\n"
            f"Return only a JSON array of objects with no additional explanation."
        )
        resp = completion(
            model=llm_model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
        rows = json.loads(raw)
        all_rows.extend(rows)

    result = pd.DataFrame(all_rows)


    # Build column schema metadata


    from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep


    _col_schema = TableSchema(columns=[


    TableColumn(name=str(col), type=str(result.dtypes[col]))


    for col in result.columns


    ])


    _metadata = {


    "dagster/row_count": MetadataValue.int(len(result)),


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

    return result
