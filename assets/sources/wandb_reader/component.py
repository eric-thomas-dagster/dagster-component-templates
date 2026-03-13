"""W&B Reader Component.

Fetch Weights & Biases (W&B) experiment runs and their config, summary metrics,
and metadata as a Dagster asset DataFrame.
"""

import os
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import pandas as pd
from dagster import (
    AssetExecutionContext,
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


class WandbReaderComponent(Component, Model, Resolvable):
    """Component for fetching W&B experiment runs and metrics.

    Retrieves run metadata, config parameters, and summary metrics from a
    Weights & Biases project and returns them as a flattened DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.WandbReaderComponent
        attributes:
          asset_name: experiment_runs
          project: my-ml-project
          entity: my-team
          filters:
            state: finished
          n_runs: 50
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    project: str = Field(description="W&B project name")

    entity: Optional[str] = Field(
        default=None,
        description="W&B entity/username (None = use default from API key)"
    )

    api_key_env_var: str = Field(
        default="WANDB_API_KEY",
        description="Environment variable containing W&B API key"
    )

    filters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Run filters (e.g., {\"state\": \"finished\"})"
    )

    n_runs: int = Field(
        default=100,
        description="Maximum number of runs to fetch"
    )

    include_history: bool = Field(
        default=False,
        description="Include training history step count for each run"
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        project = self.project
        entity = self.entity
        api_key_env_var = self.api_key_env_var
        filters = self.filters
        n_runs = self.n_runs
        include_history = self.include_history
        deps = self.deps
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
        _comp_name = "wandb_reader"  # component directory name
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
            description=f"W&B runs from project {project}",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def wandb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Fetch W&B experiment runs and metrics as a DataFrame."""
            try:
                import wandb
            except ImportError:
                raise ImportError("wandb is required: pip install wandb")

            api = wandb.Api(api_key=os.environ.get(api_key_env_var))

            project_path = f"{entity}/{project}" if entity else project
            context.log.info(f"Fetching up to {n_runs} runs from W&B project '{project_path}'")

            runs = api.runs(project_path, filters=filters, per_page=n_runs)

            rows = []
            for run in runs:
                row: Dict[str, Any] = {
                    "run_id": run.id,
                    "run_name": run.name,
                    "state": run.state,
                    "created_at": run.created_at,
                }
                # Flatten config
                row.update({f"config_{k}": v for k, v in run.config.items()})
                # Flatten summary metrics (skip internal W&B keys)
                row.update({
                    f"summary_{k}": v
                    for k, v in run.summary.items()
                    if not k.startswith("_")
                })
                if include_history:
                    try:
                        history = run.history()
                        row["history_steps"] = len(history) if history is not None else 0
                    except Exception:
                        row["history_steps"] = 0
                rows.append(row)

            df = pd.DataFrame(rows)

            context.log.info(f"Fetched {len(df)} runs from W&B project '{project_path}'")

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


        _schema_checks = build_column_schema_change_checks(assets=[wandb_reader_asset])


        return Definitions(assets=[wandb_reader_asset], asset_checks=list(_schema_checks))
