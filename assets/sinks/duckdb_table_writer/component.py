"""DuckDB Table Writer Asset Component."""

from typing import Dict, List, Literal, Optional
import pandas as pd
from pathlib import Path
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
)
from pydantic import Field


class DuckDBTableWriterComponent(Component, Model, Resolvable):
    """
    Component for writing pandas DataFrames to DuckDB tables.

    This component reads data from an upstream asset (as a DataFrame) and writes
    it to a DuckDB database table. Perfect for persisting synthetic data, API
    responses, or transformed data to a local database.

    Features:
    - Automatic table creation from DataFrame schema
    - Support for create/replace/append modes
    - Local DuckDB file storage (no server setup needed)
    - Efficient columnar storage
    - Full SQL query support on written data
    """

    asset_name: str = Field(description="Name of the asset to create")

    database_path: str = Field(
        default="data.duckdb",
        description="Path to the DuckDB database file (will be created if doesn't exist)"
    )

    table_name: str = Field(
        description="Name of the table to write data to"
    )

    write_mode: Literal["create", "replace", "append"] = Field(
        default="replace",
        description="Write mode: 'create' (fail if exists), 'replace' (drop and recreate), 'append' (add rows)"
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
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

    upstream_asset_keys: Optional[str] = Field(
        default=None,
        description='Comma-separated list of upstream asset keys to load DataFrames from (automatically set by custom lineage)'
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the DuckDB table writer."""

        # Capture fields for closure
        asset_name = self.asset_name
        database_path = self.database_path
        table_name = self.table_name
        write_mode = self.write_mode
        description = self.description or f"Write data to DuckDB table {table_name}"
        group_name = self.group_name or None
        upstream_asset_keys_str = self.upstream_asset_keys

        # Parse upstream asset keys if provided
        upstream_keys = []
        if upstream_asset_keys_str:
            upstream_keys = [k.strip() for k in upstream_asset_keys_str.split(',')]

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
        _comp_name = "duckdb_table_writer"  # component directory name
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
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def duckdb_writer_asset(context: AssetExecutionContext, **kwargs) -> None:
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
            """Write DataFrame to DuckDB table."""
            import duckdb

            # Load upstream assets based on configuration
            upstream_assets = {}

            # If upstream_asset_keys is configured, try to load assets explicitly
            if upstream_keys and hasattr(context, 'load_asset_value'):
                # Real execution context - load assets explicitly
                context.log.info(f"Loading {len(upstream_keys)} upstream asset(s) via context.load_asset_value()")
                for key in upstream_keys:
                    try:
                        # Convert string key to AssetKey if needed
                        asset_key = AssetKey(key) if isinstance(key, str) else key
                        value = context.load_asset_value(asset_key)
                        upstream_assets[key] = value
                        context.log.info(f"  - Loaded '{key}': {type(value).__name__}")
                    except Exception as e:
                        context.log.error(f"  - Failed to load '{key}': {e}")
                        raise
            else:
                # Preview/mock context or no upstream_keys - fall back to kwargs
                upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"DuckDB Table Writer '{asset_name}' requires at least one upstream asset "
                    "that produces a DataFrame. Connect an upstream asset using the custom lineage UI."
                )

            # Get the first (and should be only) upstream DataFrame
            df = list(upstream_assets.values())[0]

            if not isinstance(df, pd.DataFrame):
                raise ValueError(
                    f"Expected DataFrame from upstream asset, got {type(df)}"
                )

            context.log.info(
                f"Writing {len(df)} rows to DuckDB table '{table_name}' "
                f"at {database_path}"
            )

            # Ensure database directory exists
            db_path = Path(database_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Connect to DuckDB
            con = duckdb.connect(str(db_path))

            try:
                # Check if table exists
                table_exists = con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_name = '{table_name}'"
                ).fetchone()[0] > 0

                if write_mode == "create":
                    if table_exists:
                        raise ValueError(
                            f"Table {table_name} already exists and write_mode is 'create'"
                        )
                    # Create new table
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    context.log.info(f"Created new table {table_name}")

                elif write_mode == "replace":
                    if table_exists:
                        con.execute(f"DROP TABLE {table_name}")
                        context.log.info(f"Dropped existing table {table_name}")
                    # Create table with data
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    context.log.info(f"Created table {table_name}")

                elif write_mode == "append":
                    if not table_exists:
                        # Create table if it doesn't exist
                        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        context.log.info(
                            f"Created new table {table_name} (table didn't exist)"
                        )
                    else:
                        # Append to existing table
                        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                        context.log.info(f"Appended {len(df)} rows to {table_name}")

                # Log final row count
                row_count = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                context.log.info(
                    f"Table {table_name} now has {row_count} total rows"
                )

            finally:
                con.close()

        return Definitions(assets=[duckdb_writer_asset])
