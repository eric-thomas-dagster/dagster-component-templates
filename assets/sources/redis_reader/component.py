"""Redis Reader Component.

Read keys from a Redis instance by pattern, returning their values as a DataFrame.
Supports strings, hashes, lists, sets, and sorted sets.
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional
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


@dataclass
class RedisReaderComponent(Component, Model, Resolvable):
    """Component for reading keys from a Redis instance.

    Scans keys matching a pattern and returns their values as a DataFrame.
    Supports auto-detection of data types or explicit type specification.

    Example:
        ```yaml
        type: dagster_component_templates.RedisReaderComponent
        attributes:
          asset_name: redis_user_sessions
          key_pattern: "session:*"
          data_type: hash
          limit: 1000
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    host_env_var: str = Field(
        default="REDIS_HOST",
        description="Environment variable containing the Redis host address",
    )
    port: int = Field(
        default=6379,
        description="Redis port number",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the Redis password",
    )
    db: int = Field(
        default=0,
        description="Redis database number",
    )
    key_pattern: str = Field(
        default="*",
        description="Glob pattern for keys to fetch (e.g. 'session:*')",
    )
    data_type: str = Field(
        default="auto",
        description="Redis data type: 'auto', 'string', 'hash', 'list', 'set', 'zset'",
    )
    limit: Optional[int] = Field(
        default=None,
        description="Maximum number of keys to read",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage",
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization",
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
        host_env_var = self.host_env_var
        port = self.port
        password_env_var = self.password_env_var
        db = self.db
        key_pattern = self.key_pattern
        data_type = self.data_type
        limit = self.limit
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
        _comp_name = "redis_reader"  # component directory name
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
            description=f"Redis reader for pattern {key_pattern}",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def redis_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Read Redis keys matching pattern and return as DataFrame."""
            try:
                import redis as redis_lib
            except ImportError:
                raise ImportError("redis required: pip install redis")

            r = redis_lib.Redis(
                host=os.environ[host_env_var],
                port=port,
                password=os.environ.get(password_env_var) if password_env_var else None,
                db=db,
            )

            context.log.info(f"Fetching Redis keys matching pattern: {key_pattern}")
            keys = r.keys(key_pattern)
            if limit:
                keys = keys[:limit]

            rows = []
            for key in keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                dtype = r.type(key).decode() if data_type == "auto" else data_type

                if dtype == "string":
                    val = r.get(key)
                    rows.append({
                        "key": key_str,
                        "value": val.decode() if val else None,
                        "type": dtype,
                    })
                elif dtype == "hash":
                    val = {k.decode(): v.decode() for k, v in r.hgetall(key).items()}
                    rows.append({"key": key_str, **val, "type": dtype})
                elif dtype == "list":
                    val = [v.decode() for v in r.lrange(key, 0, -1)]
                    rows.append({"key": key_str, "value": str(val), "type": dtype})
                elif dtype in ("set", "zset"):
                    val = [
                        v.decode()
                        for v in (
                            r.smembers(key)
                            if dtype == "set"
                            else [m for m, _ in r.zscan_iter(key)]
                        )
                    ]
                    rows.append({"key": key_str, "value": str(val), "type": dtype})

            df = pd.DataFrame(rows)
            context.log.info(f"Retrieved {len(df)} keys from Redis matching {key_pattern}")

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
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
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


        _schema_checks = build_column_schema_change_checks(assets=[redis_reader_asset])


        return Definitions(assets=[redis_reader_asset], asset_checks=list(_schema_checks))
