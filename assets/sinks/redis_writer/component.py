"""Redis Writer Component.

Write a DataFrame to Redis as hashes, strings, or list values.
Each row is keyed by a specified column value.
"""

import os
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
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class RedisWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to Redis.

    Writes each DataFrame row to Redis as a hash (HSET), string (SET),
    or appended list item (RPUSH), keyed by a specified column value.

    Example:
        ```yaml
        type: dagster_component_templates.RedisWriterComponent
        attributes:
          asset_name: write_sessions_to_redis
          upstream_asset_key: computed_sessions
          key_column: session_id
          write_mode: hash
          expire_seconds: 3600
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
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
    key_column: str = Field(
        description="DataFrame column to use as the Redis key"
    )
    write_mode: str = Field(
        default="hash",
        description="Write mode: 'hash' (HSET), 'string' (SET serialized row), or 'list' (RPUSH value column)",
    )
    expire_seconds: Optional[int] = Field(
        default=None,
        description="Key expiration in seconds (None = no expiration)",
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        host_env_var = self.host_env_var
        port = self.port
        password_env_var = self.password_env_var
        db = self.db
        key_column = self.key_column
        write_mode = self.write_mode
        expire_seconds = self.expire_seconds
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
        _comp_name = "redis_writer"  # component directory name
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
            description=f"Write DataFrame to Redis (mode: {write_mode})",
        )
        def redis_writer_asset(
            context: AssetExecutionContext, upstream: pd.DataFrame
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
        ) -> MaterializeResult:
            """Write DataFrame rows to Redis using the specified write mode."""
            try:
                import redis as redis_lib
                import json
            except ImportError:
                raise ImportError("redis required: pip install redis")

            r = redis_lib.Redis(
                host=os.environ[host_env_var],
                port=port,
                password=os.environ.get(password_env_var) if password_env_var else None,
                db=db,
            )

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Writing {len(records)} rows to Redis (mode: {write_mode}, key_column: {key_column})"
            )

            for row in records:
                key = str(row[key_column])
                if write_mode == "hash":
                    # Write all fields as a Redis hash
                    mapping = {
                        str(k): str(v) for k, v in row.items() if v is not None
                    }
                    r.hset(key, mapping=mapping)
                elif write_mode == "string":
                    # Serialize entire row as JSON string
                    r.set(key, json.dumps(row, default=str))
                elif write_mode == "list":
                    # Append value (all fields except key as JSON) to a list
                    value = {k: v for k, v in row.items() if k != key_column}
                    r.rpush(key, json.dumps(value, default=str))
                else:
                    raise ValueError(
                        f"Invalid write_mode='{write_mode}'. Use 'hash', 'string', or 'list'."
                    )

                if expire_seconds:
                    r.expire(key, expire_seconds)

            context.log.info(f"Successfully wrote {len(records)} keys to Redis")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "write_mode": MetadataValue.text(write_mode),
                    "key_column": MetadataValue.text(key_column),
                
                    "dagster/row_count": MetadataValue.int(len(upstream)),}
            )

        return Definitions(assets=[redis_writer_asset])
