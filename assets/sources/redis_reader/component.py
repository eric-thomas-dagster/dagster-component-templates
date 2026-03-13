"""Redis Reader Component.

Read keys from a Redis instance by pattern, returning their values as a DataFrame.
Supports strings, hashes, lists, sets, and sorted sets.
"""

import os
from dataclasses import dataclass
from typing import Optional, List
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

        @asset(
            name=asset_name,
            description=f"Redis reader for pattern {key_pattern}",
            partitions_def=partitions_def,
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
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "key_pattern": MetadataValue.text(key_pattern),
            })
            return df

        return Definitions(assets=[redis_reader_asset])
