"""Cassandra Writer Component.

Write a DataFrame to an Apache Cassandra table using INSERT CQL statements.
Supports insert and update (using IF EXISTS / USING TIMESTAMP) modes.
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
class CassandraWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to an Apache Cassandra table.

    Builds INSERT CQL statements from DataFrame columns and executes them
    against a Cassandra keyspace. Supports insert and upsert modes.

    Example:
        ```yaml
        type: dagster_component_templates.CassandraWriterComponent
        attributes:
          asset_name: write_events_to_cassandra
          upstream_asset_key: processed_events
          hosts:
            - localhost
          keyspace: analytics
          table: events
          if_exists: insert
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
    hosts: List[str] = Field(
        default=["localhost"],
        description="List of Cassandra host addresses",
    )
    port: int = Field(
        default=9042,
        description="Cassandra port number",
    )
    keyspace: str = Field(description="Cassandra keyspace name")
    table: str = Field(description="Cassandra table name to write to")
    if_exists: str = Field(
        default="insert",
        description="Write mode: 'insert' (INSERT INTO) or 'update' (INSERT INTO ... IF NOT EXISTS replaces with upsert semantics)",
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the Cassandra username",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the Cassandra password",
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
        hosts = self.hosts
        port = self.port
        keyspace = self.keyspace
        table = self.table
        if_exists = self.if_exists
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
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
        _comp_name = "cassandra_writer"  # component directory name
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
            description=f"Write DataFrame to Cassandra {keyspace}.{table}",
        )
        def cassandra_writer_asset(
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
            """Write DataFrame rows to Cassandra table using INSERT CQL."""
            try:
                from cassandra.cluster import Cluster
                from cassandra.auth import PlainTextAuthProvider
                from cassandra.query import BatchStatement
            except ImportError:
                raise ImportError("cassandra-driver required: pip install cassandra-driver")

            auth = None
            if username_env_var:
                auth = PlainTextAuthProvider(
                    os.environ[username_env_var],
                    os.environ[password_env_var],
                )

            context.log.info(f"Connecting to Cassandra at {hosts} keyspace {keyspace}")
            cluster = Cluster(hosts, port=port, auth_provider=auth)
            session = cluster.connect(keyspace)

            records = upstream.to_dict(orient="records")
            if not records:
                context.log.info("No records to write.")
                return MaterializeResult(
                    metadata={"row_count": MetadataValue.int(0)
                "dagster/row_count": MetadataValue.int(len(upstream)),}
                )

            columns = list(upstream.columns)
            placeholders = ", ".join(["%s"] * len(columns))
            col_list = ", ".join(columns)

            if if_exists == "update":
                # Cassandra INSERT is always an upsert (no true insert-only without IF NOT EXISTS)
                cql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
            else:
                cql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

            context.log.info(
                f"Writing {len(records)} rows to Cassandra {keyspace}.{table} (mode: {if_exists})"
            )

            prepared = session.prepare(cql)
            batch_size = 100
            total_written = 0
            for i in range(0, len(records), batch_size):
                batch = BatchStatement()
                chunk = records[i : i + batch_size]
                for row in chunk:
                    values = tuple(row.get(col) for col in columns)
                    batch.add(prepared, values)
                session.execute(batch)
                total_written += len(chunk)

            context.log.info(f"Successfully wrote {total_written} rows to Cassandra {keyspace}.{table}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "keyspace": MetadataValue.text(keyspace),
                    "table": MetadataValue.text(table),
                }
            )

        return Definitions(assets=[cassandra_writer_asset])
