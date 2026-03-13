"""Cassandra Reader Component.

Execute a CQL query against an Apache Cassandra keyspace,
returning results as a DataFrame.
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
class CassandraReaderComponent(Component, Model, Resolvable):
    """Component for executing CQL queries against an Apache Cassandra keyspace.

    Connects to a Cassandra cluster, runs a CQL query, and returns
    results as a DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.CassandraReaderComponent
        attributes:
          asset_name: cassandra_events
          hosts:
            - cassandra.example.com
          keyspace: analytics
          query: "SELECT * FROM events LIMIT 10000"
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    hosts: List[str] = Field(
        default=["localhost"],
        description="List of Cassandra host addresses",
    )
    port: int = Field(
        default=9042,
        description="Cassandra port number",
    )
    keyspace: str = Field(description="Cassandra keyspace name")
    query: str = Field(
        description="CQL query to execute (e.g. 'SELECT * FROM users LIMIT 1000')"
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the Cassandra username",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable containing the Cassandra password",
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
        hosts = self.hosts
        port = self.port
        keyspace = self.keyspace
        query = self.query
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
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
            description=f"Cassandra query on keyspace {keyspace}",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def cassandra_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Execute CQL query against Cassandra and return results as DataFrame."""
            try:
                from cassandra.cluster import Cluster
                from cassandra.auth import PlainTextAuthProvider
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

            context.log.info(f"Executing CQL: {query}")
            rows = session.execute(query)
            df = pd.DataFrame(list(rows))

            context.log.info(f"Retrieved {len(df)} rows from Cassandra keyspace {keyspace}")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "keyspace": MetadataValue.text(keyspace),
            })
            return df

        return Definitions(assets=[cassandra_reader_asset])
