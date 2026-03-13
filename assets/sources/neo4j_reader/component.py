"""Neo4j Reader Component.

Execute a Cypher query against a Neo4j graph database,
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
class Neo4jReaderComponent(Component, Model, Resolvable):
    """Component for executing Cypher queries against a Neo4j graph database.

    Connects to Neo4j, runs a Cypher query, and returns
    results as a DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.Neo4jReaderComponent
        attributes:
          asset_name: neo4j_people
          query: "MATCH (n:Person) RETURN n.name AS name, n.age AS age LIMIT 100"
          group_name: sources
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    uri_env_var: str = Field(
        default="NEO4J_URI",
        description="Environment variable containing the Neo4j connection URI",
    )
    username_env_var: str = Field(
        default="NEO4J_USERNAME",
        description="Environment variable containing the Neo4j username",
    )
    password_env_var: str = Field(
        default="NEO4J_PASSWORD",
        description="Environment variable containing the Neo4j password",
    )
    query: str = Field(
        description="Cypher query to execute (e.g. 'MATCH (n:Person) RETURN n.name, n.age LIMIT 100')"
    )
    database: Optional[str] = Field(
        default=None,
        description="Neo4j database name (None = default database)",
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
        uri_env_var = self.uri_env_var
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
        query = self.query
        database = self.database
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
            description="Neo4j Cypher query results",
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def neo4j_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Execute Cypher query against Neo4j and return results as DataFrame."""
            try:
                from neo4j import GraphDatabase
            except ImportError:
                raise ImportError("neo4j required: pip install neo4j")

            uri = os.environ[uri_env_var]
            context.log.info(f"Connecting to Neo4j at {uri}")

            driver = GraphDatabase.driver(
                uri,
                auth=(os.environ[username_env_var], os.environ[password_env_var]),
            )

            context.log.info(f"Executing Cypher: {query}")
            with driver.session(database=database) as session:
                result = session.run(query)
                records = [dict(record) for record in result]
            driver.close()

            df = pd.DataFrame(records)
            context.log.info(f"Retrieved {len(df)} records from Neo4j")
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "database": MetadataValue.text(database or "default"),
            })
            return df

        return Definitions(assets=[neo4j_reader_asset])
