"""Neo4j Writer Component.

Write a DataFrame to Neo4j as graph nodes using Cypher MERGE or CREATE statements.
Each DataFrame row becomes a node with the specified label.
"""

import os
from dataclasses import dataclass
from typing import Optional
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
class Neo4jWriterComponent(Component, Model, Resolvable):
    """Component for writing a DataFrame to Neo4j as graph nodes.

    Creates or merges nodes in a Neo4j graph database from DataFrame rows,
    using a specified node label and optional identity column for MERGE.

    Example:
        ```yaml
        type: dagster_component_templates.Neo4jWriterComponent
        attributes:
          asset_name: write_people_to_neo4j
          upstream_asset_key: processed_people
          node_label: Person
          id_column: person_id
          merge: true
          group_name: sinks
        ```
    """

    asset_name: str = Field(description="Name of the output asset to create")
    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame asset"
    )
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
    node_label: str = Field(description="Neo4j node label to assign to created nodes")
    id_column: str = Field(
        description="DataFrame column to use as the node identity property for MERGE"
    )
    merge: bool = Field(
        default=True,
        description="If True, use MERGE to create or update nodes; if False, use CREATE",
    )
    database: Optional[str] = Field(
        default=None,
        description="Neo4j database name (None = default database)",
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
        upstream_asset_key = self.upstream_asset_key
        uri_env_var = self.uri_env_var
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
        node_label = self.node_label
        id_column = self.id_column
        merge = self.merge
        database = self.database
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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            description=f"Write DataFrame to Neo4j as {node_label} nodes",
        )
        def neo4j_writer_asset(
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
            """Write DataFrame rows to Neo4j as graph nodes."""
            try:
                from neo4j import GraphDatabase
            except ImportError:
                raise ImportError("neo4j required: pip install neo4j")

            uri = os.environ[uri_env_var]
            driver = GraphDatabase.driver(
                uri,
                auth=(os.environ[username_env_var], os.environ[password_env_var]),
            )

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Writing {len(records)} nodes to Neo4j label {node_label} (merge: {merge})"
            )

            # Build Cypher: MERGE or CREATE on id_column, then SET all properties
            if merge:
                cypher = (
                    f"UNWIND $rows AS row "
                    f"MERGE (n:{node_label} {{{id_column}: row.{id_column}}}) "
                    f"SET n += row"
                )
            else:
                cypher = (
                    f"UNWIND $rows AS row "
                    f"CREATE (n:{node_label}) "
                    f"SET n = row"
                )

            with driver.session(database=database) as session:
                session.run(cypher, rows=records)
            driver.close()

            context.log.info(f"Successfully wrote {len(records)} {node_label} nodes to Neo4j")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "node_label": MetadataValue.text(node_label),
                    "merge": MetadataValue.bool(merge),
                }
            )

        return Definitions(assets=[neo4j_writer_asset])
