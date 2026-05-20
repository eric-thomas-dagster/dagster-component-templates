"""PolarsReadDatabaseComponent — push a SQL query to the database, get polars out.

The pushdown story for SQL sources is "the query IS the pushdown" — the
database engine plans + executes the query and returns only the rows
that matched. Polars's `pl.read_database_uri()` wraps that and turns
the result set into a polars DataFrame natively (no pandas intermediate).

Use when:
- Source is a SQL warehouse/database
- You want the engine to do the filter/aggregation (push compute to data)
- Output should land in polars for the next step

Example:
    type: dagster_component_templates.PolarsReadDatabaseComponent
    attributes:
      asset_name: paid_orders_q1
      connection_uri: postgresql://user:pass@host:5432/orders
      query: |
        SELECT order_id, customer_id, amount, status, created_at
        FROM raw.orders
        WHERE status = 'paid' AND created_at >= '2026-01-01'
      group_name: ingestion

For pushdown to actually fire you need to write the predicate INTO the
query. Polars's read_database executes the SQL as-is — it doesn't
synthesize WHERE clauses for you.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class PolarsReadDatabaseComponent(Component, Model, Resolvable):
    """Execute a SQL query against a database and return the result as polars."""

    asset_name: str = Field(description="Output Dagster asset name")
    query: str = Field(
        description=(
            "SQL query to execute on the database. The query IS the pushdown — "
            "write WHERE clauses, joins, aggregations into the SQL and the "
            "database engine optimizes + executes."
        ),
    )
    connection_uri: Optional[str] = Field(
        default=None,
        description=(
            "Database connection URI (literal). polars uses connectorx by "
            "default for fast typed results. Examples: "
            "'postgresql://user:pass@host:5432/db', 'mysql://...', "
            "'sqlite:///path/to/file.db', 'snowflake://...'. "
            "Set this OR connection_uri_env_var."
        ),
    )
    connection_uri_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the database connection URI. Set this OR connection_uri.",
    )
    output_type: str = Field(
        default="polars",
        description="'polars' (default) or 'pandas'. Auto-converts at boundary if pandas.",
    )
    engine: str = Field(
        default="connectorx",
        description="Polars read engine: 'connectorx' (default, fast, typed) or 'adbc' (Arrow Database Connectivity).",
    )
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=500)

    @classmethod
    def get_description(cls) -> str:
        return "Execute SQL against a database, return polars (compute pushed down via the SQL query itself)."

    def _resolve_uri(self) -> str:
        import os
        if self.connection_uri:
            return self.connection_uri
        if self.connection_uri_env_var:
            v = os.environ.get(self.connection_uri_env_var)
            if not v:
                raise EnvironmentError(f"Env var {self.connection_uri_env_var!r} is not set")
            return v
        raise ValueError("Set either 'connection_uri' or 'connection_uri_env_var'")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        output_type = self.output_type.lower()
        if output_type not in ("polars", "pandas"):
            raise ValueError(f"output_type must be 'polars' or 'pandas', got {self.output_type!r}")
        if self.engine not in ("connectorx", "adbc"):
            raise ValueError(f"engine must be 'connectorx' or 'adbc', got {self.engine!r}")

        asset_name = self.asset_name
        query = self.query
        engine_name = self.engine
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        resolve_uri = self._resolve_uri

        kinds = list(self.kinds or []) or ["polars", "sql"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _polars_read_database_asset(context: AssetExecutionContext) -> Any:
            import polars as pl

            uri = resolve_uri()
            context.log.info(f"polars_read_database: executing query via {engine_name}")

            # read_database_uri kwarg name varies by polars version:
            # older versions accept `engine="connectorx"`; newer expose adbc
            # via the same kw. The function is the canonical "push to DB" path.
            result_pl = pl.read_database_uri(query=query, uri=uri, engine=engine_name)
            row_count = result_pl.height
            context.log.info(f"polars_read_database: returned {row_count} rows × {result_pl.width} columns")

            _meta_df = result_pl.to_pandas()
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            metadata = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "polars/engine": MetadataValue.text(engine_name),
                "polars/query_pushdown": MetadataValue.md(f"```sql\n{query.strip()}\n```"),
            }
            if include_preview and row_count > 0:
                try:
                    _prev = _meta_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)

            return result_pl if output_type == "polars" else result_pl.to_pandas()

        return Definitions(assets=[_polars_read_database_asset])
