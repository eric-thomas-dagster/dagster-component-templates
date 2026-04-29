"""Databricks IO Manager.

Stores Dagster assets as Unity Catalog Delta tables via the Databricks
SQL Connector. Supports partitioned assets (per-partition DELETE+INSERT
inside a transaction) and multi-component asset keys (mapped to
``schema.table``). Optional staging via cloud-storage Parquet + ``COPY INTO``
for larger datasets.

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize_ident(component: str) -> str:
    cleaned = "".join(c if c.isalnum() else "_" for c in component)
    return cleaned.lower().strip("_") or "t"


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, float) and v != v:  # NaN
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _databricks_type_for(dtype) -> str:
    s = str(dtype)
    if s.startswith("float"):
        return "DOUBLE"
    if s.startswith("int") or s.startswith("uint"):
        return "BIGINT"
    if s.startswith("bool"):
        return "BOOLEAN"
    return "STRING"


class DatabricksIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Unity Catalog Delta tables."""

    server_hostname: str = Field(description="Databricks workspace hostname")
    http_path: str = Field(description="HTTP path to the SQL warehouse or cluster")
    access_token: Optional[str] = Field(default=None, description="Databricks PAT (resolved from env via dg.EnvVar)")
    catalog: str = Field(default="main", description="Unity Catalog catalog name")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    staging_location: Optional[str] = Field(
        default=None,
        description="Cloud storage URI for staging Parquet data, e.g. 's3://bucket/staging' or 'abfss://...'",
    )
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def _get_connection(self):
        from databricks import sql as databricks_sql
        return databricks_sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token or "",
        )

    def _table_name(self, context) -> tuple[str, str]:
        if context.has_asset_key:
            path = [_sanitize_ident(p) for p in context.asset_key.path]
        else:
            path = [_sanitize_ident(p) for p in (context.step_key, context.name)]
        if len(path) >= 2:
            return path[0], "_".join(path[1:])
        return self.default_schema, path[0]

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"DatabricksIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )

        schema, table = self._table_name(context)
        qualified = f"`{self.catalog}`.`{schema}`.`{table}`"

        col_defs = ", ".join(
            f"`{c}` {_databricks_type_for(obj[c].dtype)}" for c in obj.columns
        )
        if context.has_partition_key:
            col_defs = f"{col_defs}, `{self.partition_column}` STRING"

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{schema}`")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({col_defs}) USING DELTA")

            if self.staging_location:
                # Stage Parquet to cloud storage and COPY INTO. Single transactional MERGE-like step.
                import uuid
                stage_path = f"{self.staging_location.rstrip('/')}/{schema}_{table}_{uuid.uuid4().hex}.parquet"
                df = obj.copy()
                if context.has_partition_key:
                    df[self.partition_column] = str(context.partition_key)
                df.to_parquet(stage_path, index=False)

                if context.has_partition_key:
                    cur.execute(
                        f"DELETE FROM {qualified} "
                        f"WHERE `{self.partition_column}` = "
                        f"{_sql_literal(str(context.partition_key))}"
                    )
                else:
                    cur.execute(f"DELETE FROM {qualified}")
                cur.execute(
                    f"COPY INTO {qualified} FROM '{stage_path}' "
                    "FILEFORMAT = PARQUET "
                    "FORMAT_OPTIONS ('mergeSchema' = 'true') "
                    "COPY_OPTIONS ('mergeSchema' = 'true')"
                )
            else:
                # Inline DELETE+INSERT — Databricks SQL auto-commits each statement,
                # but Delta's per-statement ACID semantics ensure each is atomic.
                if context.has_partition_key:
                    cur.execute(
                        f"DELETE FROM {qualified} "
                        f"WHERE `{self.partition_column}` = "
                        f"{_sql_literal(str(context.partition_key))}"
                    )
                else:
                    cur.execute(f"DELETE FROM {qualified}")

                if len(obj) > 0:
                    col_list = ", ".join(f"`{c}`" for c in obj.columns)
                    if context.has_partition_key:
                        col_list = f"{col_list}, `{self.partition_column}`"
                    pk_literal = _sql_literal(str(context.partition_key)) if context.has_partition_key else None
                    rows_sql = []
                    for _, row in obj.iterrows():
                        vals = ", ".join(_sql_literal(v) for v in row)
                        if context.has_partition_key:
                            vals = f"{vals}, {pk_literal}"
                        rows_sql.append(f"({vals})")
                    cur.execute(f"INSERT INTO {qualified} ({col_list}) VALUES {', '.join(rows_sql)}")
        finally:
            conn.close()

        context.add_output_metadata(
            {
                "table": dg.MetadataValue.text(qualified),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        upstream = context.upstream_output
        schema, table = self._table_name(upstream)
        qualified = f"`{self.catalog}`.`{schema}`.`{table}`"

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            if upstream.has_partition_key:
                cur.execute(
                    f"SELECT * FROM {qualified} "
                    f"WHERE `{self.partition_column}` = "
                    f"{_sql_literal(str(upstream.partition_key))}"
                )
            else:
                cur.execute(f"SELECT * FROM {qualified}")
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()
