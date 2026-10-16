"""Databricks IO Manager component.

YAML/Component wrapper around `DatabricksIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Union
from collections.abc import Mapping
import dagster as dg
from pydantic import Field
import pandas as pd


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




def _range_keys(ctx) -> Optional[list]:
    """Partition keys when ``ctx`` covers a partition RANGE (single_run backfill).

    For a range, Dagster reports ``has_partition_key=False`` while
    ``has_asset_partitions=True`` (and ``partition_key`` raises), so gating on
    ``has_partition_key`` alone would silently treat a range as unpartitioned.
    Returns None for single-partition or unpartitioned contexts.
    """
    if ctx.has_partition_key:
        return None
    if ctx.has_asset_partitions:
        keys = [str(k) for k in ctx.asset_partition_keys]
        if keys:
            return keys
    return None


def _partition_label(keys: list) -> str:
    return keys[0] if len(keys) == 1 else f"{keys[0]}...{keys[-1]}"


def _build_partitioned_frame(obj, keys: list, column: str, single: bool) -> pd.DataFrame:
    """Return one DataFrame carrying ``column`` for every row being written.

    single=True: ``obj`` is a DataFrame for the one partition ``keys[0]``.
    single=False (range): ``obj`` is either a Mapping {partition_key: DataFrame}
    whose keys equal the range, or a DataFrame that already has ``column`` whose
    distinct values are a subset of ``keys``.
    """
    if single:
        df = obj.copy()
        df[column] = keys[0]
        return df
    expected = set(keys)
    if isinstance(obj, Mapping):
        got = {str(k) for k in obj.keys()}
        if got != expected:
            raise ValueError(
                f"Multi-partition output keys must equal the materialized partition range. "
                f"Missing: {sorted(expected - got)}; unexpected: {sorted(got - expected)}."
            )
        by_key = {str(k): v for k, v in obj.items()}
        frames = []
        for k in keys:
            v = by_key[k]
            if not isinstance(v, pd.DataFrame):
                raise TypeError(f"Value for partition {k!r} must be a pandas DataFrame; got {type(v).__name__}.")
            part = v.copy()
            part[column] = k
            frames.append(part)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame({column: []})
    if isinstance(obj, pd.DataFrame):
        if column not in obj.columns:
            raise ValueError(
                f"This asset materialized a partition range ({_partition_label(keys)}) but the returned "
                f"DataFrame has no {column!r} column to split rows by partition. Return a DataFrame that "
                f"includes {column!r}, or a dict {{partition_key: DataFrame}}."
            )
        if obj[column].isna().any():
            raise ValueError(f"Column {column!r} contains nulls; every row must name its partition key.")
        df = obj.copy()
        df[column] = df[column].astype(str)
        bad = set(df[column].unique()) - expected
        if bad:
            raise ValueError(
                f"Column {column!r} has values outside the materialized partition range: {sorted(bad)} "
                f"(range: {_partition_label(keys)})."
            )
        return df
    raise TypeError(
        f"A multi-partition (single_run backfill) output must be a dict {{partition_key: DataFrame}} "
        f"or a DataFrame containing {column!r}; got {type(obj).__name__}."
    )


class DatabricksIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Unity Catalog Delta tables.

    Partition ranges (single_run backfills): when an asset with
    ``BackfillPolicy.single_run()`` materializes several partitions in one run,
    return either a dict ``{partition_key: DataFrame}`` (keys must equal the
    range) or one DataFrame that already contains ``partition_column`` whose
    values are a subset of the range. Exactly the range's rows are deleted and
    re-inserted; other partitions are never touched. Loading a range returns ONE
    DataFrame filtered with ``partition_column IN (...)``.
    """

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
        range_keys = _range_keys(context)
        if range_keys is None and not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"DatabricksIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )

        schema, table = self._table_name(context)
        qualified = f"`{self.catalog}`.`{schema}`.`{table}`"

        partition_keys = [str(context.partition_key)] if context.has_partition_key else range_keys
        if partition_keys is not None:
            # obj now carries partition_column on every row (single key or whole range).
            obj = _build_partitioned_frame(
                obj, partition_keys, self.partition_column, single=range_keys is None
            )
            in_list = ", ".join(_sql_literal(k) for k in partition_keys)
            delete_sql = f"DELETE FROM {qualified} WHERE `{self.partition_column}` IN ({in_list})"
        else:
            delete_sql = f"DELETE FROM {qualified}"

        col_defs = ", ".join(
            f"`{c}` {'STRING' if partition_keys is not None and c == self.partition_column else _databricks_type_for(obj[c].dtype)}"
            for c in obj.columns
        )

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{schema}`")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({col_defs}) USING DELTA")

            if self.staging_location:
                # Stage Parquet to cloud storage and COPY INTO. Single transactional MERGE-like step.
                import uuid
                stage_path = f"{self.staging_location.rstrip('/')}/{schema}_{table}_{uuid.uuid4().hex}.parquet"
                obj.to_parquet(stage_path, index=False)

                cur.execute(delete_sql)
                cur.execute(
                    f"COPY INTO {qualified} FROM '{stage_path}' "
                    "FILEFORMAT = PARQUET "
                    "FORMAT_OPTIONS ('mergeSchema' = 'true') "
                    "COPY_OPTIONS ('mergeSchema' = 'true')"
                )
            else:
                # Inline DELETE+INSERT — Databricks SQL auto-commits each statement,
                # but Delta's per-statement ACID semantics ensure each is atomic.
                cur.execute(delete_sql)

                if len(obj) > 0:
                    col_list = ", ".join(f"`{c}`" for c in obj.columns)
                    rows_sql = []
                    for _, row in obj.iterrows():
                        vals = ", ".join(_sql_literal(v) for v in row)
                        rows_sql.append(f"({vals})")
                    cur.execute(f"INSERT INTO {qualified} ({col_list}) VALUES {', '.join(rows_sql)}")
        finally:
            conn.close()

        context.add_output_metadata(
            {
                "table": dg.MetadataValue.text(qualified),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    _partition_label(partition_keys) if partition_keys is not None else "(unpartitioned)"
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
            load_keys = [str(upstream.partition_key)] if upstream.has_partition_key else _range_keys(upstream)
            if load_keys is not None:
                in_list = ", ".join(_sql_literal(k) for k in load_keys)
                cur.execute(
                    f"SELECT * FROM {qualified} "
                    f"WHERE `{self.partition_column}` IN ({in_list})"
                )
            else:
                cur.execute(f"SELECT * FROM {qualified}")
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()



class DatabricksIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Databricks IO manager that reads and writes Pandas DataFrames as Unity Catalog Delta tables.

    Features:
      - Partition-aware writes via DELETE+INSERT scoped by ``partition_column``
        (Delta's per-statement ACID semantics make each DELETE/INSERT atomic)
      - Multi-component asset keys map to ``catalog.schema.table`` (first component is the schema)
      - Output metadata records the qualified table name, row count, and partition key
      - Optional ``staging_location`` for Parquet + ``COPY INTO`` ingestion (preferred for large datasets)
      - Idempotent partition replacement: re-running a partition replaces only that partition's rows

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    server_hostname: str = Field(description="Databricks workspace hostname, e.g. 'adb-1234567890.12.azuredatabricks.net'")
    http_path: str = Field(description="HTTP path to the SQL warehouse or cluster, e.g. '/sql/1.0/warehouses/abc123'")
    access_token_env_var: str = Field(description="Environment variable holding the Databricks personal access token")
    catalog: str = Field(default="main", description="Unity Catalog catalog name")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    staging_location: Optional[str] = Field(
        default=None,
        description="Cloud storage URI for staging data, e.g. 's3://bucket/staging' or 'abfss://container@account.dfs.core.windows.net/staging'",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = DatabricksIOManager(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=dg.EnvVar(self.access_token_env_var) if self.access_token_env_var else None,
            catalog=self.catalog,
            default_schema=self.default_schema,
            staging_location=self.staging_location,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
