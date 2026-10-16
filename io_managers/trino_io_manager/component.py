"""Trino IO Manager component.

YAML/Component wrapper around `TrinoIOManager`. Use `resource_key:
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
    """Render a Python value as a Trino SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, float) and v != v:  # NaN
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _trino_type_for(dtype) -> str:
    s = str(dtype)
    if s.startswith("float"):
        return "DOUBLE"
    if s.startswith("int") or s.startswith("uint"):
        return "BIGINT"
    if s.startswith("bool"):
        return "BOOLEAN"
    return "VARCHAR"




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


class TrinoIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames via Trino SQL.

    Partition ranges (single_run backfills): when an asset with
    ``BackfillPolicy.single_run()`` materializes several partitions in one run,
    return either a dict ``{partition_key: DataFrame}`` (keys must equal the
    range) or one DataFrame that already contains ``partition_column`` whose
    values are a subset of the range. Exactly the range's rows are deleted and
    re-inserted; other partitions are never touched. Loading a range returns ONE
    DataFrame filtered with ``partition_column IN (...)``.
    """

    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Trino catalog, e.g. 'iceberg', 'hive', 'delta'")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    password: Optional[str] = Field(default=None, description="Trino password (resolved from env via dg.EnvVar)")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def _get_connection(self):
        import trino.dbapi as trino_dbapi
        import trino.auth as trino_auth
        return trino_dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.default_schema,
            auth=trino_auth.BasicAuthentication(self.user, self.password) if self.password else None,
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
                f"TrinoIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )

        schema, table = self._table_name(context)
        qualified = f"{self.catalog}.{schema}.{table}"

        partition_keys = [str(context.partition_key)] if context.has_partition_key else range_keys
        if partition_keys is not None:
            # df carries partition_column on every row (single key or whole range).
            obj = _build_partitioned_frame(
                obj, partition_keys, self.partition_column, single=range_keys is None
            )

        cols_with_types = list(obj.columns)
        col_defs = ", ".join(
            f'"{c}" {"VARCHAR" if partition_keys is not None and c == self.partition_column else _trino_type_for(obj[c].dtype)}'
            for c in cols_with_types
        )

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            # Trino DDL is auto-committed per statement; multi-statement
            # transactional semantics are achieved via START TRANSACTION / COMMIT.
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({col_defs})")

            # Per-partition: DELETE + INSERT inside an explicit transaction.
            # For unpartitioned: full TRUNCATE-equivalent (DELETE) + INSERT.
            cur.execute("START TRANSACTION")
            try:
                if partition_keys is not None:
                    in_list = ", ".join(_sql_literal(k) for k in partition_keys)
                    cur.execute(
                        f"DELETE FROM {qualified} "
                        f"WHERE \"{self.partition_column}\" IN ({in_list})"
                    )
                else:
                    cur.execute(f"DELETE FROM {qualified}")

                if len(obj) > 0:
                    col_list = ", ".join(f'"{c}"' for c in cols_with_types)
                    rows_sql = []
                    for _, row in obj.iterrows():
                        vals = ", ".join(_sql_literal(v) for v in row)
                        rows_sql.append(f"({vals})")
                    cur.execute(f"INSERT INTO {qualified} ({col_list}) VALUES {', '.join(rows_sql)}")
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
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
        qualified = f"{self.catalog}.{schema}.{table}"
        conn = self._get_connection()
        try:
            load_keys = [str(upstream.partition_key)] if upstream.has_partition_key else _range_keys(upstream)
            if load_keys is not None:
                in_list = ", ".join(_sql_literal(k) for k in load_keys)
                query = (
                    f"SELECT * FROM {qualified} "
                    f"WHERE \"{self.partition_column}\" IN ({in_list})"
                )
            else:
                query = f"SELECT * FROM {qualified}"
            return pd.read_sql(query, conn)
        finally:
            conn.close()



class TrinoIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Trino IO manager that reads and writes Pandas DataFrames via Trino SQL.

    Features:
      - Partition-aware writes via transactional DELETE+INSERT scoped by ``partition_column``
      - Multi-component asset keys map to ``catalog.schema.table`` (first component is the schema)
      - Output metadata records the qualified table name, row count, and partition key
      - Idempotent partition replacement: re-running a partition replaces only that partition's rows

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Trino catalog, e.g. 'iceberg', 'hive', 'delta'")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding the Trino password (omit for no auth)",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = TrinoIOManager(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            default_schema=self.default_schema,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
