from typing import Union
"""Postgres IO Manager component.

YAML/Component wrapper around `PostgresIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from collections.abc import Mapping
import dagster as dg
from pydantic import Field
from typing import Optional
import pandas as pd


def _sanitize_ident(component: str) -> str:
    """Sanitize an identifier component for safe interpolation into SQL.

    Lowercases, replaces non-alphanumeric chars with underscores. Postgres
    quoting around the final identifier still applies.
    """
    cleaned = "".join(c if c.isalnum() else "_" for c in component)
    return cleaned.lower().strip("_") or "t"




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


class PostgresIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Postgres tables.

    Partition ranges (single_run backfills): when an asset with
    ``BackfillPolicy.single_run()`` materializes several partitions in one run,
    return either a dict ``{partition_key: DataFrame}`` (keys must equal the
    range) or one DataFrame that already contains ``partition_column`` whose
    values are a subset of the range. Exactly the range's rows are deleted and
    re-inserted in one transaction; other partitions are never touched. Loading
    a range returns ONE DataFrame filtered with ``partition_column IN (...)``.
    """

    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(description="PostgreSQL database name")
    user: str = Field(description="PostgreSQL username")
    password: Optional[str] = Field(default=None, description="PostgreSQL password (resolved from env via dg.EnvVar)")
    default_schema: str = Field(default="public", description="Schema used when asset key has only one component")
    if_exists: str = Field(default="replace", description="Behavior for unpartitioned writes when the table already exists: 'replace', 'append', or 'fail'")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def _get_engine(self):
        import sqlalchemy as sa
        password = self.password or ""
        url = f"postgresql+psycopg2://{self.user}:{password}@{self.host}:{self.port}/{self.database}"
        return sa.create_engine(url)

    def _table_name(self, context) -> tuple[str, str]:
        """Return ``(schema, table)`` for an asset.

        Multi-component asset keys (``len(path) >= 2``) map the first component
        to the schema and join the rest with ``_`` for the table. Single-component
        keys land in ``default_schema``.
        """
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
                f"PostgresIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        import sqlalchemy as sa

        schema, table = self._table_name(context)
        engine = self._get_engine()
        partition_keys = [str(context.partition_key)] if context.has_partition_key else range_keys
        row_count = len(obj)

        with engine.begin() as conn:  # transaction
            # Ensure schema exists.
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

            if partition_keys is not None:
                df = _build_partitioned_frame(
                    obj, partition_keys, self.partition_column, single=range_keys is None
                )
                # First write may need to create the table; use append after that.
                exists = conn.execute(
                    sa.text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :s AND table_name = :t"
                    ),
                    {"s": schema, "t": table},
                ).first()
                if exists:
                    conn.execute(
                        sa.text(
                            f'DELETE FROM "{schema}"."{table}" '
                            f'WHERE "{self.partition_column}" IN :pks'
                        ).bindparams(sa.bindparam("pks", expanding=True)),
                        {"pks": partition_keys},
                    )
                    df.to_sql(table, conn, schema=schema, if_exists="append", index=False)
                else:
                    df.to_sql(table, conn, schema=schema, if_exists="replace", index=False)
                row_count = len(df)
            else:
                obj.to_sql(table, conn, schema=schema, if_exists=self.if_exists, index=False)

        context.add_output_metadata(
            {
                "table": dg.MetadataValue.text(f"{schema}.{table}"),
                "row_count": dg.MetadataValue.int(row_count),
                "partition_key": dg.MetadataValue.text(
                    _partition_label(partition_keys) if partition_keys is not None else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        import sqlalchemy as sa

        upstream = context.upstream_output
        schema, table = self._table_name(upstream)
        engine = self._get_engine()
        with engine.connect() as conn:
            load_keys = [str(upstream.partition_key)] if upstream.has_partition_key else _range_keys(upstream)
            if load_keys is not None:
                return pd.read_sql(
                    sa.text(
                        f'SELECT * FROM "{schema}"."{table}" '
                        f'WHERE "{self.partition_column}" IN :pks'
                    ).bindparams(sa.bindparam("pks", expanding=True)),
                    conn,
                    params={"pks": load_keys},
                )
            return pd.read_sql(sa.text(f'SELECT * FROM "{schema}"."{table}"'), conn)



class PostgresIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PostgreSQL IO manager so assets are automatically stored in and loaded from Postgres tables.

    Features:
      - Partition-aware writes via transactional DELETE+INSERT scoped by
        ``partition_column`` — backfills don't clobber sibling partitions
      - Multi-component asset keys map to ``schema.table`` (first component is the schema)
      - Output metadata records the qualified table name, row count, and partition key
      - Idempotent partition replacement: re-running a partition replaces only that partition's rows

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(description="PostgreSQL database name")
    user: str = Field(description="PostgreSQL username")
    password_env_var: str = Field(description="Environment variable holding the PostgreSQL password")
    default_schema: str = Field(
        default="public",
        description="Schema used when asset key has only one component",
    )
    if_exists: str = Field(
        default="replace",
        description="Behavior for unpartitioned writes when table exists: 'replace', 'append', or 'fail'",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = PostgresIOManager(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            default_schema=self.default_schema,
            if_exists=self.if_exists,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
