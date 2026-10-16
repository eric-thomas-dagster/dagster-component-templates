"""Athena IO Manager component.

YAML/Component wrapper around `AthenaIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from collections.abc import Mapping
from typing import Optional, Union, Any
import dagster as dg
from pydantic import Field
import pandas as pd


def _sanitize_ident(component: str) -> str:
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


class AthenaIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that writes Parquet to S3 and registers in Glue/Athena.

    Partition ranges (single_run backfills): when an asset with
    ``BackfillPolicy.single_run()`` materializes several partitions in one run,
    return either a dict ``{partition_key: DataFrame}`` (keys must equal the
    range) or one DataFrame that already contains ``partition_column`` whose
    values are a subset of the range. Only those partitions are rewritten;
    other partitions are never touched. Loading a range returns ONE DataFrame
    filtered with ``partition_column IN (...)``.

    Note: Athena ``overwrite_partitions`` only rewrites partitions that appear in the
    written data, so a range key with zero rows leaves that partition's old files in place.
    """

    database: str = Field(description="Athena database (Glue catalog database)")
    s3_staging_dir: str = Field(description="S3 URI for Athena query results, e.g. 's3://my-bucket/athena-results/'")
    s3_output_location: Optional[str] = Field(default=None, description="S3 URI prefix where Parquet assets are written")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key (resolved from env via dg.EnvVar)")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret key (resolved from env via dg.EnvVar)")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used as the Hive partition column when writing partitioned assets",
    )

    def _get_wrangler_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if self.aws_access_key_id and self.aws_secret_access_key:
            import boto3
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
            kwargs["boto3_session"] = session
        return kwargs

    def _output_root(self) -> str:
        return (self.s3_output_location or self.s3_staging_dir).rstrip("/")

    def _table_name(self, context) -> tuple[str, str, str]:
        """Return ``(database, table, s3_path)`` for an asset.

        Multi-component asset keys map the first component to the database and
        join the rest with ``_`` for the table. Single-component keys land in
        the configured ``database``.
        """
        if context.has_asset_key:
            path = [_sanitize_ident(p) for p in context.asset_key.path]
        else:
            path = [_sanitize_ident(p) for p in (context.step_key, context.name)]
        if len(path) >= 2:
            db = path[0]
            tbl = "_".join(path[1:])
        else:
            db = self.database
            tbl = path[0]
        s3_path = f"{self._output_root()}/{db}/{tbl}/"
        return db, tbl, s3_path

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        range_keys = _range_keys(context)
        if range_keys is None and not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"AthenaIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        import awswrangler as wr

        database, table, s3_path = self._table_name(context)
        wrangler_kwargs = self._get_wrangler_kwargs()

        # Ensure database exists (Glue creates lazily but be explicit).
        try:
            wr.catalog.create_database(name=database, exist_ok=True, **wrangler_kwargs)
        except Exception:
            # Older awswrangler versions may not have exist_ok; ignore "already exists".
            pass

        partition_keys = [str(context.partition_key)] if context.has_partition_key else range_keys
        row_count = len(obj)
        if partition_keys is not None:
            df = _build_partitioned_frame(
                obj, partition_keys, self.partition_column, single=range_keys is None
            )
            row_count = len(df)
            wr.s3.to_parquet(
                df=df,
                path=s3_path,
                dataset=True,
                database=database,
                table=table,
                mode="overwrite_partitions",
                partition_cols=[self.partition_column],
                **wrangler_kwargs,
            )
        else:
            wr.s3.to_parquet(
                df=obj,
                path=s3_path,
                dataset=True,
                database=database,
                table=table,
                mode="overwrite",
                **wrangler_kwargs,
            )

        context.add_output_metadata(
            {
                "table": dg.MetadataValue.text(f"{database}.{table}"),
                "object_path": dg.MetadataValue.path(s3_path),
                "row_count": dg.MetadataValue.int(row_count),
                "partition_key": dg.MetadataValue.text(
                    _partition_label(partition_keys) if partition_keys is not None else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        import awswrangler as wr

        upstream = context.upstream_output
        database, table, _ = self._table_name(upstream)
        wrangler_kwargs = self._get_wrangler_kwargs()

        load_keys = [str(upstream.partition_key)] if upstream.has_partition_key else _range_keys(upstream)
        if load_keys is not None:
            in_list = ", ".join("'" + k.replace("'", "''") + "'" for k in load_keys)
            query = (
                f'SELECT * FROM "{database}"."{table}" '
                f'WHERE "{self.partition_column}" IN ({in_list})'
            )
        else:
            query = f'SELECT * FROM "{database}"."{table}"'
        return wr.athena.read_sql_query(
            query,
            database=database,
            s3_output=self.s3_staging_dir,
            **wrangler_kwargs,
        )



class AthenaIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Athena IO manager that stores assets as Parquet on S3 and queries them via Amazon Athena.

    This manager does **two** things on every materialization:
      1. Writes a Parquet dataset to ``s3_output_location/<database>/<table>/``
      2. Registers/updates the table in the AWS Glue Data Catalog so Athena can query it

    Features:
      - Partition-aware writes: ``mode='overwrite_partitions'`` rewrites only the
        Hive partition matching ``partition_column = '<partition_key>'``
      - Multi-component asset keys map to ``database.table`` (first component is the database)
      - Output metadata records the qualified table name, S3 path, row count, and partition key
      - Idempotent partition replacement: re-running a partition replaces only that partition's files

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    database: str = Field(description="Athena database (Glue catalog database)")
    s3_staging_dir: str = Field(description="S3 URI for Athena query results, e.g. 's3://my-bucket/athena-results/'")
    s3_output_location: Optional[str] = Field(
        default=None,
        description="S3 URI prefix where Parquet assets are written, e.g. 's3://my-bucket/assets/'",
    )
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS access key (uses instance role if not set)",
    )
    aws_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS secret key",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Hive partition column name used when writing partitioned assets",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = AthenaIOManager(
            database=self.database,
            s3_staging_dir=self.s3_staging_dir,
            s3_output_location=self.s3_output_location,
            region_name=self.region_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_env_var) if self.aws_access_key_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_key_env_var) if self.aws_secret_key_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
