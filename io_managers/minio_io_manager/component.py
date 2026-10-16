from typing import Union
"""MinIO IO Manager component.

YAML/Component wrapper around `MinIODeltaIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from collections.abc import Mapping
import dagster as dg
from pydantic import Field
from typing import Optional
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid S3 keys."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")




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


class MinIODeltaIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Delta tables on MinIO.

    Partition ranges (single_run backfills): when an asset with
    ``BackfillPolicy.single_run()`` materializes several partitions in one run,
    return either a dict ``{partition_key: DataFrame}`` (keys must equal the
    range) or one DataFrame that already contains ``partition_column`` whose
    values are a subset of the range. Exactly the range's rows are replaced via
    a predicate overwrite; other partitions are never touched. Loading a range returns ONE DataFrame
    filtered with ``partition_column IN (...)``.
    """

    bucket: str = Field(description="S3/MinIO bucket to store Delta tables in")
    prefix: str = Field(default="dagster/assets", description="Key prefix within the bucket")
    endpoint_url: str = Field(default="http://localhost:9000", description="MinIO endpoint URL")
    access_key: Optional[str] = Field(default=None, description="MinIO access key (resolved from env via dg.EnvVar)")
    secret_key: Optional[str] = Field(default=None, description="MinIO secret key (resolved from env via dg.EnvVar)")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition overwrites in handle_output / load_input",
    )

    def _get_storage_options(self) -> dict[str, str]:
        opts: dict[str, str] = {
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        if self.access_key:
            opts["AWS_ACCESS_KEY_ID"] = self.access_key
        if self.secret_key:
            opts["AWS_SECRET_ACCESS_KEY"] = self.secret_key
        return opts

    def _table_path(self, context) -> str:
        """Build the Delta table path for an asset (or output).

        Layout: ``s3://<bucket>/<prefix>/<asset_path>``
        Partition data goes inside the table directory; per-partition writes
        use Delta's predicate-based overwrite to scope to a single partition.
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            base = "/".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        return f"s3://{self.bucket}/{self.prefix}/{base}"

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        range_keys = _range_keys(context)
        if range_keys is None and not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"MinIODeltaIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        from deltalake.writer import write_deltalake

        path = self._table_path(context)
        storage_options = self._get_storage_options()

        partition_keys = [str(context.partition_key)] if context.has_partition_key else range_keys
        row_count = len(obj)
        if partition_keys is not None:
            df = _build_partitioned_frame(
                obj, partition_keys, self.partition_column, single=range_keys is None
            )
            df[self.partition_column] = df[self.partition_column].map(_sanitize)
            row_count = len(df)
            in_list = ", ".join(
                "'" + _sanitize(k).replace("'", "''") + "'" for k in partition_keys
            )
            predicate = (
                f"{self.partition_column} = {in_list}"
                if len(partition_keys) == 1
                else f"{self.partition_column} IN ({in_list})"
            )
            write_deltalake(
                path,
                df,
                mode="overwrite",
                partition_by=[self.partition_column],
                predicate=predicate,
                storage_options=storage_options,
            )
        else:
            write_deltalake(
                path,
                obj,
                mode="overwrite",
                storage_options=storage_options,
            )

        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(path),
                "row_count": dg.MetadataValue.int(row_count),
                "partition_key": dg.MetadataValue.text(
                    _partition_label(partition_keys) if partition_keys is not None else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        from deltalake import DeltaTable

        upstream = context.upstream_output
        path = self._table_path(upstream)
        dt = DeltaTable(path, storage_options=self._get_storage_options())
        load_keys = [str(upstream.partition_key)] if upstream.has_partition_key else _range_keys(upstream)
        if load_keys is not None:
            values = [_sanitize(k) for k in load_keys]
            if len(values) == 1:
                return dt.to_pandas(partitions=[(self.partition_column, "=", values[0])])
            return dt.to_pandas(partitions=[(self.partition_column, "in", values)])
        return dt.to_pandas()



class MinIOIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Delta Lake IO manager pointed at a MinIO endpoint for S3-compatible lakehouse storage.

    Features:
      - Partitioned assets are written with Delta predicate-based overwrite
        so per-partition rewrites don't clobber sibling partitions
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested table paths, preventing collisions between assets of the same name
      - Output metadata records the Delta table URI, row count, and partition key
      - S3-compatible (MinIO, LocalStack, Cloudflare R2, etc.) via ``endpoint_url``

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(default="lake", description="S3/MinIO bucket to store Delta tables in")
    prefix: str = Field(
        default="dagster/assets",
        description="Key prefix within the bucket for asset tables",
    )
    endpoint_url: str = Field(
        default="http://localhost:9000",
        description="MinIO endpoint URL, e.g. 'http://localhost:9000'",
    )
    access_key_env_var: str = Field(
        default="MINIO_ACCESS_KEY",
        description="Environment variable holding the MinIO access key",
    )
    secret_key_env_var: str = Field(
        default="MINIO_SECRET_KEY",
        description="Environment variable holding the MinIO secret key",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition Delta overwrites",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = MinIODeltaIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            endpoint_url=self.endpoint_url,
            access_key=dg.EnvVar(self.access_key_env_var) if self.access_key_env_var else None,
            secret_key=dg.EnvVar(self.secret_key_env_var) if self.secret_key_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
