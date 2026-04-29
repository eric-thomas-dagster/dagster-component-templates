"""Athena IO Manager.

Stores Dagster assets as Parquet files on S3 and registers them as
external tables in the AWS Glue Data Catalog so they can be queried via
Amazon Athena. Supports partitioned assets (per-partition Parquet files
under a Hive-style ``partition_key=<value>/`` prefix) and multi-component
asset keys (mapped to ``database.table``).

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize_ident(component: str) -> str:
    cleaned = "".join(c if c.isalnum() else "_" for c in component)
    return cleaned.lower().strip("_") or "t"


class AthenaIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that writes Parquet to S3 and registers in Glue/Athena."""

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
        if not isinstance(obj, pd.DataFrame):
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

        if context.has_partition_key:
            partition_value = str(context.partition_key)
            df = obj.copy()
            df[self.partition_column] = partition_value
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
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        import awswrangler as wr

        upstream = context.upstream_output
        database, table, _ = self._table_name(upstream)
        wrangler_kwargs = self._get_wrangler_kwargs()

        if upstream.has_partition_key:
            partition_value = str(upstream.partition_key)
            query = (
                f'SELECT * FROM "{database}"."{table}" '
                f'WHERE "{self.partition_column}" = \'{partition_value}\''
            )
        else:
            query = f'SELECT * FROM "{database}"."{table}"'
        return wr.athena.read_sql_query(
            query,
            database=database,
            s3_output=self.s3_staging_dir,
            **wrangler_kwargs,
        )
