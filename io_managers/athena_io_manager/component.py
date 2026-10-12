"""Athena IO Manager component.

YAML/Component wrapper around `AthenaIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Union, Any
import dagster as dg
from pydantic import Field
import pandas as pd


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
