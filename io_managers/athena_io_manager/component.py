"""Athena IO Manager component — reads and writes DataFrames via Amazon Athena + S3."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class AthenaIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Athena IO manager that stores assets as Parquet on S3 and queries them via Athena."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    database: str = Field(description="Athena database (Glue catalog database)")
    s3_staging_dir: str = Field(description="S3 URI for Athena query results, e.g. 's3://my-bucket/athena-results/'")
    s3_output_location: Optional[str] = Field(default=None, description="S3 URI prefix where Parquet assets are written, e.g. 's3://my-bucket/assets/'")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS access key (uses instance role if not set)")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS secret key")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        database = self.database
        s3_staging_dir = self.s3_staging_dir
        s3_output_location = self.s3_output_location or s3_staging_dir
        region_name = self.region_name
        aws_access_key = os.environ.get(self.aws_access_key_env_var, "") if self.aws_access_key_env_var else None
        aws_secret_key = os.environ.get(self.aws_secret_key_env_var, "") if self.aws_secret_key_env_var else None

        @dg.io_manager
        def athena_pandas_io_manager(init_context):
            class _AthenaPandasIOManager(dg.IOManager):
                def _get_wrangler_kwargs(self):
                    kwargs = {}
                    if aws_access_key and aws_secret_key:
                        import boto3
                        session = boto3.Session(
                            aws_access_key_id=aws_access_key,
                            aws_secret_access_key=aws_secret_key,
                            region_name=region_name,
                        )
                        kwargs["boto3_session"] = session
                    return kwargs

                def handle_output(self, context, obj: pd.DataFrame):
                    import awswrangler as wr
                    table = context.asset_key.path[-1]
                    path = f"{s3_output_location.rstrip('/')}/{table}/"
                    wr.s3.to_parquet(
                        df=obj,
                        path=path,
                        dataset=True,
                        database=database,
                        table=table,
                        **self._get_wrangler_kwargs(),
                    )
                    context.add_output_metadata({"row_count": len(obj), "s3_path": path, "table": table})

                def load_input(self, context):
                    import awswrangler as wr
                    table = context.asset_key.path[-1]
                    return wr.athena.read_sql_query(
                        f'SELECT * FROM "{database}"."{table}"',
                        database=database,
                        s3_output=s3_staging_dir,
                        **self._get_wrangler_kwargs(),
                    )

            return _AthenaPandasIOManager()

        return dg.Definitions(resources={self.resource_key: athena_pandas_io_manager})
