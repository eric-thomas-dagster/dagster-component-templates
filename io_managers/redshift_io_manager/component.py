"""Redshift IO Manager component — reads and writes Pandas DataFrames via Amazon Redshift."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class RedshiftIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a RedshiftPandasIOManager so assets are automatically stored in and loaded from Amazon Redshift."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    host: str = Field(description="Redshift cluster endpoint host")
    port: int = Field(default=5439, description="Redshift port (default 5439)")
    database: str = Field(description="Redshift database name")
    user: str = Field(description="Redshift username")
    password_env_var: str = Field(description="Environment variable holding the Redshift password")
    schema_name: Optional[str] = Field(default="public", description="Default schema for asset tables")
    s3_bucket: Optional[str] = Field(default=None, description="S3 bucket for UNLOAD/COPY staging (required for large datasets)")
    s3_prefix: Optional[str] = Field(default="dagster_temp", description="S3 key prefix for staging files")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS access key (for S3 staging)")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS secret key (for S3 staging)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_aws.redshift import RedshiftClientResource
        import pandas as pd

        host = self.host
        port = self.port
        database = self.database
        user = self.user
        password = os.environ.get(self.password_env_var, "")
        schema_name = self.schema_name or "public"

        @dg.io_manager(required_resource_keys=set())
        def redshift_pandas_io_manager(init_context):
            import sqlalchemy as sa

            class _RedshiftPandasIOManager(dg.IOManager):
                def _get_engine(self):
                    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
                    return sa.create_engine(url)

                def handle_output(self, context, obj: pd.DataFrame):
                    table = context.asset_key.path[-1]
                    engine = self._get_engine()
                    obj.to_sql(table, engine, schema=schema_name, if_exists="replace", index=False)
                    context.add_output_metadata({"row_count": len(obj), "table": f"{schema_name}.{table}"})

                def load_input(self, context):
                    table = context.asset_key.path[-1]
                    engine = self._get_engine()
                    return pd.read_sql(f'SELECT * FROM "{schema_name}"."{table}"', engine)

            return _RedshiftPandasIOManager()

        return dg.Definitions(resources={self.resource_key: redshift_pandas_io_manager})
