"""GCS Parquet IO Manager component — reads and writes Pandas DataFrames as Parquet files on Google Cloud Storage."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class GCSParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IO manager that stores assets as Parquet files on Google Cloud Storage."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    bucket: str = Field(description="GCS bucket to store Parquet files in")
    prefix: str = Field(default="assets", description="GCS object prefix for asset files, e.g. 'dagster/assets'")
    project: Optional[str] = Field(default=None, description="GCP project ID (uses default project if not set)")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Environment variable holding a JSON service account key (uses ADC if not set)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        bucket = self.bucket
        prefix = self.prefix
        project = self.project
        gcp_credentials_env_var = self.gcp_credentials_env_var

        @dg.io_manager
        def gcs_parquet_io_manager(init_context):
            class _GCSParquetIOManager(dg.IOManager):
                def _get_fs(self):
                    import gcsfs
                    kwargs = {"project": project} if project else {}
                    if gcp_credentials_env_var:
                        import json
                        creds_str = os.environ.get(gcp_credentials_env_var, "")
                        if creds_str:
                            kwargs["token"] = json.loads(creds_str)
                    return gcsfs.GCSFileSystem(**kwargs)

                def _object_path(self, context):
                    table = context.asset_key.path[-1]
                    return f"{bucket}/{prefix}/{table}.parquet"

                def handle_output(self, context, obj: pd.DataFrame):
                    fs = self._get_fs()
                    path = self._object_path(context)
                    with fs.open(path, "wb") as f:
                        obj.to_parquet(f, index=False)
                    context.add_output_metadata({"row_count": len(obj), "gcs_path": f"gs://{path}"})

                def load_input(self, context):
                    fs = self._get_fs()
                    path = self._object_path(context)
                    with fs.open(path, "rb") as f:
                        return pd.read_parquet(f)

            return _GCSParquetIOManager()

        return dg.Definitions(resources={self.resource_key: gcs_parquet_io_manager})
