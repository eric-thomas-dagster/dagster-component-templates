"""Lance IO Manager component — reads and writes embeddings and ML datasets via LanceDB."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class LanceIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LanceDB IO manager for ML-optimised columnar storage (great for embeddings and vector data)."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    base_path: str = Field(default="./lance_data", description="Directory (or S3/GCS URI) where LanceDB tables are stored, e.g. '/data/lance' or 's3://my-bucket/lance'")
    table_name_prefix: Optional[str] = Field(default=None, description="Optional prefix prepended to every table name, e.g. 'prod_'")
    s3_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS access key when using S3-based storage")
    s3_secret_key_env_var: Optional[str] = Field(default=None, description="Environment variable for AWS secret key when using S3-based storage")
    s3_endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint URL, e.g. 'http://localhost:9000' for MinIO")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import pandas as pd

        base_path = self.base_path
        table_name_prefix = self.table_name_prefix or ""
        s3_access_key = os.environ.get(self.s3_access_key_env_var, "") if self.s3_access_key_env_var else None
        s3_secret_key = os.environ.get(self.s3_secret_key_env_var, "") if self.s3_secret_key_env_var else None
        s3_endpoint_url = self.s3_endpoint_url

        @dg.io_manager
        def lance_io_manager(init_context):
            class _LanceIOManager(dg.IOManager):
                def _get_db(self):
                    import lancedb
                    storage_options = {}
                    if s3_access_key:
                        storage_options["aws_access_key_id"] = s3_access_key
                    if s3_secret_key:
                        storage_options["aws_secret_access_key"] = s3_secret_key
                    if s3_endpoint_url:
                        storage_options["aws_endpoint"] = s3_endpoint_url
                        storage_options["allow_http"] = "true"
                    return lancedb.connect(base_path, storage_options=storage_options or None)

                def _table_name(self, context):
                    return f"{table_name_prefix}{context.asset_key.path[-1]}"

                def handle_output(self, context, obj):
                    db = self._get_db()
                    name = self._table_name(context)
                    if isinstance(obj, pd.DataFrame):
                        db.create_table(name, obj, mode="overwrite")
                    else:
                        # Accept pyarrow Table or list of dicts
                        db.create_table(name, obj, mode="overwrite")
                    context.add_output_metadata({"lance_table": name, "base_path": base_path})

                def load_input(self, context):
                    db = self._get_db()
                    name = self._table_name(context)
                    tbl = db.open_table(name)
                    return tbl.to_pandas()

            return _LanceIOManager()

        return dg.Definitions(resources={self.resource_key: lance_io_manager})
