"""Lance IO Manager component.

YAML/Component wrapper around `LanceIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Any
import dagster as dg
from pydantic import Field
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid table names / paths."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


class LanceIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames (or pyarrow Tables) as LanceDB tables."""

    base_path: str = Field(description="Directory or object-storage URI where LanceDB tables live")
    table_name_prefix: Optional[str] = Field(default=None, description="Optional prefix prepended to every table name")
    s3_access_key: Optional[str] = Field(default=None, description="S3 access key (resolved from env via dg.EnvVar)")
    s3_secret_key: Optional[str] = Field(default=None, description="S3 secret key (resolved from env via dg.EnvVar)")
    s3_endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint URL (e.g. MinIO)")

    def _get_db(self):
        import lancedb
        storage_options: dict[str, str] = {}
        if self.s3_access_key:
            storage_options["aws_access_key_id"] = self.s3_access_key
        if self.s3_secret_key:
            storage_options["aws_secret_access_key"] = self.s3_secret_key
        if self.s3_endpoint_url:
            storage_options["aws_endpoint"] = self.s3_endpoint_url
            storage_options["allow_http"] = "true"
        return lancedb.connect(self.base_path, storage_options=storage_options or None)

    def _table_name(self, context) -> str:
        """Build the LanceDB table name for an asset (or output).

        Layout: ``<prefix><asset_path_joined>[__<partition_key>]``
        Multi-component asset keys are flattened with ``__`` so the result is a
        valid single-segment table name. Partition key (when present) is
        appended after a ``__`` separator.
        """
        prefix = self.table_name_prefix or ""
        if context.has_asset_key:
            base = "__".join(_sanitize(p) for p in context.asset_key.path)
        else:
            base = "__".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        if context.has_partition_key:
            return f"{prefix}{base}__{_sanitize(context.partition_key)}"
        return f"{prefix}{base}"

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        if obj is None:
            return
        # Lance accepts pandas DataFrames and pyarrow Tables; reject everything else.
        try:
            import pyarrow as pa
            is_arrow = isinstance(obj, pa.Table)
        except Exception:
            is_arrow = False
        if not isinstance(obj, pd.DataFrame) and not is_arrow:
            raise TypeError(
                f"LanceIOManager only handles pandas DataFrames or pyarrow Tables; got {type(obj).__name__}. "
                f"Use a different IO manager for non-tabular outputs."
            )
        db = self._get_db()
        name = self._table_name(context)
        db.create_table(name, obj, mode="overwrite")
        row_count = len(obj) if isinstance(obj, pd.DataFrame) else obj.num_rows
        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(f"{self.base_path.rstrip('/')}/{name}.lance"),
                "row_count": dg.MetadataValue.int(row_count),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        db = self._get_db()
        name = self._table_name(context.upstream_output)
        return db.open_table(name).to_pandas()



class LanceIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LanceDB IO manager for ML-optimised columnar storage.

    Features:
      - Partitioned assets land in tables named ``<asset_key>__<partition_key>``
      - Multi-component asset keys (e.g. ``["features","embeddings","text"]``) are
        flattened with ``__`` so each combination becomes its own table
      - Output metadata records the table path, row count, and partition key
      - Backed by a local directory or any object-storage URI (S3, GCS,
        S3-compatible MinIO via ``s3_endpoint_url``)

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    base_path: str = Field(
        default="./lance_data",
        description="Directory (or S3/GCS URI) where LanceDB tables are stored",
    )
    table_name_prefix: Optional[str] = Field(
        default=None,
        description="Optional prefix prepended to every table name, e.g. 'prod_'",
    )
    s3_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS access key when using S3-based storage",
    )
    s3_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS secret key when using S3-based storage",
    )
    s3_endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom S3 endpoint URL, e.g. 'http://localhost:9000' for MinIO",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = LanceIOManager(
            base_path=self.base_path,
            table_name_prefix=self.table_name_prefix,
            s3_access_key=dg.EnvVar(self.s3_access_key_env_var) if self.s3_access_key_env_var else None,
            s3_secret_key=dg.EnvVar(self.s3_secret_key_env_var) if self.s3_secret_key_env_var else None,
            s3_endpoint_url=self.s3_endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
