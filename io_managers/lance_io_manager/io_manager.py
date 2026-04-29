"""Lance IO Manager.

Stores Dagster assets as LanceDB tables — an ML-optimised columnar format
ideal for embeddings and vector data. Backed by a local directory or any
object-storage URI (S3, GCS, MinIO via S3-compat).

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


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
