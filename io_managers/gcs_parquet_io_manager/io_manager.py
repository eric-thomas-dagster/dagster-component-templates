"""GCS Parquet IO Manager.

Stores Dagster assets as Parquet files on Google Cloud Storage. Supports
partitioned assets, multi-component asset keys, and ADC or service-account
JSON authentication.

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid GCS keys."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


class GCSParquetIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Parquet on GCS."""

    bucket: str = Field(description="GCS bucket to store Parquet files in")
    prefix: str = Field(default="dagster/assets", description="Object prefix under the bucket")
    project: Optional[str] = Field(default=None, description="GCP project ID (uses default if empty)")
    gcp_credentials_json: Optional[str] = Field(
        default=None,
        description="JSON service-account key contents (resolved from env via dg.EnvVar). Uses ADC if empty.",
    )

    def _get_fs(self):
        import gcsfs
        kwargs: dict[str, Any] = {}
        if self.project:
            kwargs["project"] = self.project
        if self.gcp_credentials_json:
            import json
            kwargs["token"] = json.loads(self.gcp_credentials_json)
        return gcsfs.GCSFileSystem(**kwargs)

    def _object_path(self, context) -> str:
        """Build the GCS path for an asset (or output).

        Layout: ``<bucket>/<prefix>/<asset_path>/<partition_key>.parquet``
        For non-partitioned assets the partition segment is omitted:
        ``<bucket>/<prefix>/<asset_path>.parquet``
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            base = "/".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        if context.has_partition_key:
            return f"{self.bucket}/{self.prefix}/{base}/{_sanitize(context.partition_key)}.parquet"
        return f"{self.bucket}/{self.prefix}/{base}.parquet"

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"GCSParquetIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        path = self._object_path(context)
        with self._get_fs().open(path, "wb") as f:
            obj.to_parquet(f, index=False)
        context.add_output_metadata(
            {
                "object_path": dg.MetadataValue.path(f"gs://{path}"),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        path = self._object_path(context.upstream_output)
        with self._get_fs().open(path, "rb") as f:
            return pd.read_parquet(f)
