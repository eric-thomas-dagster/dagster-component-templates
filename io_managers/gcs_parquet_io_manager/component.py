"""GCS Parquet IO Manager component.

YAML/Component wrapper around `GCSParquetIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Any
import dagster as dg
from pydantic import Field
import pandas as pd


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



class GCSParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IO manager that stores assets as Parquet files on Google Cloud Storage.

    Features:
      - Partitioned assets land at ``<prefix>/<asset_key>/<partition_key>.parquet``
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested GCS paths, preventing collisions between assets of the same name
      - Output metadata records the GCS URI, row count, and partition key
      - Authenticates via ADC by default, or a JSON service-account key

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(description="GCS bucket to store Parquet files in")
    prefix: str = Field(
        default="dagster/assets",
        description="GCS object prefix for asset files, e.g. 'dagster/assets'",
    )
    project: Optional[str] = Field(default=None, description="GCP project ID (uses default if empty)")
    gcp_credentials_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding a JSON service-account key. Leave empty to use Application Default Credentials.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = GCSParquetIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            project=self.project,
            gcp_credentials_json=dg.EnvVar(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
