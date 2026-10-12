"""LakeFS IO Manager component.

YAML/Component wrapper around `LakeFSIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field
from datetime import datetime, timezone
from typing import Any, Optional
import pandas as pd


def _sanitize(component: str) -> str:
    """Replace characters that produce confusing or invalid lakeFS object keys."""
    return component.replace("[", "--").replace("]", "--").replace(" ", "_")


class LakeFSIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Parquet on lakeFS."""

    repository: str = Field(description="lakeFS repository name")
    branch: str = Field(default="main", description="Branch within the repository")
    endpoint_url: str = Field(description="lakeFS server endpoint URL, e.g. 'http://lakefs:8000'")
    prefix: str = Field(default="dagster/assets", description="Key prefix within the branch")
    access_key: Optional[str] = Field(default=None, description="lakeFS access key (resolved from env via dg.EnvVar)")
    secret_key: Optional[str] = Field(default=None, description="lakeFS secret key (resolved from env via dg.EnvVar)")
    commit_per_materialization: bool = Field(
        default=False,
        description="If True, call the lakeFS API after each handle_output to commit the change to the branch",
    )
    commit_message_template: str = Field(
        default="Materialized {asset_key} at {timestamp}",
        description="Format string for commit messages. Substitutions: {asset_key}, {partition_key}, {timestamp}, {row_count}",
    )

    def _get_fs(self):
        """S3-compatible filesystem talking to the lakeFS S3 gateway."""
        import s3fs
        kwargs: dict[str, Any] = {
            "client_kwargs": {"endpoint_url": self.endpoint_url},
        }
        if self.access_key:
            kwargs["key"] = self.access_key
        if self.secret_key:
            kwargs["secret"] = self.secret_key
        return s3fs.S3FileSystem(**kwargs)

    def _object_path(self, context) -> str:
        """Build the lakeFS object path for an asset (or output).

        Layout: ``<repository>/<branch>/<prefix>/<asset_path>/<partition_key>.parquet``
        For non-partitioned assets the partition segment is omitted:
        ``<repository>/<branch>/<prefix>/<asset_path>.parquet``
        """
        if context.has_asset_key:
            key_parts = [_sanitize(p) for p in context.asset_key.path]
            base = "/".join(key_parts)
        else:
            base = "/".join(_sanitize(p) for p in (context.run_id, context.step_key, context.name))
        if context.has_partition_key:
            return f"{self.repository}/{self.branch}/{self.prefix}/{base}/{_sanitize(context.partition_key)}.parquet"
        return f"{self.repository}/{self.branch}/{self.prefix}/{base}.parquet"

    def _format_commit_message(self, context: dg.OutputContext, row_count: int) -> str:
        asset_key = "/".join(context.asset_key.path) if context.has_asset_key else context.step_key
        partition_key = str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
        timestamp = datetime.now(timezone.utc).isoformat()
        return self.commit_message_template.format(
            asset_key=asset_key,
            partition_key=partition_key,
            timestamp=timestamp,
            row_count=row_count,
        )

    def _commit(self, context: dg.OutputContext, row_count: int) -> Optional[str]:
        """Commit the current branch state to lakeFS via the official `lakefs` SDK."""
        try:
            import lakefs
            from lakefs.client import Client
        except ImportError:
            raise ImportError(
                "commit_per_materialization=True requires the `lakefs` package. "
                "Install it with `pip install lakefs`."
            )
        client = Client(
            host=self.endpoint_url,
            username=self.access_key or "",
            password=self.secret_key or "",
        )
        repo = lakefs.Repository(self.repository, client=client)
        branch = repo.branch(self.branch)
        message = self._format_commit_message(context, row_count)
        commit = branch.commit(message=message)
        commit_id = getattr(commit, "id", None) or getattr(commit, "get_commit", lambda: None)()
        return str(commit_id) if commit_id else None

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"LakeFSIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        path = self._object_path(context)
        with self._get_fs().open(path, "wb") as f:
            obj.to_parquet(f, index=False)

        metadata: dict[str, Any] = {
            "object_path": dg.MetadataValue.path(f"s3://{path}"),
            "lakefs_uri": dg.MetadataValue.path(
                f"lakefs://{self.repository}/{self.branch}/{self.prefix}/"
                + "/".join(_sanitize(p) for p in (context.asset_key.path if context.has_asset_key else []))
            ),
            "row_count": dg.MetadataValue.int(len(obj)),
            "branch": dg.MetadataValue.text(self.branch),
            "partition_key": dg.MetadataValue.text(
                str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
            ),
        }

        if self.commit_per_materialization:
            commit_id = self._commit(context, len(obj))
            if commit_id:
                metadata["commit_id"] = dg.MetadataValue.text(commit_id)

        context.add_output_metadata(metadata)

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        path = self._object_path(context.upstream_output)
        with self._get_fs().open(path, "rb") as f:
            return pd.read_parquet(f)



class LakeFSIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a lakeFS IO manager that stores assets as Parquet on a versioned data lake.

    LakeFS (https://lakefs.io) is a Git-for-data layer over S3-compatible storage:
    every write goes to a *branch*, and *commits* snapshot the branch state. This
    manager writes Parquet via the lakeFS S3 gateway and (optionally) commits
    after each materialization so every asset version is reproducible.

    Features:
      - Partitioned assets land at ``<prefix>/<asset_key>/<partition_key>.parquet`` on the branch
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested object paths, preventing collisions between same-named assets
      - Output metadata records the lakeFS URI, branch, row count, partition key,
        and (when commit_per_materialization is True) the commit ID
      - Optional auto-commit per materialization via ``commit_per_materialization=True``
        with a configurable ``commit_message_template``

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    repository: str = Field(description="lakeFS repository name")
    branch: str = Field(default="main", description="Branch within the repository")
    endpoint_url: str = Field(
        description="lakeFS server endpoint URL, e.g. 'http://lakefs:8000'",
    )
    prefix: str = Field(
        default="dagster/assets",
        description="Key prefix within the branch, e.g. 'dagster/assets'",
    )
    lakefs_access_key_env_var: str = Field(
        description="Environment variable holding the lakeFS access key",
    )
    lakefs_secret_key_env_var: str = Field(
        description="Environment variable holding the lakeFS secret key",
    )
    commit_per_materialization: bool = Field(
        default=False,
        description="If True, commit the branch after every successful materialization",
    )
    commit_message_template: str = Field(
        default="Materialized {asset_key} at {timestamp}",
        description="Format string. Available substitutions: {asset_key}, {partition_key}, {timestamp}, {row_count}",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = LakeFSIOManager(
            repository=self.repository,
            branch=self.branch,
            endpoint_url=self.endpoint_url,
            prefix=self.prefix,
            access_key=dg.EnvVar(self.lakefs_access_key_env_var) if self.lakefs_access_key_env_var else None,
            secret_key=dg.EnvVar(self.lakefs_secret_key_env_var) if self.lakefs_secret_key_env_var else None,
            commit_per_materialization=self.commit_per_materialization,
            commit_message_template=self.commit_message_template,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
