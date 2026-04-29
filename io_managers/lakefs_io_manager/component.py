"""LakeFS IO Manager component.

YAML/Component wrapper around `LakeFSIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field

from .io_manager import LakeFSIOManager


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
