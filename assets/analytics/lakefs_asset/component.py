"""LakeFS Asset Component.

A control-plane Dagster asset that runs git-like operations on a lakeFS
repository: `commit`, `merge`, or `create_branch`. Does NOT read or write
data — pair with `io_managers/lakefs_io_manager` for the data plane.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class LakeFSAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a lakeFS control-plane operation as a Dagster asset.

    Three operation modes:

    - ``commit``         — commit staged changes on a branch with a message
                           and optional metadata. Use after data assets have
                           materialized via ``lakefs_io_manager`` to seal
                           those staged writes into a versioned snapshot.
    - ``merge``          — merge a source branch into a destination branch.
    - ``create_branch``  — create a new branch from a source ref.

    This asset performs lakeFS API calls only — it does not read or write
    data files. Declare upstream data assets via ``deps`` so the lakeFS
    operation runs after they've materialized; pair with the
    ``lakefs_io_manager`` IO manager component for the data plane.

    All lakeFS operations use the official `lakefs` Python SDK.
    """

    # --- lakeFS connection config ---------------------------------------------
    endpoint: str = Field(
        description="lakeFS endpoint URL, e.g. 'http://localhost:8000' or 'https://lakefs.my-company.com'."
    )
    access_key_env_var: str = Field(
        default="LAKEFS_ACCESS_KEY_ID",
        description="Environment variable holding the lakeFS access key ID.",
    )
    secret_key_env_var: str = Field(
        default="LAKEFS_SECRET_ACCESS_KEY",
        description="Environment variable holding the lakeFS secret access key.",
    )

    # --- Repository and operation config -------------------------------------
    repository: str = Field(description="lakeFS repository name.")
    mode: str = Field(
        default="commit",
        description=(
            "Operation: 'commit' (commit staged changes on a branch), "
            "'merge' (merge source branch into destination), "
            "'create_branch' (create a new branch from a source ref)."
        ),
    )
    branch: str = Field(
        default="main",
        description="Branch to commit to (commit mode), or the source branch (merge / create_branch mode).",
    )
    destination_branch: Optional[str] = Field(
        default=None,
        description="Target branch for merge mode.",
    )
    source_ref: Optional[str] = Field(
        default=None,
        description="Source ref (branch or commit SHA) for create_branch mode.",
    )
    commit_message: str = Field(
        default="Dagster materialization commit",
        description="Commit message used in commit mode.",
    )
    metadata: Optional[dict[str, str]] = Field(
        default=None,
        description="Key-value metadata attached to the commit (commit mode only).",
    )

    # --- Asset metadata -------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")
    group_name: Optional[str] = Field(
        default="data_versioning",
        description="Dagster asset group name.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on. Use to declare ordering — typically the data assets whose writes this commit/merge should seal.",
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        endpoint = self.endpoint
        access_key = dg.EnvVar(self.access_key_env_var)
        secret_key = dg.EnvVar(self.secret_key_env_var)
        repository = self.repository
        mode = self.mode
        branch = self.branch
        destination_branch = self.destination_branch
        source_ref = self.source_ref
        commit_message = self.commit_message
        commit_metadata = self.metadata
        asset_name = self.asset_name
        group_name = self.group_name
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        if mode == "merge" and not destination_branch:
            raise ValueError("destination_branch must be set when mode is 'merge'.")
        if mode == "create_branch" and not source_ref:
            raise ValueError("source_ref must be set when mode is 'create_branch'.")
        if mode not in ("commit", "merge", "create_branch"):
            raise ValueError(f"Unknown mode '{mode}'. Expected: commit, merge, create_branch.")

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(asset_name),
            group_name=group_name,
            deps=dep_keys,
            kinds={"lakefs", "storage"},
        )
        def _lakefs_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import lakefs
            from lakefs.client import Client

            client = Client(host=endpoint, username=access_key.get_value(), password=secret_key.get_value())
            repo = lakefs.Repository(repository, client=client)

            if mode == "commit":
                context.log.info(
                    f"lakeFS commit: repo={repository} branch={branch} message='{commit_message}'"
                )
                ref = repo.branch(branch).commit(
                    message=commit_message,
                    metadata=commit_metadata or {},
                )
                commit_id = ref.get_commit().id
                context.log.info(f"Committed: {commit_id}")
                return dg.MaterializeResult(
                    metadata={
                        "mode": dg.MetadataValue.text(mode),
                        "repository": dg.MetadataValue.text(repository),
                        "branch": dg.MetadataValue.text(branch),
                        "commit_id": dg.MetadataValue.text(commit_id),
                        "commit_message": dg.MetadataValue.text(commit_message),
                    }
                )

            if mode == "merge":
                context.log.info(
                    f"lakeFS merge: {branch} -> {destination_branch} (repo={repository})"
                )
                merge_ref = repo.branch(branch).merge_into(destination_branch)
                ref_id = getattr(merge_ref, "id", str(merge_ref))
                context.log.info(f"Merge complete: {ref_id}")
                return dg.MaterializeResult(
                    metadata={
                        "mode": dg.MetadataValue.text(mode),
                        "repository": dg.MetadataValue.text(repository),
                        "source_branch": dg.MetadataValue.text(branch),
                        "destination_branch": dg.MetadataValue.text(destination_branch),
                        "reference": dg.MetadataValue.text(ref_id),
                    }
                )

            # mode == "create_branch"
            context.log.info(
                f"lakeFS create_branch: '{branch}' from '{source_ref}' (repo={repository})"
            )
            new_branch = repo.branch(branch).create(source_reference=source_ref)
            context.log.info(f"Branch created: {new_branch.id}")
            return dg.MaterializeResult(
                metadata={
                    "mode": dg.MetadataValue.text(mode),
                    "repository": dg.MetadataValue.text(repository),
                    "new_branch": dg.MetadataValue.text(branch),
                    "source_ref": dg.MetadataValue.text(source_ref),
                }
            )

        return dg.Definitions(assets=[_lakefs_asset])
