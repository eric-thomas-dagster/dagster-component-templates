import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class LakeFSAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Interact with a lakeFS repository for data versioning as a Dagster asset.

    Supports three operation modes:

    - ``commit``         — commit staged changes on a branch with a message and
                           optional metadata.
    - ``merge``          — merge a source branch into a destination branch.
    - ``create_branch``  — create a new branch from a source ref.

    All lakeFS operations are performed via the lakeFS REST API using
    ``requests`` with HTTP Basic authentication.
    """

    # --- lakeFS connection config ---------------------------------------------
    lakefs_endpoint_env_var: str = Field(
        default="LAKEFS_ENDPOINT",
        description=(
            "Name of the environment variable holding the lakeFS endpoint URL "
            "(e.g. http://localhost:8000)."
        ),
    )
    access_key_env_var: str = Field(
        default="LAKEFS_ACCESS_KEY_ID",
        description="Name of the environment variable holding the lakeFS access key ID.",
    )
    secret_key_env_var: str = Field(
        default="LAKEFS_SECRET_ACCESS_KEY",
        description="Name of the environment variable holding the lakeFS secret access key.",
    )

    # --- Repository and operation config -------------------------------------
    repository: str = Field(
        description="lakeFS repository name."
    )
    mode: str = Field(
        default="commit",
        description=(
            "Operation mode. One of: "
            "'commit' (commit staged changes on a branch), "
            "'merge' (merge source branch into destination branch), "
            "'create_branch' (create a new branch from a source ref)."
        ),
    )
    branch: str = Field(
        default="main",
        description=(
            "Branch to commit to (commit mode), or the source branch "
            "(merge / create_branch mode)."
        ),
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
    group_name: Optional[str] = Field(
        default="data_versioning",
        description="Dagster asset group name.",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    asset_name: str = Field(
        description="Dagster asset key for this component.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _get_credentials(self) -> tuple[str, str, str]:
        """Resolve lakeFS endpoint and credentials from environment variables."""
        endpoint = os.environ.get(self.lakefs_endpoint_env_var)
        if not endpoint:
            raise ValueError(
                f"Environment variable '{self.lakefs_endpoint_env_var}' "
                "is not set or is empty."
            )
        access_key = os.environ.get(self.access_key_env_var)
        if not access_key:
            raise ValueError(
                f"Environment variable '{self.access_key_env_var}' "
                "is not set or is empty."
            )
        secret_key = os.environ.get(self.secret_key_env_var)
        if not secret_key:
            raise ValueError(
                f"Environment variable '{self.secret_key_env_var}' "
                "is not set or is empty."
            )
        return endpoint.rstrip("/"), access_key, secret_key

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        dep_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            deps=dep_keys,
            kinds={"lakefs", "storage"},
        )
        def _lakefs_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import requests

            endpoint, access_key, secret_key = component._get_credentials()
            auth = (access_key, secret_key)
            base = f"{endpoint}/api/v1/repositories/{component.repository}"

            mode = component.mode

            if mode == "commit":
                url = f"{base}/branches/{component.branch}/commits"
                payload: dict = {"message": component.commit_message}
                if component.metadata:
                    payload["metadata"] = component.metadata

                context.log.info(
                    f"Committing to lakeFS: repo={component.repository}, "
                    f"branch={component.branch}, message='{component.commit_message}'"
                )
                response = requests.post(url, json=payload, auth=auth)
                response.raise_for_status()

                result = response.json()
                commit_id = result.get("id", "unknown")
                context.log.info(f"Committed: {commit_id}")

                return dg.MaterializeResult(
                    metadata={
                        "mode": mode,
                        "repository": component.repository,
                        "branch": component.branch,
                        "commit_id": commit_id,
                        "commit_message": component.commit_message,
                    }
                )

            elif mode == "merge":
                if not component.destination_branch:
                    raise ValueError(
                        "destination_branch must be set when mode is 'merge'."
                    )
                url = (
                    f"{base}/refs/{component.branch}/merge/"
                    f"{component.destination_branch}"
                )
                context.log.info(
                    f"Merging lakeFS branch: {component.branch} -> "
                    f"{component.destination_branch} "
                    f"(repo={component.repository})"
                )
                response = requests.post(url, json={}, auth=auth)
                response.raise_for_status()

                result = response.json()
                reference = result.get("reference", "unknown")
                context.log.info(f"Merge complete: {reference}")

                return dg.MaterializeResult(
                    metadata={
                        "mode": mode,
                        "repository": component.repository,
                        "source_branch": component.branch,
                        "destination_branch": component.destination_branch,
                        "reference": reference,
                    }
                )

            elif mode == "create_branch":
                if not component.source_ref:
                    raise ValueError(
                        "source_ref must be set when mode is 'create_branch'."
                    )
                url = f"{base}/branches"
                payload = {
                    "name": component.branch,
                    "source": component.source_ref,
                }
                context.log.info(
                    f"Creating lakeFS branch '{component.branch}' "
                    f"from ref '{component.source_ref}' "
                    f"(repo={component.repository})"
                )
                response = requests.post(url, json=payload, auth=auth)
                response.raise_for_status()

                new_branch = response.json() if response.text.strip() else component.branch
                context.log.info(f"Branch created: {component.branch}")

                return dg.MaterializeResult(
                    metadata={
                        "mode": mode,
                        "repository": component.repository,
                        "new_branch": component.branch,
                        "source_ref": component.source_ref,
                    }
                )

            else:
                raise ValueError(
                    f"Unknown mode '{mode}'. Expected one of: commit, merge, create_branch."
                )

        return dg.Definitions(assets=[_lakefs_asset])
