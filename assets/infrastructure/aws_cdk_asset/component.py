import json
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Set

import dagster as dg
from pydantic import Field


class AWSCDKAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy AWS CDK stacks as a Dagster asset.

    Runs ``cdk deploy`` (or ``cdk destroy``) via subprocess, streaming output
    to the Dagster run log. Stack outputs are captured and surfaced as asset
    metadata, making them available to downstream assets.
    """

    asset_name: str = Field(
        description="Dagster asset key for this CDK deployment asset."
    )
    working_dir: str = Field(
        description=(
            "Filesystem path to the directory containing cdk.json and the CDK "
            "app entry point."
        )
    )
    stacks: Optional[list[str]] = Field(
        default=None,
        description=(
            "Specific CDK stack names to deploy. When None all stacks are "
            'deployed (equivalent to ``cdk deploy "*"``).'
        ),
    )
    context: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "CDK context key/value pairs passed as ``-c key=value`` arguments."
        ),
    )
    parameters: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "CloudFormation parameter overrides in "
            "``StackName:ParameterName=value`` format, passed via "
            "``--parameters``."
        ),
    )
    require_approval: str = Field(
        default="never",
        description=(
            'Approval level for security-sensitive changes. One of "never", '
            '"any-change", or "broadening". Use "never" for fully automated runs.'
        ),
    )
    exclusively: bool = Field(
        default=False,
        description=(
            "Pass ``--exclusively`` to deploy only the listed stacks without "
            "deploying their dependency stacks."
        ),
    )
    hotswap: bool = Field(
        default=False,
        description=(
            "Pass ``--hotswap`` for faster deployments of Lambda functions and "
            "ECS services without a full CloudFormation update. Recommended for "
            "development environments only."
        ),
    )
    rollback: bool = Field(
        default=True,
        description=(
            "Pass ``--rollback`` to roll back the stack on deployment failure. "
            "Set to False to leave the stack in a failed state for debugging."
        ),
    )
    outputs_file: Optional[str] = Field(
        default=None,
        description=(
            "Path where CDK writes stack outputs as JSON via ``--outputs-file``. "
            "When None a temporary path under /tmp is used automatically."
        ),
    )
    profile: Optional[str] = Field(
        default=None,
        description="AWS CLI profile name to use for authentication.",
    )
    region: Optional[str] = Field(
        default=None,
        description="AWS region override (e.g. ``us-east-1``).",
    )
    cdk_bin: str = Field(
        default="cdk",
        description=(
            "Path or name of the CDK CLI executable. Defaults to ``cdk`` which "
            "must be available on PATH (installed via ``npm install -g aws-cdk``)."
        ),
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Additional environment variables to inject into the CDK subprocess. "
            "Merged on top of the current process environment."
        ),
    )
    operation: str = Field(
        default="deploy",
        description='CDK operation to run. Either "deploy" or "destroy".',
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description surfaced in the Dagster UI.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description=(
            "Dagster asset keys that this asset depends on. Useful when "
            "downstream data assets need to declare a dependency on this "
            "provisioning asset."
        ),
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

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    @classmethod
    def get_component_schema(cls):
        return cls.schema()

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        # Capture all config as local variables for use inside the asset fn.
        asset_name = self.asset_name
        working_dir = self.working_dir
        stacks = self.stacks
        context_vars = self.context
        parameters = self.parameters
        require_approval = self.require_approval
        exclusively = self.exclusively
        hotswap = self.hotswap
        rollback = self.rollback
        outputs_file = self.outputs_file
        profile = self.profile
        region = self.region
        cdk_bin = self.cdk_bin
        env_vars = self.env_vars
        operation = self.operation
        group_name = self.group_name
        description = self.description
        deps = self.deps

        # Resolve the effective outputs file path once.
        effective_outputs_file = outputs_file or f"/tmp/{asset_name}_outputs.json"

        @dg.asset(
            name=asset_name,
            group_name=group_name,
            description=description,
            deps=deps or [],
        )
        def _cdk_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            # ----------------------------------------------------------------
            # Build the CDK command
            # ----------------------------------------------------------------
            cmd: list[str] = [cdk_bin, operation]

            # Stack selection
            if stacks:
                cmd.extend(stacks)
            else:
                cmd.append("*")

            # Core flags
            cmd.extend(["--require-approval", require_approval])
            cmd.extend(["--outputs-file", effective_outputs_file])

            if exclusively:
                cmd.append("--exclusively")

            if hotswap:
                cmd.append("--hotswap")

            if not rollback:
                cmd.append("--no-rollback")

            # Context variables: -c key=value
            for key, value in (context_vars or {}).items():
                cmd.extend(["-c", f"{key}={value}"])

            # CloudFormation parameters: --parameters StackName:Param=value
            for key, value in (parameters or {}).items():
                cmd.extend(["--parameters", f"{key}={value}"])

            if profile:
                cmd.extend(["--profile", profile])

            if region:
                cmd.extend(["--region", region])

            # Always run non-interactively
            cmd.append("--ci")

            context.log.info(f"Running CDK command: {' '.join(cmd)}")
            context.log.info(f"Working directory: {working_dir}")

            # ----------------------------------------------------------------
            # Execute with streaming output
            # ----------------------------------------------------------------
            proc = subprocess.Popen(
                cmd,
                cwd=working_dir,
                env={**os.environ, **(env_vars or {})},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            output_lines: list[str] = []
            assert proc.stdout is not None
            for line in iter(proc.stdout.readline, ""):
                stripped = line.rstrip()
                context.log.info(stripped)
                output_lines.append(stripped)

            proc.wait()

            if proc.returncode != 0:
                raise Exception(
                    f"CDK {operation} failed (exit {proc.returncode}). "
                    "Check the run log above for details."
                )

            # ----------------------------------------------------------------
            # Parse CDK output for deployed stack names and resource counts
            # ----------------------------------------------------------------
            deployed_stacks: list[str] = []
            for line in output_lines:
                # CDK prints "✅  StackName" on success lines
                if "✅" in line:
                    parts = line.split("✅", 1)
                    if len(parts) == 2:
                        stack_label = parts[1].strip()
                        # Strip trailing parenthetical resource counts
                        stack_label = stack_label.split("(")[0].strip()
                        if stack_label:
                            deployed_stacks.append(stack_label)

            # ----------------------------------------------------------------
            # Read stack outputs JSON if present
            # ----------------------------------------------------------------
            stack_outputs: dict = {}
            outputs_path = Path(effective_outputs_file)
            if outputs_path.exists():
                try:
                    with outputs_path.open() as fh:
                        stack_outputs = json.load(fh)
                    context.log.info(
                        f"Stack outputs loaded from {effective_outputs_file}"
                    )
                except Exception as exc:
                    context.log.warning(
                        f"Could not parse outputs file {effective_outputs_file}: {exc}"
                    )

            # ----------------------------------------------------------------
            # Surface metadata in Dagster
            # ----------------------------------------------------------------
            metadata: dict = {
                "stacks": dg.MetadataValue.json(
                    deployed_stacks if deployed_stacks else (stacks or ["*"])
                ),
                "outputs": dg.MetadataValue.json(stack_outputs),
                "working_dir": dg.MetadataValue.path(working_dir),
                "operation": dg.MetadataValue.text(operation),
                "cdk_command": dg.MetadataValue.text(" ".join(cmd)),
            }

            if outputs_path.exists():
                metadata["outputs_file"] = dg.MetadataValue.path(
                    effective_outputs_file
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_cdk_asset])
