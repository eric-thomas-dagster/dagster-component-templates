"""ShellCommandAssetComponent.

Wraps `dagster-shell`'s `execute_shell_command` so a shell command (or script) is a real Dagster asset — captured stdout/stderr in the run log, exit code → asset success/failure, optional cwd + env vars.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ShellCommandAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a shell command as a Dagster asset via dagster-shell."""

    asset_name: str = Field(description="Dagster asset name.")
    command: str = Field(description="Shell command (or multiline script) to execute.")
    cwd: Optional[str] = Field(default=None, description="Working directory for the command.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Additional env vars to pass to the shell.")
    group_name: str = Field(default="shell", description="Asset group.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_shell import execute_shell_command
        cmd = self.command
        cwd = self.cwd
        env_vars = self.env_vars or {}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or f"Run shell: {cmd[:60]}...",
            group_name=self.group_name,
            deps=deps_keys,
        )
        def _shell_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            output, return_code = execute_shell_command(
                shell_command=cmd, output_logging="STREAM", log=context.log,
                cwd=cwd, env=env_vars,
            )
            return dg.MaterializeResult(metadata={
                "exit_code": dg.MetadataValue.int(return_code),
                "stdout_preview": dg.MetadataValue.text(output[:2000] if output else ""),
            })
        return dg.Definitions(assets=[_shell_asset])

