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
            import os
            import subprocess
            import sys

            merged_env = {**os.environ, **env_vars}
            print(f"$ {cmd}", flush=True)
            # Stream stdout/stderr through the parent process so the
            # ComputeLogManager captures them. Do NOT `capture_output=True` here
            # — that swallows the streams and starves the CLM.
            proc = subprocess.Popen(
                cmd, shell=True, cwd=cwd, env=merged_env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1,
            )
            stdout_lines = []
            for line in proc.stdout or []:
                sys.stdout.write(line)
                sys.stdout.flush()
                stdout_lines.append(line)
            stderr_data = (proc.stderr.read() if proc.stderr else "") or ""
            if stderr_data:
                sys.stderr.write(stderr_data)
                sys.stderr.flush()
            proc.wait()
            if proc.returncode != 0:
                raise dg.Failure(
                    description=f"Shell command exited {proc.returncode}",
                    metadata={"stderr": dg.MetadataValue.text(stderr_data[:2000])},
                )
            return dg.MaterializeResult(metadata={
                "exit_code": dg.MetadataValue.int(proc.returncode),
                "stdout_preview": dg.MetadataValue.text("".join(stdout_lines)[:2000]),
            })
        return dg.Definitions(assets=[_shell_asset])

