"""SSH Resource component.

Reusable SSH command-execution logic -- the real action behind this repo's
asset-shaped `ssh_asset` (`assets/ingestion/ssh_asset`), extracted as a
`ConfigurableResource` so any op or asset a user writes can call it directly,
instead of being locked inside one pre-built asset. Same `paramiko`-based
connection conventions as `ssh_asset`: host/username/port via env vars, and
the same private-key-path / private-key-content / password auth cascade.
"""
import io
import os
import shlex
import tempfile
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


def _build_command_with_environment(command: str, environment: Optional[Dict[str, str]]) -> str:
    """Prefix `command` with `export K=V; ` for each entry in `environment`, safely
    shell-quoted. Deliberately NOT using paramiko's `exec_command(environment=...)`
    kwarg -- that silently does nothing unless the remote sshd's `AcceptEnv` config
    allows each specific variable name, which is rarely true outside a controlled
    fleet. A shell-level export always works regardless of server config."""
    if not environment:
        return command
    exports = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in environment.items())
    return f"export {exports}; {command}"


class SSHResource(dg.ConfigurableResource):
    """Open an SSH connection and run one or more commands, returning stdout/stderr/exit
    code per command -- the reusable action behind the Airflow `SSHOperator` equivalent."""

    host: str
    username: str
    port: int = 22
    password: str = ""
    private_key: str = ""
    private_key_path: str = ""
    known_hosts_check: bool = False
    timeout_seconds: int = 300

    def _connect(self):
        import paramiko

        pkey = None
        tmp_key_file: Optional[str] = None
        if self.private_key_path:
            pkey = paramiko.RSAKey.from_private_key_file(self.private_key_path)
        elif self.private_key:
            try:
                pkey = paramiko.RSAKey.from_private_key(io.StringIO(self.private_key))
            except paramiko.SSHException:
                try:
                    pkey = paramiko.Ed25519Key.from_private_key(io.StringIO(self.private_key))
                except paramiko.SSHException:
                    pkey = paramiko.ECDSAKey.from_private_key(io.StringIO(self.private_key))
            with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
                kf.write(self.private_key)
                tmp_key_file = kf.name
            os.chmod(tmp_key_file, 0o600)

        client = paramiko.SSHClient()
        if not self.known_hosts_check:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        else:
            client.load_system_host_keys()

        client.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            pkey=pkey,
            password=self.password or None,
            timeout=self.timeout_seconds,
        )
        return client, tmp_key_file

    def run_command(
        self,
        command: str,
        timeout: Optional[int] = None,
        environment: Optional[Dict[str, str]] = None,
        working_dir: Optional[str] = None,
        log=None,
    ) -> Dict[str, Any]:
        """Run ONE command over a fresh SSH connection. Returns
        `{"stdout": str, "stderr": str, "exit_code": int}`. Does not raise on a
        non-zero exit code -- the caller decides whether that's a failure."""
        client, tmp_key_file = self._connect()
        try:
            full_command = command
            if working_dir:
                full_command = f"cd {working_dir} && {full_command}"
            full_command = _build_command_with_environment(full_command, environment)

            if log:
                log.info(f"ssh_resource: running on {self.host}: {full_command}")

            stdin, stdout, stderr = client.exec_command(full_command, timeout=timeout if timeout is not None else self.timeout_seconds)
            stdin.close()
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            exit_code = stdout.channel.recv_exit_status()
            return {"stdout": stdout_text, "stderr": stderr_text, "exit_code": exit_code}
        finally:
            client.close()
            if tmp_key_file:
                try:
                    os.unlink(tmp_key_file)
                except OSError:
                    pass


class SSHResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an SSHResource (run_command over a real paramiko SSH connection) for
    use by other components, ops, or assets -- the Airflow `SSHOperator` equivalent
    as reusable, injectable logic rather than a single-purpose asset.

    Reuses `ssh_asset`'s exact auth env-var convention: `host_env_var`/
    `username_env_var` required; auth via `private_key_path_env_var` (file already
    on disk, takes precedence), `private_key_env_var` (PEM content, written to a
    secure temp file), or `password_env_var` (fallback), in that order.
    """

    resource_key: str = Field(default="ssh_resource", description="Key used to register this resource. Other components/assets reference it via resource_key.")
    host_env_var: str = Field(description="Env var containing the SSH host/IP.")
    username_env_var: str = Field(description="Env var containing the SSH username.")
    port: int = Field(default=22, description="SSH port.")
    password_env_var: Optional[str] = Field(default=None, description="Env var containing the SSH password. Fallback when no private key option is set.")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var containing PEM-encoded private key content.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var containing a path to a private key file already on disk. Takes precedence over private_key_env_var when both are set.")
    known_hosts_check: bool = Field(default=False, description="When False (default), accept any host key (AutoAddPolicy). When True, use the system known_hosts file and reject unknown host keys.")
    timeout_seconds: int = Field(default=300, description="Default connection + per-command timeout in seconds.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='SSH Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SSHResource(
            host=dg.EnvVar(self.host_env_var),
            username=dg.EnvVar(self.username_env_var),
            port=self.port,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else "",
            private_key=dg.EnvVar(self.private_key_env_var) if self.private_key_env_var else "",
            private_key_path=dg.EnvVar(self.private_key_path_env_var) if self.private_key_path_env_var else "",
            known_hosts_check=self.known_hosts_check,
            timeout_seconds=self.timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
