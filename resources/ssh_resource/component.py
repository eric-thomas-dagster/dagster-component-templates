"""SSH Resource component.

Reusable SSH command-execution logic -- the real action behind this repo's
asset-shaped `ssh_asset` (`assets/ingestion/ssh_asset`), extracted as a
`ConfigurableResource` so any op or asset a user writes can call it directly,
instead of being locked inside one pre-built asset. Same `paramiko`-based
connection conventions as `ssh_asset`: host/username/port via env vars (or a
literal value), and the same private-key-path / private-key-content / password
auth cascade.
"""
import io
import os
import shlex
import tempfile
import warnings
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, unquote, parse_qsl

import dagster as dg
from pydantic import Field


def _parse_connection_uri(uri: str) -> Dict[str, Any]:
    """Parse an Airflow-style connection URI
    ('ssh://login:password@host:port?key_path=/path/to/key', per
    https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
    into its parts. Login/password are percent-decoded."""
    parts = urlsplit(uri)
    extra = dict(parse_qsl(parts.query))
    return {
        "login": unquote(parts.username) if parts.username else None,
        "password": unquote(parts.password) if parts.password else None,
        "host": parts.hostname,
        "port": parts.port,
        "key_path": extra.get("key_path"),
    }


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


def _load_private_key_with_fallback(loader):
    """Try RSA, then Ed25519, then ECDSA -- the same fallback chain used for
    PEM-content keys, applied to key-FILE loading too so both input paths support
    all three key types consistently. `loader(key_cls)` performs the actual load."""
    import paramiko

    last_exc: Optional[Exception] = None
    for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
        try:
            return loader(key_cls)
        except paramiko.SSHException as exc:
            last_exc = exc
    raise last_exc  # type: ignore[misc]


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
            pkey = _load_private_key_with_fallback(lambda key_cls: key_cls.from_private_key_file(self.private_key_path))
        elif self.private_key:
            pkey = _load_private_key_with_fallback(lambda key_cls: key_cls.from_private_key(io.StringIO(self.private_key)))
            with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
                kf.write(self.private_key)
                tmp_key_file = kf.name
            os.chmod(tmp_key_file, 0o600)

        client = paramiko.SSHClient()
        if not self.known_hosts_check:
            warnings.warn(
                "ssh_resource: known_hosts_check=False -- accepting ANY remote host key "
                "(paramiko.AutoAddPolicy). This accepts a man-in-the-middle host silently. "
                "Set known_hosts_check=True for any security-sensitive connection.",
                stacklevel=2,
            )
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
                full_command = f"cd {shlex.quote(working_dir)} && {full_command}"
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
    `username_env_var` (or the literal `host`/`username`) required; auth via
    `private_key_path_env_var` (file already on disk, takes precedence),
    `private_key_env_var` (PEM content, written to a secure temp file), or
    `password_env_var` (fallback), in that order.
    """

    resource_key: str = Field(default="ssh_resource", description="Key used to register this resource. Other components/assets reference it via resource_key.")
    host: Optional[str] = Field(default=None, description="SSH host/IP, as a literal value. Set this OR host_env_var OR connection_uri_env_var.")
    host_env_var: Optional[str] = Field(default=None, description="Env var containing the SSH host/IP. Set this OR host.")
    connection_uri_env_var: Optional[str] = Field(default=None, description="Env var holding an Airflow-style connection URI ('ssh://login:password@host:port?key_path=/path/to/key') as an alternative to setting host/username/password_env_var individually. Explicit fields, if also set, win over the URI's corresponding value.")
    username: Optional[str] = Field(default=None, description="SSH username, as a literal value. Set this OR username_env_var.")
    username_env_var: Optional[str] = Field(default=None, description="Env var containing the SSH username. Set this OR username.")
    port: int = Field(default=22, description="SSH port.")
    password_env_var: Optional[str] = Field(default=None, description="Env var containing the SSH password. Fallback when no private key option is set.")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var containing PEM-encoded private key content.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var containing a path to a private key file already on disk. Takes precedence over private_key_env_var when both are set.")
    known_hosts_check: bool = Field(default=False, description="When False (default), accept any host key (AutoAddPolicy) -- logs a warning on every connection. When True, use the system known_hosts file and reject unknown host keys.")
    timeout_seconds: int = Field(default=300, description="Default connection + per-command timeout in seconds.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='SSH Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        uri_parsed: Optional[Dict[str, Any]] = None
        if self.connection_uri_env_var:
            uri_value = dg.EnvVar(self.connection_uri_env_var).get_value()
            if uri_value:
                uri_parsed = _parse_connection_uri(uri_value)

        if self.host:
            host: Any = self.host
        elif self.host_env_var:
            host = dg.EnvVar(self.host_env_var)
        elif uri_parsed and uri_parsed["host"]:
            host = uri_parsed["host"]
        else:
            raise ValueError("Set either 'host', 'host_env_var', or 'connection_uri_env_var' (with a URI that includes a host).")

        if self.username:
            username: Any = self.username
        elif self.username_env_var:
            username = dg.EnvVar(self.username_env_var)
        elif uri_parsed and uri_parsed["login"]:
            username = uri_parsed["login"]
        else:
            raise ValueError("Set either 'username', 'username_env_var', or 'connection_uri_env_var' (with a URI that includes a login).")

        port = self.port
        if port == 22 and uri_parsed and uri_parsed["port"]:
            port = uri_parsed["port"]

        password: Any = ""
        if self.password_env_var:
            password = dg.EnvVar(self.password_env_var)
        elif uri_parsed and uri_parsed["password"]:
            password = uri_parsed["password"]

        private_key_path: Any = ""
        if self.private_key_path_env_var:
            private_key_path = dg.EnvVar(self.private_key_path_env_var)
        elif uri_parsed and uri_parsed["key_path"]:
            private_key_path = uri_parsed["key_path"]

        resource = SSHResource(
            host=host,
            username=username,
            port=port,
            password=password,
            private_key=dg.EnvVar(self.private_key_env_var) if self.private_key_env_var else "",
            private_key_path=private_key_path,
            known_hosts_check=self.known_hosts_check,
            timeout_seconds=self.timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
