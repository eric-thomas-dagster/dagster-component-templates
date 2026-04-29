"""SFTP Resource component."""
from dataclasses import dataclass
from typing import Optional
import dagster as dg
from pydantic import Field


class SFTPResource(dg.ConfigurableResource):
    host: str
    port: int = 22
    username: str
    password: str = ""
    private_key_pem: str = ""
    private_key_path: str = ""
    known_hosts_path: str = ""
    auto_accept_unknown_host_key: bool = False

    def get_client(self):
        import paramiko
        import io
        ssh = paramiko.SSHClient()
        if self.known_hosts_path:
            ssh.load_host_keys(self.known_hosts_path)
        else:
            ssh.load_system_host_keys()
        if self.auto_accept_unknown_host_key:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        else:
            ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        connect_kwargs = {"hostname": self.host, "port": self.port, "username": self.username}
        if self.private_key_pem:
            pkey = paramiko.RSAKey.from_private_key(io.StringIO(self.private_key_pem))
            connect_kwargs["pkey"] = pkey
        elif self.private_key_path:
            connect_kwargs["key_filename"] = self.private_key_path
        elif self.password:
            connect_kwargs["password"] = self.password
        ssh.connect(**connect_kwargs)
        return ssh.open_sftp()


@dataclass
class SFTPResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an SFTP resource for reading and writing files over SSH."""

    resource_key: str = Field(default="sftp_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="SFTP server hostname or IP")
    port: int = Field(default=22, description="SFTP port")
    username: str = Field(description="SSH username")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the SSH password")
    private_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the SSH private key PEM string")
    private_key_path: Optional[str] = Field(default=None, description="Path to SSH private key file on disk")
    known_hosts_path: Optional[str] = Field(
        default=None,
        description="Path to a known_hosts file. If empty, the system known_hosts (~/.ssh/known_hosts) is loaded.",
    )
    auto_accept_unknown_host_key: bool = Field(
        default=False,
        description="If True, automatically accept unknown host keys (insecure — enables MITM). Default False rejects unknown hosts.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SFTPResource(
            host=self.host,
            port=self.port,
            username=self.username,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else "",
            private_key_pem=dg.EnvVar(self.private_key_env_var) if self.private_key_env_var else "",
            private_key_path=self.private_key_path or "",
            known_hosts_path=self.known_hosts_path or "",
            auto_accept_unknown_host_key=self.auto_accept_unknown_host_key,
        )
        return dg.Definitions(resources={self.resource_key: resource})
