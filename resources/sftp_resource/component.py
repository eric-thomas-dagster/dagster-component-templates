"""SFTP Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class SFTPResource(dg.ConfigurableResource):
    host: str
    port: int = 22
    username: str
    password: str = ""
    private_key_pem: str = ""
    private_key_path: str = ""

    def get_client(self):
        import paramiko
        import io
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SFTPResource(
            host=self.host,
            port=self.port,
            username=self.username,
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else "",
            private_key_pem=os.environ.get(self.private_key_env_var, "") if self.private_key_env_var else "",
            private_key_path=self.private_key_path or "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
