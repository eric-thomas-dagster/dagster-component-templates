"""External SFTP Path Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalSftpPathAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an SFTP directory as an observable external asset."""
    asset_key: str = Field(description="Dagster asset key")
    host: str = Field(description="SFTP host")
    remote_path: str = Field(description="Remote directory path")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"SFTP {self.host}{self.remote_path}",
            kinds={"sftp", "files"},
            metadata={
                "host": self.host,
                "remote_path": self.remote_path,
                "dagster/uri": f"sftp://{self.host}{self.remote_path}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
