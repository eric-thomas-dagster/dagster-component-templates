"""dbt Cloud Resource component."""
from dataclasses import dataclass
import dagster as dg
from pydantic import Field


@dataclass
class DbtCloudResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-dbt DbtCloudClientResource for use by other components."""

    resource_key: str = Field(default="dbt_cloud_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    auth_token_env_var: str = Field(description="Environment variable holding the dbt Cloud API token")
    account_id: int = Field(description="dbt Cloud account ID (numeric)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_dbt import DbtCloudClientResource
        resource = DbtCloudClientResource(
            auth_token=dg.EnvVar(self.auth_token_env_var),
            account_id=self.account_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
