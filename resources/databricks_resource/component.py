"""Databricks Resource component.

Works with Databricks on AWS, Databricks on GCP, and **Azure Databricks**
(set host to `https://adb-XXX.azuredatabricks.net`). Auth options:

  1. Personal Access Token (PAT) — default, works everywhere
  2. Microsoft Entra OAuth (Azure Databricks only) — set the Entra
     env vars; the Databricks SDK auto-detects and uses OAuth.

For the Entra path, omit `token_env_var` and ensure the runtime has
`ARM_TENANT_ID`, `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET` env vars set (or
`DATABRICKS_AZURE_RESOURCE_ID` for managed identity in Azure compute).
The Databricks SDK reads these natively.
"""
from typing import Optional
import os

import dagster as dg
from pydantic import Field


class DatabricksResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Databricks resource for use by other components.

    Compatible with Azure Databricks via the standard host URL pattern.
    Supports both PAT (token_env_var) and Entra OAuth (env-var-driven).
    """

    resource_key: str = Field(
        default="databricks_resource",
        description="Key used to register this resource.",
    )
    host: str = Field(
        description=(
            "Databricks workspace URL. Examples: "
            "'https://adb-1234567890.azuredatabricks.net' (Azure Databricks), "
            "'https://dbc-abcdef12-3456.cloud.databricks.com' (Databricks on AWS)."
        )
    )
    token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a Databricks PAT. Leave empty to use Entra OAuth "
            "(Azure Databricks only) — set ARM_TENANT_ID/CLIENT_ID/CLIENT_SECRET "
            "in the runtime, or rely on managed identity in Azure compute."
        ),
    )
    cluster_id: Optional[str] = Field(default=None, description="Default cluster ID for job submissions")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_databricks import DatabricksClientResource
        kwargs = {"host": self.host, "cluster_id": self.cluster_id}
        if self.token_env_var:
            kwargs["token"] = dg.EnvVar(self.token_env_var)
        # else: SDK auto-discovers auth from ARM_*/DATABRICKS_* env vars
        resource = DatabricksClientResource(**kwargs)
        return dg.Definitions(resources={self.resource_key: resource})
