"""Azure Key Vault Resource.

Inject as a Dagster resource into ops/assets that need to read secrets
at runtime. Replaces the per-component `*_env_var` fields with a
centralized secret-management pattern that respects the standard Azure
RBAC model (Key Vault Secrets User role).

Example:

    @asset
    def my_asset(kv: KeyVaultResource):
        db_password = kv.get("postgres-admin-password")
        # ...

Or via the standard env-var pattern: pass `KV_PREFIX_NAME` (e.g.
`KV__POSTGRES_PASSWORD`) and the resource will resolve it on-demand.

Auth: standard DefaultAzureCredential chain. The principal needs
"Key Vault Secrets User" role on the vault (RBAC mode) or a Get-Secret
access policy (legacy mode).
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class KeyVaultResource(dg.ConfigurableResource):
    """Azure Key Vault client for runtime secret retrieval."""

    vault_url: str = Field(
        description="Vault URL (e.g. https://my-kv.vault.azure.net)"
    )
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def _client(self):
        try:
            from azure.identity import DefaultAzureCredential, ClientSecretCredential
            from azure.keyvault.secrets import SecretClient
        except ImportError as e:
            raise ImportError(
                "azure-identity + azure-keyvault-secrets required: "
                "pip install azure-identity azure-keyvault-secrets"
            ) from e

        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        else:
            cred = DefaultAzureCredential()
        return SecretClient(vault_url=self.vault_url, credential=cred)

    def get(self, name: str, version: Optional[str] = None) -> str:
        """Get a secret value. Raises ResourceNotFoundError if missing."""
        secret = self._client().get_secret(name, version=version)
        return secret.value

    def try_get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Get a secret value, returning default (or None) if not found."""
        try:
            return self.get(name)
        except Exception:
            return default

    def set(self, name: str, value: str, content_type: Optional[str] = None) -> None:
        """Set a secret value. Useful for rotation jobs."""
        self._client().set_secret(name, value, content_type=content_type)

    def list_names(self) -> list:
        """Enumerate secret names in the vault (does not include values)."""
        return [s.name for s in self._client().list_properties_of_secrets()]


class KeyVaultResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a KeyVaultResource for use by other components / ops."""

    resource_key: str = Field(default="key_vault", description="Dagster resource key")
    vault_url: str = Field(
        description="Vault URL (e.g. https://my-kv.vault.azure.net)"
    )
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = KeyVaultResource(
            vault_url=self.vault_url,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
