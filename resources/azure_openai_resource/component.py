"""Azure OpenAI Resource.

Distinct from `openai_resource` (which targets api.openai.com directly):
Azure OpenAI uses Azure deployment IDs, regional endpoints, and Microsoft
Entra ID OAuth (or API key) for auth. Many enterprise Azure customers
use Azure OpenAI specifically for compliance / data residency reasons.

Provides an `openai.AzureOpenAI` client (and an async variant) that
existing components like `openai_llm` can consume by passing this
resource in instead of the generic OpenAIResource.

Auth options:
  1. API key (api_key_env_var) — simplest
  2. Microsoft Entra ID OAuth (DefaultAzureCredential) — preferred for
     production. Principal needs "Cognitive Services User" on the
     Azure OpenAI resource.

Reference: https://learn.microsoft.com/azure/ai-services/openai/
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class AzureOpenAIResource(dg.ConfigurableResource):
    """Azure OpenAI Service client wrapper."""

    azure_endpoint: str = Field(
        description="Azure OpenAI endpoint URL (e.g. https://my-aoai.openai.azure.com)"
    )
    api_version: str = Field(
        default="2024-10-21",
        description="Azure OpenAI API version (use the latest GA — see Azure docs)",
    )

    # API key auth (option 1)
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Azure OpenAI API key. Mutually exclusive with Entra OAuth.",
    )

    # Microsoft Entra ID auth (option 2 — preferred)
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    # Default deployment for convenience — components can override per-call
    default_deployment: Optional[str] = Field(
        default=None,
        description="Default deployment name (the model alias you configured in Azure OpenAI Studio)",
    )

    def get_client(self):
        """Return a configured openai.AzureOpenAI client."""
        try:
            import openai
        except ImportError as e:
            raise ImportError("openai required: pip install 'openai>=1.0'") from e

        api_key = os.environ.get(self.api_key_env_var) if self.api_key_env_var else None

        if api_key:
            return openai.AzureOpenAI(
                api_key=api_key,
                azure_endpoint=self.azure_endpoint,
                api_version=self.api_version,
            )

        # Entra OAuth via DefaultAzureCredential
        try:
            from azure.identity import (
                DefaultAzureCredential,
                ClientSecretCredential,
                get_bearer_token_provider,
            )
        except ImportError as e:
            raise ImportError("azure-identity required for Entra auth: pip install azure-identity") from e

        tenant = os.environ.get(self.tenant_id_env_var) if self.tenant_id_env_var else None
        client = os.environ.get(self.client_id_env_var) if self.client_id_env_var else None
        secret = os.environ.get(self.client_secret_env_var) if self.client_secret_env_var else None
        if tenant and client and secret:
            cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
        else:
            cred = DefaultAzureCredential()

        token_provider = get_bearer_token_provider(
            cred, "https://cognitiveservices.azure.com/.default"
        )
        return openai.AzureOpenAI(
            azure_ad_token_provider=token_provider,
            azure_endpoint=self.azure_endpoint,
            api_version=self.api_version,
        )

    def chat(self, messages: list, deployment: Optional[str] = None, **kwargs) -> str:
        """Convenience: run a chat completion and return the response text."""
        deployment = deployment or self.default_deployment
        if not deployment:
            raise ValueError(
                "deployment is required (either as method arg or default_deployment on resource)"
            )
        client = self.get_client()
        resp = client.chat.completions.create(
            model=deployment,
            messages=messages,
            **kwargs,
        )
        return resp.choices[0].message.content or ""

    def embed(self, texts: list, deployment: Optional[str] = None) -> list:
        """Convenience: run text embeddings, returns list[list[float]]."""
        deployment = deployment or self.default_deployment
        if not deployment:
            raise ValueError("deployment is required")
        client = self.get_client()
        resp = client.embeddings.create(model=deployment, input=texts)
        return [d.embedding for d in resp.data]


class AzureOpenAIResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AzureOpenAIResource for use by other components / ops."""

    resource_key: str = Field(default="azure_openai", description="Dagster resource key")
    azure_endpoint: str = Field(description="Azure OpenAI endpoint URL")
    api_version: str = Field(default="2024-10-21")
    default_deployment: Optional[str] = Field(default=None)
    api_key_env_var: Optional[str] = Field(default=None)
    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = AzureOpenAIResource(
            azure_endpoint=self.azure_endpoint,
            api_version=self.api_version,
            default_deployment=self.default_deployment,
            api_key_env_var=self.api_key_env_var,
            tenant_id_env_var=self.tenant_id_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
