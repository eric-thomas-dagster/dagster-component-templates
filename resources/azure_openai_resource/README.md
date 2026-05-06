# Azure OpenAI Resource

Azure OpenAI Service client (distinct from generic OpenAI). Supports Azure deployment IDs, regional endpoints, api_version, and Microsoft Entra OAuth (or API key) auth.

## Why this is distinct from `openai_resource`

The official `dagster-openai` `OpenAIResource` targets `api.openai.com`
directly with API-key auth. Azure OpenAI Service has different shape:

| Aspect | OpenAI direct | Azure OpenAI |
|---|---|---|
| Endpoint | `api.openai.com` | `<resource>.openai.azure.com` |
| Model selection | `model="gpt-4"` | `model="<your-deployment-id>"` |
| api_version | not required | required (e.g. `2024-10-21`) |
| Auth | API key | API key OR Microsoft Entra OAuth |
| Compliance | per-OpenAI policies | per-tenant Azure governance, data residency |

## Auth options

**API key (simple, dev):**
```yaml
attributes:
  azure_endpoint: https://my-aoai.openai.azure.com
  api_key_env_var: AZURE_OPENAI_API_KEY
  default_deployment: gpt-4o-mini
```

**Microsoft Entra OAuth (production):**
```yaml
attributes:
  azure_endpoint: https://my-aoai.openai.azure.com
  tenant_id_env_var: AZURE_TENANT_ID
  client_id_env_var: AZURE_CLIENT_ID
  client_secret_env_var: AZURE_CLIENT_SECRET
  default_deployment: gpt-4o-mini
```

Service principal needs **Cognitive Services User** on the Azure OpenAI
resource. In Azure compute (ACA, AKS, Azure Functions) omit the env vars
and the resource picks up the managed identity automatically.

## Convenience methods

- `chat(messages, deployment=None, **kwargs)` → response text
- `embed(texts, deployment=None)` → list of embeddings
- `get_client()` → raw `openai.AzureOpenAI` client
