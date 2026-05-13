# Running multiple instances of a component (multi-tenant ingestion)

A common need: one Dagster project ingests data from N customers / N accounts of the same upstream system. For example, pulling Airtable data for 10 different customer bases, or Salesforce data for 3 different orgs. This document is the single source of truth for that pattern.

Companion to [`assets/ingestion/DESTINATIONS.md`](assets/ingestion/DESTINATIONS.md), which covers the same shape for *destinations* (one project writing to multiple Snowflake accounts, etc.).

## The pattern

Every component is **multi-instance by default** — you just install the component once and create one `defs.yaml` per instance. Each defs.yaml is an independent asset with its own credentials and config.

```
my-project/
  src/my_project/
    components/
      airtable_ingestion/        # installed once
        component.py
        ...
    defs/
      airtable_customer_a/       # instance 1
        defs.yaml                # references AIRTABLE_API_KEY_CUSTOMER_A
      airtable_customer_b/       # instance 2
        defs.yaml                # references AIRTABLE_API_KEY_CUSTOMER_B
      airtable_customer_c/       # instance 3
        defs.yaml                # references AIRTABLE_API_KEY_CUSTOMER_C
```

Three pieces make this work:

1. **One install, many configurations.** `dagster-component add <id>` drops the component into `components/<id>/` once. Every `defs/<name>/defs.yaml` that references `components.<id>.component.<Class>` creates a new asset instance.
2. **Jinja env-var interpolation in defs.yaml.** Any field can reference `{{ env('SOME_VAR') }}`. Use a different env var per instance.
3. **Per-instance asset keys.** Each `defs.yaml` sets a unique `asset_name:` (or `asset_key:`) so the assets don't collide in the catalog.

## Worked example — Airtable for three customers

```yaml
# src/my_project/defs/airtable_customer_a/defs.yaml
type: my_project.components.airtable_ingestion.component.AirtableIngestionComponent
attributes:
  asset_name: customer_a_airtable
  api_key: "{{ env('AIRTABLE_API_KEY_CUSTOMER_A') }}"
  base_id: "appAAAAAAAAAAAAAA"
  table_names: [Customers, Orders, Products]
  group_name: customer_a
```

```yaml
# src/my_project/defs/airtable_customer_b/defs.yaml
type: my_project.components.airtable_ingestion.component.AirtableIngestionComponent
attributes:
  asset_name: customer_b_airtable
  api_key: "{{ env('AIRTABLE_API_KEY_CUSTOMER_B') }}"
  base_id: "appBBBBBBBBBBBBBB"
  table_names: [Customers, Orders, Products]
  group_name: customer_b
```

```yaml
# src/my_project/defs/airtable_customer_c/defs.yaml
type: my_project.components.airtable_ingestion.component.AirtableIngestionComponent
attributes:
  asset_name: customer_c_airtable
  api_key: "{{ env('AIRTABLE_API_KEY_CUSTOMER_C') }}"
  base_id: "appCCCCCCCCCCCCCC"
  table_names: [Customers, Orders, Products]
  group_name: customer_c
```

```bash
export AIRTABLE_API_KEY_CUSTOMER_A=keyAAAA...
export AIRTABLE_API_KEY_CUSTOMER_B=keyBBBB...
export AIRTABLE_API_KEY_CUSTOMER_C=keyCCCC...
uv run dg dev
```

Three independent assets in the Dagster catalog, each with its own credentials, each materializing independently, each with its own freshness / schedule / sensor as needed.

## Generating the boilerplate — Python script

If you have many customers, generate the YAML programmatically. Drop a script next to your `defs/` directory:

```python
# scripts/scaffold_airtable_customers.py
from pathlib import Path
import yaml

CUSTOMERS = [
    {"id": "customer_a", "base_id": "appAAAAAAAAAAAAAA", "env_suffix": "CUSTOMER_A"},
    {"id": "customer_b", "base_id": "appBBBBBBBBBBBBBB", "env_suffix": "CUSTOMER_B"},
    {"id": "customer_c", "base_id": "appCCCCCCCCCCCCCC", "env_suffix": "CUSTOMER_C"},
]
TABLES = ["Customers", "Orders", "Products"]
DEFS_ROOT = Path("src/my_project/defs")

for c in CUSTOMERS:
    folder = DEFS_ROOT / f"airtable_{c['id']}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "defs.yaml").write_text(yaml.safe_dump({
        "type": "my_project.components.airtable_ingestion.component.AirtableIngestionComponent",
        "attributes": {
            "asset_name": f"{c['id']}_airtable",
            "api_key": "{{ env('AIRTABLE_API_KEY_" + c["env_suffix"] + "') }}",
            "base_id": c["base_id"],
            "table_names": TABLES,
            "group_name": c["id"],
        },
    }, sort_keys=False))
    print(f"wrote {folder / 'defs.yaml'}")
```

Run it whenever your customer list changes; commit the generated YAML.

## Combining with multi-destination ingestion

Multi-instance sources compose naturally with multi-account destinations (see [`DESTINATIONS.md`](assets/ingestion/DESTINATIONS.md)):

```yaml
# Each customer's Airtable lands in their own Snowflake account
type: ...AirtableIngestionComponent
attributes:
  asset_name: customer_a_airtable
  api_key: "{{ env('AIRTABLE_API_KEY_CUSTOMER_A') }}"
  base_id: appAAAAAAAAAAAAAA
  table_names: [Customers, Orders, Products]

  destination: snowflake
  destination_credentials_url: "{{ env('SNOWFLAKE_URL_CUSTOMER_A') }}"
  dataset_name: customer_a_airtable
  persist_only: true
```

## When this pattern doesn't fit

- **Hundreds of customers with the same schema.** Generating hundreds of asset definitions is doable (see the scaffold script) but the asset graph in the UI gets crowded. Consider partitioning a single asset by customer instead — Dagster's static or dynamic partitions can model "one customer per partition" cleanly. See the partition fields on most ingestion components (`partition_type`, `partition_values`, `dynamic_partition_name`).
- **Customer credentials rotate frequently.** Env vars are stable; if customers rotate keys often, fetch credentials from a secret manager at run-time and inject via a resource rather than via Jinja `env()`.
- **Per-customer schedules / freshness.** Each instance can have its own `freshness_max_lag_minutes` / sensor / schedule. That's the whole point of instance-level config — exercise it.

## Auth patterns that already work this way

The same pattern works for any component with credentials. The pieces that vary are the field names and (for non-dlt sources) where the credential is consumed:

| Component family | Per-instance credential field | Typical env-var convention |
|---|---|---|
| Airtable / Salesforce / HubSpot / Notion / dlt-based sources | `api_key` / `access_token` / `client_secret` | `<VENDOR>_API_KEY_<TENANT>` |
| GitHub / GitLab / Bitbucket | `access_token` | `GITHUB_TOKEN_<ORG>` |
| LLM providers (OpenAI / Anthropic / Gemini / LiteLLM) | `api_key` | `OPENAI_API_KEY_<APP>` etc. |
| Database resources (`postgres_resource`, `mysql_resource`, etc.) | `password` / connection URL | `<DB>_URL_<TENANT>` or per-field env vars |
| Cloud storage components (`s3_*`, `gcs_*`, `adls_*`) | role-based / ambient credentials | typically Workload Identity / IAM role per Dagster deployment; per-tenant via `aws_profile` / explicit creds |

Cloud-storage components (S3 / GCS / ADLS) usually default to ambient credentials (instance profile / Workload Identity / `DefaultAzureCredential`). For multi-tenant cloud-storage access, the pattern shifts: instead of multiple env vars per Dagster deployment, you typically have one Dagster deployment per tenant, OR a service principal with cross-tenant access roles.
