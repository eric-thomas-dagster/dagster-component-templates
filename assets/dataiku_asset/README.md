# Dataiku DSS Scenario Assets

Connects to a Dataiku DSS instance at **prepare time**, discovers all scenarios across one or more projects, and creates **one Dagster asset per scenario**. At execution time each asset triggers its Dataiku scenario via the DSS REST API and optionally waits for it to finish.

The Dataiku Python SDK is **not required**. All API communication uses the Dataiku DSS REST API directly via `requests`, authenticated with HTTP Basic Auth `(api_key, "")`.

---

## Why StateBackedComponent?

Discovering scenarios requires network calls to your Dataiku DSS instance. With `StateBackedComponent` those calls happen exactly once — at prepare/deploy time — and the result is written to a small JSON file on disk. Every subsequent code-server reload reads from that cache with zero network overhead, so startup is instant regardless of how many projects or scenarios exist in your Dataiku instance.

To refresh the cache (e.g. after adding new scenarios):

```bash
# During local development — refreshes automatically on dagster dev startup
dagster dev

# In CI/CD or when building a Docker image
dg utils refresh-defs-state
```

---

## Required Dataiku API Permissions

The API key used must have the following permissions on each project:

| Permission | Why |
|---|---|
| **Project Reader** | Enumerate projects (`GET /api/projects/`) and list scenarios (`GET /api/projects/{key}/scenarios/`) |
| **Scenario Runner** | Trigger scenario execution (`POST /api/projects/{key}/scenarios/{id}/run/`) |

A single personal API key with Reader + Runner on the relevant projects is sufficient.

### How to obtain a Dataiku API key

1. Log into your Dataiku DSS instance.
2. Navigate to your user profile (top-right avatar > **Profile**).
3. Click **API Keys** in the left sidebar.
4. Click **Create a new personal API key** and copy the generated key.
5. Store it in the environment variable referenced by `api_key_env_var` (e.g. `DATAIKU_API_KEY`).

For service accounts, ask your Dataiku administrator to create a group API key under **Administration > Security > API Keys**.

---

## Required packages

```
requests>=2.28.0
```

No Dataiku Python SDK installation is needed.

---

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `host_env_var` | Yes | — | Env var containing the Dataiku DSS host URL, e.g. `https://dataiku.company.com` |
| `api_key_env_var` | Yes | — | Env var containing the Dataiku DSS API key |
| `project_keys` | No | `null` (all projects) | List of specific project keys to include |
| `exclude_project_keys` | No | `null` | Project keys to exclude (applied after `project_keys`) |
| `scenario_name_prefix` | No | `null` | Only include scenarios whose name starts with this string |
| `exclude_scenario_ids` | No | `null` | Scenario IDs to exclude |
| `group_name` | No | `"dataiku"` | Dagster asset group name for all generated assets |
| `key_prefix` | No | `null` | Optional prefix segment prepended to all asset keys |
| `wait_for_completion` | No | `true` | Poll until the scenario finishes before returning |
| `poll_interval_seconds` | No | `10` | Seconds between status polls (when `wait_for_completion=true`) |
| `scenario_timeout_seconds` | No | `7200` | Maximum seconds to wait for a scenario before raising an error |

---

## Asset keys

Asset keys are constructed as:

```
[key_prefix, project_key, scenario_id]   # when key_prefix is set
[project_key, scenario_id]               # when key_prefix is None
```

All segments are lowercased and non-alphanumeric characters are replaced with `_`.

---

## Example YAML

### Minimal — all projects, all scenarios

```yaml
type: dagster_component_templates.DataikuAssetComponent
attributes:
  host_env_var: DATAIKU_HOST
  api_key_env_var: DATAIKU_API_KEY
```

### Specific projects with filtering

```yaml
type: dagster_component_templates.DataikuAssetComponent
attributes:
  host_env_var: DATAIKU_HOST
  api_key_env_var: DATAIKU_API_KEY
  project_keys:
    - CUSTOMER_ANALYTICS
    - ML_MODELS
  group_name: dataiku
  wait_for_completion: true
```

### Advanced — prefix filter, key prefix, longer timeout

```yaml
type: dagster_component_templates.DataikuAssetComponent
attributes:
  host_env_var: DATAIKU_HOST
  api_key_env_var: DATAIKU_API_KEY
  project_keys:
    - CUSTOMER_ANALYTICS
    - ML_MODELS
  exclude_project_keys:
    - SANDBOX
  scenario_name_prefix: "prod_"
  exclude_scenario_ids:
    - s_legacy_cleanup
  key_prefix: dku
  group_name: dataiku_prod
  wait_for_completion: true
  poll_interval_seconds: 15
  scenario_timeout_seconds: 14400
```

---

## How to refresh the scenario cache

The cached state is stored as a JSON file managed by Dagster's `DefsStateConfig`. To force a refresh:

```bash
# Refresh locally
dg utils refresh-defs-state

# Or simply restart dagster dev — it refreshes on startup
dagster dev
```

In Docker-based deployments, run `dg utils refresh-defs-state` as part of your image build step so the cache is baked in and no network call is needed at runtime.

---

## Fallback behaviour

If `StateBackedComponent` is not available (Dagster < 1.8), the component falls back to fetching the scenario list on every `build_defs` call. Upgrade to `dagster>=1.8` to enable caching.
