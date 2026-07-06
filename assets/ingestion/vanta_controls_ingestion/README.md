# Vanta Controls Ingestion

Fetches Vanta compliance controls (SOC 2, ISO 27001, HIPAA, PCI, etc.) plus their status, framework, description, and evidence counts via `GET /v1/controls`. Returns a flattened pandas DataFrame — one row per control.

Pairs with `vanta_resource` for OAuth2 client-credentials auth (cached tokens).

## Installation

`pip install requests>=2.28 pandas>=1.5.0`

## Fields

### Core

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | (required) | Name of the Dagster asset to create |
| `resource_name` | `str` | `"vanta_resource"` | Resource key of a registered `vanta_resource` |
| `framework` | `str \| None` | `None` | Filter by compliance framework (soc2, iso27001, hipaa, pci). Omit for all. |
| `status` | `str \| None` | `None` | Filter by control status (OK, NEEDS_ATTENTION, DEACTIVATED). Omit for all. |
| `limit` | `int` | `1000` | Maximum number of controls to fetch |
| `page_size` | `int` | `100` | Per-page size for pagination |
| `api_path` | `str` | `"/v1/controls"` | Vanta API endpoint path |

### Standard catalog

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str \| None` | `None` | Asset description |
| `group_name` | `str` | `"vanta"` | Asset group |
| `owners` | `list[str] \| None` | `None` | Asset owners |
| `asset_tags` | `dict[str,str] \| None` | `None` | Extra catalog tags |
| `kinds` | `list[str]` | `["vanta","rest"]` | Asset kinds |
| `freshness_max_lag_minutes` | `int \| None` | `None` | Freshness threshold |
| `freshness_cron` | `str \| None` | `None` | Freshness cron |
| `include_preview_metadata` | `bool` | `True` | Emit preview in metadata |
| `preview_rows` | `int` | `25` | Preview row count |
| `deps` | `list[str] \| None` | `None` | Upstream deps |

## Configuration

```yaml
type: dagster_community_components.VantaControlsIngestionComponent
attributes:
  asset_name: vanta_soc2_controls
  resource_name: vanta_resource
  framework: soc2
  limit: 1000
```

## Output columns

Common columns from `GET /v1/controls` (flattened via `pd.json_normalize`):

- `control_id`
- `framework`
- `name`
- `description`
- `status`
- `evidence_count`
- `last_evaluated_at`

Exact schema depends on your Vanta plan + enabled frameworks. See https://developer.vanta.com/reference/getcontrols.
