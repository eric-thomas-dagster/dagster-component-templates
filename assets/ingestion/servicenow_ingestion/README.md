# ServiceNowIngestionComponent

Read records from any ServiceNow table (incident, change_request, sc_request, sys_user, cmdb_ci, custom tables) into a DataFrame asset. Uses the standard ServiceNow Table API, paginates via `sysparm_limit` + `sysparm_offset`, applies an optional encoded-query filter, and supports both basic auth (dev instances) and bearer-token OAuth (production).

## Use cases

- Pull last 90 days of incidents → warehouse for service-quality / MTTR analytics
- Snapshot the CMDB into a data product downstream of asset discovery
- Sync change-management activity for compliance / audit reporting
- Materialize a custom ServiceNow table for downstream Dagster transforms

## Dependencies

- `requests>=2.28.0`
- `pandas>=1.5.0`

## Configuration

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Output asset name |
| `instance_env_var` | `str` | Yes | — | Env var with the instance subdomain (just `mycompany`, not `https://...`) |
| `username_env_var` | `str` | Conditional | — | Required unless `bearer_token_env_var` is set |
| `password_env_var` | `str` | Conditional | — | Required unless `bearer_token_env_var` is set |
| `bearer_token_env_var` | `str` | Conditional | — | OAuth bearer token. When set, basic-auth fields are ignored |
| `table` | `str` | Yes | — | ServiceNow table name (`incident`, `change_request`, `sc_request`, `sys_user`, `cmdb_ci`, custom) |
| `sysparm_query` | `str` | No | — | ServiceNow encoded query (e.g. `state=approved^priority=1`) |
| `sysparm_fields` | `str` | No | — | Comma-separated field list. Defaults to all fields |
| `sysparm_display_value` | `str` | No | `"false"` | `"false"` = sys_id only, `"true"` = display only, `"all"` = both |
| `page_size` | `int` | No | `1000` | Records per API call (`sysparm_limit`) |
| `max_records` | `int` | No | — | Cap on total records returned |
| `verify_ssl` | `bool` | No | `true` | Disable only for self-signed dev instances |

## Common ServiceNow tables

| Table | Returns |
|---|---|
| `incident` | All incidents |
| `change_request` | Change requests |
| `change_task` | Tasks within change requests |
| `problem` | Problem records |
| `sc_request` | Service Catalog requests (REQ) |
| `sc_req_item` | Service Catalog request items (RITM) |
| `sys_user` | Users |
| `sys_user_group` | Groups |
| `cmdb_ci` | Configuration items (CMDB) |
| `cmdb_ci_server` | CI Servers |
| `kb_knowledge` | Knowledge base articles |
| `task` | Top-level Task table (parent of incidents, changes, etc.) |
| `<x_company_table>` | Custom (scoped) tables |

## ServiceNow encoded queries

`sysparm_query` is ServiceNow's query syntax — operators joined by `^`:

- `state=1` — exact match
- `priority<=2` — comparisons
- `state=1^priority=1` — AND
- `state=1^ORstate=2` — OR  
- `sys_created_on>=javascript:gs.daysAgoStart(30)` — last 30 days
- `assigned_to.user_name=alice` — dot-walk to referenced fields
- `assigned_toISEMPTY` — null check
- `short_descriptionLIKEdatabase` — substring match

See <https://www.servicenow.com/docs/bundle/yokohama-platform-user-interface/page/use/common-ui-elements/reference/r_OpAvailableFiltersQueries.html> for the full operator list.

## Examples

Last 30 days of P1 / P2 incidents:

```yaml
type: dagster_component_templates.ServiceNowIngestionComponent
attributes:
  asset_name: high_priority_incidents
  instance_env_var: SNOW_INSTANCE
  username_env_var: SNOW_USERNAME
  password_env_var: SNOW_PASSWORD
  table: incident
  sysparm_query: "priority<=2^sys_created_on>=javascript:gs.daysAgoStart(30)"
  sysparm_fields: number,short_description,priority,state,assigned_to,sys_created_on,resolved_at
  page_size: 1000
  max_records: 50000
```

Open change requests with a specific assignment group, OAuth auth:

```yaml
type: dagster_component_templates.ServiceNowIngestionComponent
attributes:
  asset_name: open_data_eng_changes
  instance_env_var: SNOW_INSTANCE
  bearer_token_env_var: SNOW_ACCESS_TOKEN
  table: change_request
  sysparm_query: "state!=closed^assignment_group.name=data_engineering"
```

Full CMDB snapshot (server CIs only):

```yaml
type: dagster_component_templates.ServiceNowIngestionComponent
attributes:
  asset_name: cmdb_servers
  instance_env_var: SNOW_INSTANCE
  username_env_var: SNOW_USERNAME
  password_env_var: SNOW_PASSWORD
  table: cmdb_ci_server
  sysparm_display_value: "all"
  page_size: 5000
```

## Pagination

ServiceNow paginates via `sysparm_offset`. This component walks pages until either:

1. The current page returns fewer than `page_size` records (last page), OR
2. `max_records` has been reached.

For very large tables, set `page_size: 5000`–`10000` to reduce request count. ServiceNow recommends 10000 as the practical max.

## Auth

| Mode | When | How |
|---|---|---|
| Basic | Dev instances, PDIs | Set `username_env_var` + `password_env_var` |
| OAuth bearer | Production | Set `bearer_token_env_var`. Pair with `oauth_token_resource` to acquire + rotate the token via OAuth `client_credentials` grant |

The community `oauth_token_resource` component handles the OAuth dance (token acquisition, refresh-token rotation, writeback to AWS Secrets Manager / Azure Key Vault / Vault). Wire it once and point `servicenow_ingestion` (and `servicenow_sensor`) at the resulting env var.

## See also

- `servicenow_resource` — shared connection (instance + auth) for multiple ServiceNow assets in one project
- `servicenow_sensor` — trigger a Dagster job when records match a `sysparm_query`
- `oauth_token_resource` — acquire + rotate bearer tokens for production OAuth
- [Schema](schema.json) · [Example](example.yaml)
