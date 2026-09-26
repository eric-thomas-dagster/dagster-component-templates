# `HeapResourceComponent`

Registers a `HeapResource` for other components to use via `resource_key`.

> **Why no `heap_ingestion` exists:** Heap's public API is write-only (`track`/`identify`/`add_user_properties`/`delete_user`) -- there is no bulk read/list endpoint at all, so a pull-based ingestion connector genuinely isn't possible against this API. That write API is exactly what reverse-ETL needs, though, so this resource + the `heap_user_property_sync` sink exist instead.

Beyond the raw `.get_client()` escape hatch, this resource provides `.add_user_properties(identity, properties)` -- `POST https://heapanalytics.com/api/add_user_properties`.

## Pairs with

- **`heap_user_property_sync`** — reverse-ETL sink built on top of this resource.

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `heap`) | Key used to register this resource. |
| `app_id_env_var` | optional (default `HEAP_APP_ID`) | Env var holding your Heap environment ID (App ID). |

## Example
```yaml
type: dagster_component_templates.HeapResourceComponent
attributes:
  resource_key: heap
  app_id_env_var: HEAP_APP_ID
```
