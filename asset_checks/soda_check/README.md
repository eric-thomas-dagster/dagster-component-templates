# Soda Check

> **DEPRECATED — prefer [`dagster-soda`](https://main.archive.dagster-docs.io/integrations/libraries/soda).**
>
> Dagster's official `dagster-soda` package is the maintained, packaged path. Use it unless you have a reason not to.
>
> **Reasons you might still want this component:** you want to fork the SodaCL parser, swap the runtime call, change the asset-key routing logic, or otherwise own the code without taking a versioned dependency. Since this component is a single file you copy into your project, every part of it is editable.

---

Runs a Soda scan and emits **one Dagster asset check per SodaCL check** (parity with `dagster-soda`'s per-check granularity). Pre-parses the SodaCL YAML at `build_defs` time to enumerate checks, then runs the scan once at runtime and yields a result per check.

If the SodaCL YAML can't be pre-parsed, the component falls back to a single aggregate check.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | **required** | Default asset key for checks not routed via `asset_key_map` |
| `data_source_name` | `str` | **required** | Soda data source name (defined in configuration.yaml) |
| `checks_yaml_paths` | `str \| List[str]` | **required** | Path(s) to SodaCL YAML files |
| `soda_configuration_path` | `str` | `./soda/configuration.yaml` | Path to the Soda configuration YAML file |
| `asset_key_map` | `Dict[str, str]` | `None` | Optional map from SodaCL dataset name → Dagster asset key. Lets one scan attach checks to multiple assets. |
| `severity_override` | `str` | `None` | Optional — force every check to `WARN` or `ERROR`. Defaults to inheriting from SodaCL `attributes.severity`, then `ERROR`. |

## Example

```yaml
type: dagster_component_templates.SodaCheckComponent
attributes:
  asset_key: warehouse/orders
  data_source_name: my_postgres
  checks_yaml_paths:
    - ./soda/orders_checks.yaml
    - ./soda/customers_checks.yaml
  soda_configuration_path: ./soda/configuration.yaml
  asset_key_map:
    orders: warehouse/orders
    customers: warehouse/customers
  # severity_override: WARN
```

## Soda Cloud integration

If your `configuration.yaml` includes a `soda_cloud:` block, soda-core posts each scan to Soda Cloud automatically — no component changes needed:

```yaml
# configuration.yaml
soda_cloud:
  host: cloud.soda.io
  api_key_id: ${SODA_CLOUD_KEY_ID}
  api_key_secret: ${SODA_CLOUD_KEY_SECRET}
```

When Cloud is configured, this component **also surfaces the per-check Cloud URL as `MetadataValue.url(...)` in each `AssetCheckResult`**. Engineers can click straight from the Dagster catalog to the Cloud incident page (or the scan-level URL if a per-check ID isn't available).

The URL detection is best-effort across soda-core versions — if the API field names drift, the metadata key just won't appear (it doesn't break the run).

## SodaCL severity

Per-check severity is read from `attributes.severity` (case-insensitive `WARN`/`ERROR`):

```yaml
checks for orders:
  - missing_count(customer_id) = 0:
      attributes:
        severity: warn
  - duplicate_count(order_id) = 0   # defaults to ERROR
```

Priority order: `severity_override` > `attributes.severity` > runtime outcome (`warn`) > `ERROR`.

## Multi-dataset routing

When a single SodaCL file checks multiple datasets, use `asset_key_map` to route each `checks for <dataset>:` block to a different Dagster asset. Datasets not in the map fall back to the component-level `asset_key`.

## Customizing

Since this is a file-copy component, everything is yours to edit after `dagster-component add soda_check`:

- Change the check-name format → edit `_name_for_soda_check`
- Add custom dataset → asset routing → edit `_parse_sodacl_files`
- Inject extra metadata → extend the `AssetCheckResult` yields in `soda_checks`
- Swap to Soda Cloud SDK → replace the `Scan()` block

## Requirements

```
soda-core>=3.0.0
PyYAML>=5.1
```
