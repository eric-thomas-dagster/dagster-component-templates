# GCS Bucket

Create, delete, or idempotently ensure a GCS bucket exists, as a materializable Dagster asset --
the Airflow `GCSCreateBucketOperator` / `GCSDeleteBucketOperator` pair collapsed into one
component.

```yaml
type: dagster_community_components.GCSBucketAssetComponent
attributes:
  asset_name: raw_landing_bucket
  bucket_name: my-raw-landing-bucket
  action: ensure_exists
  location: US
  storage_class: STANDARD
  group_name: infrastructure
```

## Actions

- **`ensure_exists`** (default) -- create the bucket if it's missing. A no-op if it already
  exists and is owned by the caller (idempotent -- safe to materialize repeatedly). GCS's 409
  Conflict response doesn't by itself distinguish "exists and it's yours" from "someone else owns
  this globally-unique name" -- this component resolves that by calling `get_bucket()` after a
  409, which only succeeds if the bucket is actually yours.
- **`create`** -- expects the bucket NOT to already exist. Fails loudly (409 Conflict) if it does,
  even if you own it.
- **`delete`** -- idempotent. Deleting an already-absent bucket succeeds silently. Deleting a
  non-empty bucket requires `force_delete: true` (off by default -- destructive and
  irreversible).

## Credentials

Uses Google's Application Default Credentials by default. Set `credentials_env_var` to the name
of an environment variable holding a path to a service account JSON key file -- same convention
as `gcs_to_database_asset`'s `credentials_env_var`.

## Force-delete caveat

`force_delete` lists and deletes every object in the bucket one by one before deleting the bucket
itself. This matches the underlying `google-cloud-storage` client's own documented behavior and
is **not recommended by Google for buckets with more than roughly 256 objects** -- for larger
buckets, empty the bucket out-of-band (e.g. a lifecycle rule, or `gsutil -m rm`) and leave
`force_delete` at its default `false`.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | -- | Dagster asset key. |
| `bucket_name` | string | yes | -- | GCS bucket name (globally unique across all of GCS). |
| `action` | `create`\|`delete`\|`ensure_exists` | no | `ensure_exists` | See Actions above. |
| `location` | string | no | `US` | GCS bucket location (region or multi-region). Ignored for `action='delete'`. |
| `storage_class` | `STANDARD`\|`NEARLINE`\|`COLDLINE`\|`ARCHIVE` | no | `STANDARD` | Default object storage class. Ignored for `action='delete'`. |
| `credentials_env_var` | string | no | ADC | Env var with a path to a service account JSON key file. |
| `force_delete` | bool | no | `false` | `action='delete'` only -- empty the bucket first (see caveat above). |
| `group_name` | string | no | `infrastructure` | Dagster asset group name. |
| `description` | string | no | auto-generated | Asset description. |
| `deps` | list[str] | no | -- | Upstream asset keys. |
| `owners` | list[str] | no | -- | Asset owners. |
| `asset_tags` | dict | no | -- | Catalog tags. |
| `kinds` | list[str] | no | `[gcp, gcs]` | Catalog kinds. |
| `retry_policy_max_retries` | int | no | -- | Retry policy. |
| `retry_policy_delay_seconds` | int | no | `1` | Retry delay. |
| `retry_policy_backoff` | string | no | `exponential` | `linear` or `exponential`. |
| `pool` | string | no | -- | Named concurrency pool. |
