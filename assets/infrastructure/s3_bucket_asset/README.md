# S3 Bucket

Create, delete, or idempotently ensure an S3 bucket exists, as a materializable Dagster asset --
the Airflow `S3CreateBucketOperator` / `S3DeleteBucketOperator` pair collapsed into one
component.

```yaml
type: dagster_community_components.S3BucketAssetComponent
attributes:
  asset_name: raw_landing_bucket
  bucket_name: my-raw-landing-bucket
  action: ensure_exists
  aws_region: us-west-2
  group_name: infrastructure
```

## Actions

- **`ensure_exists`** (default) -- create the bucket if it's missing. A no-op if it already
  exists and is owned by the caller (idempotent -- safe to materialize repeatedly).
- **`create`** -- expects the bucket NOT to already exist. Fails loudly
  (`BucketAlreadyExists` / `BucketAlreadyOwnedByYou`) if it does, even if you own it.
- **`delete`** -- idempotent. Deleting an already-absent bucket succeeds silently. Deleting a
  non-empty bucket requires `force_delete: true` (off by default -- destructive and
  irreversible: every object, every object version, and every delete marker in the bucket is
  removed before the bucket itself is).

## Credentials

Uses boto3's default credential chain (environment variables, shared credentials file, instance
role, etc.) -- no credentials fields on the component itself, same as `s3_to_database_asset`.

## Regions

`aws_region` is passed through to `create_bucket`'s `CreateBucketConfiguration.LocationConstraint`
-- **except** for `us-east-1`, which S3 special-cases: passing a `LocationConstraint` of
`us-east-1` raises `IllegalLocationConstraintException`. This component handles that for you
(omits the constraint entirely when `aws_region` is `us-east-1` or unset).

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | -- | Dagster asset key. |
| `bucket_name` | string | yes | -- | S3 bucket name (globally unique across all of AWS). |
| `action` | `create`\|`delete`\|`ensure_exists` | no | `ensure_exists` | See Actions above. |
| `aws_region` | string | no | boto3 default | AWS region for bucket creation. Ignored for `action='delete'`. |
| `force_delete` | bool | no | `false` | `action='delete'` only -- empty the bucket first. |
| `group_name` | string | no | `infrastructure` | Dagster asset group name. |
| `description` | string | no | auto-generated | Asset description. |
| `deps` | list[str] | no | -- | Upstream asset keys. |
| `owners` | list[str] | no | -- | Asset owners. |
| `asset_tags` | dict | no | -- | Catalog tags. |
| `kinds` | list[str] | no | `[aws, s3]` | Catalog kinds. |
| `retry_policy_max_retries` | int | no | -- | Retry policy. |
| `retry_policy_delay_seconds` | int | no | `1` | Retry delay. |
| `retry_policy_backoff` | string | no | `exponential` | `linear` or `exponential`. |
| `pool` | string | no | -- | Named concurrency pool. |
