"""S3BucketAssetComponent -- Airflow-equivalent bucket lifecycle management (the
`S3CreateBucketOperator`/`S3DeleteBucketOperator` pair) as a single idempotent Dagster asset.

action='ensure_exists' (default) creates the bucket if missing and is a no-op if it already
exists and is owned by the caller -- the common "make sure this bucket is there" infra-as-code
shape. action='create' expects the bucket NOT to already exist and fails loudly if it does.
action='delete' is idempotent: deleting an already-absent bucket succeeds; deleting a non-empty
bucket requires force_delete=True (destructive, off by default).
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


def _create_bucket(s3_client, bucket_name: str, region: Optional[str]) -> None:
    """boto3's create_bucket is the one API where us-east-1 is special-cased: passing a
    LocationConstraint for it is a real, commonly-hit error (IllegalLocationConstraintException)."""
    if region and region != "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
    else:
        s3_client.create_bucket(Bucket=bucket_name)


def _bucket_exists(s3_client, bucket_name: str) -> bool:
    from botocore.exceptions import ClientError
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            return False
        raise


def _make_s3_client(region_name: Optional[str]):
    import boto3
    return boto3.client("s3", region_name=region_name) if region_name else boto3.client("s3")


def _empty_bucket(s3_client, bucket_name: str) -> int:
    """Delete every object (and, if versioning is/was ever enabled, every version and delete
    marker) so delete_bucket won't fail with BucketNotEmpty. Used only when force_delete=True."""
    deleted = 0
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket_name):
        to_delete = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", [])]
        to_delete += [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("DeleteMarkers", [])]
        if to_delete:
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})
            deleted += len(to_delete)
    return deleted


class S3BucketAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create, delete, or idempotently ensure an S3 bucket exists, as a materializable asset.

    ```yaml
    type: dagster_community_components.S3BucketAssetComponent
    attributes:
      asset_name: raw_landing_bucket
      bucket_name: my-raw-landing-bucket
      action: ensure_exists
      aws_region: us-west-2
      group_name: infrastructure
    ```
    """

    asset_name: str = Field(description="Dagster asset key.")
    bucket_name: str = Field(description="S3 bucket name (globally unique across all of AWS).")
    action: Literal["create", "delete", "ensure_exists"] = Field(
        default="ensure_exists",
        description=(
            "'ensure_exists' (default): create the bucket if missing; a no-op if it already exists "
            "and is owned by the caller (idempotent). 'create': expects the bucket NOT to already "
            "exist; fails loudly (BucketAlreadyExists / BucketAlreadyOwnedByYou) if it does. "
            "'delete': idempotent -- deleting an already-absent bucket succeeds."
        ),
    )
    aws_region: Optional[str] = Field(
        default=None,
        description="AWS region for bucket creation, e.g. 'us-west-2'. Unset uses boto3's default region chain. Ignored for action='delete'.",
    )
    force_delete: bool = Field(
        default=False,
        description=(
            "action='delete' only. S3 refuses to delete a non-empty bucket by default (safety). "
            "Set True to first delete every object/version/delete-marker in the bucket, then the "
            "bucket itself -- destructive and irreversible."
        ),
    )

    group_name: Optional[str] = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'aws' and 's3' if empty.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(default=None, description="Dagster's native named concurrency pool.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="S3 Bucket", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"aws", "s3"}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey(component.asset_name.split("/")),
            description=component.description or f"S3 bucket lifecycle ({component.action}): {component.bucket_name}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or None,
            tags=component.asset_tags or None,
            kinds=kinds,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _s3_bucket_asset(context: dg.AssetExecutionContext):
            from botocore.exceptions import ClientError

            s3_client = _make_s3_client(component.aws_region)

            if component.action == "delete":
                existed = _bucket_exists(s3_client, component.bucket_name)
                deleted_objects = 0
                if existed:
                    if component.force_delete:
                        deleted_objects = _empty_bucket(s3_client, component.bucket_name)
                    s3_client.delete_bucket(Bucket=component.bucket_name)
                    context.log.info(f"Deleted S3 bucket {component.bucket_name!r} ({deleted_objects} object(s) removed first)")
                else:
                    context.log.info(f"S3 bucket {component.bucket_name!r} already absent -- delete is a no-op")
                return dg.MaterializeResult(metadata={
                    "bucket_name": dg.MetadataValue.text(component.bucket_name),
                    "action": dg.MetadataValue.text("delete"),
                    "existed_before": dg.MetadataValue.bool(existed),
                    "objects_deleted": dg.MetadataValue.int(deleted_objects),
                })

            # action in (create, ensure_exists)
            already_existed = False
            try:
                _create_bucket(s3_client, component.bucket_name, component.aws_region)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code == "BucketAlreadyOwnedByYou" and component.action == "ensure_exists":
                    already_existed = True
                elif code == "BucketAlreadyExists" and component.action == "ensure_exists" and _bucket_exists(s3_client, component.bucket_name):
                    # us-east-1 legacy behavior: re-creating a bucket you own there can surface this
                    # code instead of BucketAlreadyOwnedByYou. head_bucket confirms it's actually ours.
                    already_existed = True
                else:
                    raise

            if already_existed:
                context.log.info(f"S3 bucket {component.bucket_name!r} already exists -- ensure_exists is a no-op")
            else:
                context.log.info(f"Created S3 bucket {component.bucket_name!r}")

            return dg.MaterializeResult(metadata={
                "bucket_name": dg.MetadataValue.text(component.bucket_name),
                "action": dg.MetadataValue.text(component.action),
                "already_existed": dg.MetadataValue.bool(already_existed),
                "aws_region": dg.MetadataValue.text(component.aws_region or "default"),
            })

        return dg.Definitions(assets=[_s3_bucket_asset])
