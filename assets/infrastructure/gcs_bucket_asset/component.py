"""GCSBucketAssetComponent -- Airflow-equivalent bucket lifecycle management (the
`GCSCreateBucketOperator`/`GCSDeleteBucketOperator` pair) as a single idempotent Dagster asset.

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


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _make_gcs_client(credentials_env_var: Optional[str]):
    import os
    from google.cloud import storage
    if credentials_env_var:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _require_env(credentials_env_var))
    return storage.Client()


class GCSBucketAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create, delete, or idempotently ensure a GCS bucket exists, as a materializable asset.

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
    """

    asset_name: str = Field(description="Dagster asset key.")
    bucket_name: str = Field(description="GCS bucket name (globally unique across all of GCS).")
    action: Literal["create", "delete", "ensure_exists"] = Field(
        default="ensure_exists",
        description=(
            "'ensure_exists' (default): create the bucket if missing; a no-op if it already exists "
            "and is owned by the caller (idempotent). 'create': expects the bucket NOT to already "
            "exist; fails loudly (409 Conflict) if it does. 'delete': idempotent -- deleting an "
            "already-absent bucket succeeds."
        ),
    )
    location: str = Field(
        default="US",
        description="GCS bucket location -- a region (e.g. 'us-central1') or multi-region (e.g. 'US', 'EU'). Ignored for action='delete'.",
    )
    storage_class: Literal["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"] = Field(
        default="STANDARD",
        description="Default storage class for objects written to the bucket. Ignored for action='delete'.",
    )
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a path to a GCP service account JSON key file. Unset uses application-default credentials (same convention as gcs_to_database_asset's credentials_env_var).",
    )
    force_delete: bool = Field(
        default=False,
        description=(
            "action='delete' only. GCS refuses to delete a non-empty bucket by default (safety). "
            "Set True to first delete every object in the bucket, then the bucket itself -- "
            "destructive and irreversible. Note: the underlying google-cloud-storage client's "
            "force-delete path lists and deletes objects in batches and is not recommended by "
            "Google for buckets with more than ~256 objects; for larger buckets, empty the bucket "
            "out-of-band first and use force_delete=False here."
        ),
    )

    group_name: Optional[str] = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'gcp' and 'gcs' if empty.")

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
        return ComponentFormConfig(label="GCS Bucket", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"gcp", "gcs"}
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
            description=component.description or f"GCS bucket lifecycle ({component.action}): {component.bucket_name}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or None,
            tags=component.asset_tags or None,
            kinds=kinds,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _gcs_bucket_asset(context: dg.AssetExecutionContext):
            from google.api_core.exceptions import Conflict, NotFound

            client = _make_gcs_client(component.credentials_env_var)

            if component.action == "delete":
                existed = True
                deleted_objects = 0
                try:
                    bucket = client.get_bucket(component.bucket_name)
                    if component.force_delete:
                        blobs = list(client.list_blobs(component.bucket_name))
                        deleted_objects = len(blobs)
                        for blob in blobs:
                            blob.delete()
                    bucket.delete(force=False)
                    context.log.info(f"Deleted GCS bucket {component.bucket_name!r} ({deleted_objects} object(s) removed first)")
                except NotFound:
                    existed = False
                    context.log.info(f"GCS bucket {component.bucket_name!r} already absent -- delete is a no-op")
                return dg.MaterializeResult(metadata={
                    "bucket_name": dg.MetadataValue.text(component.bucket_name),
                    "action": dg.MetadataValue.text("delete"),
                    "existed_before": dg.MetadataValue.bool(existed),
                    "objects_deleted": dg.MetadataValue.int(deleted_objects),
                })

            # action in (create, ensure_exists)
            already_existed = False
            bucket = client.bucket(component.bucket_name)
            bucket.storage_class = component.storage_class
            try:
                client.create_bucket(bucket, location=component.location)
            except Conflict:
                if component.action != "ensure_exists":
                    raise
                # GCS's 409 Conflict doesn't distinguish "exists and it's yours" from "someone
                # else owns this globally-unique name" -- get_bucket() does (raises Forbidden/
                # NotFound if it isn't actually ours), so ensure_exists only swallows the former.
                client.get_bucket(component.bucket_name)
                already_existed = True

            if already_existed:
                context.log.info(f"GCS bucket {component.bucket_name!r} already exists -- ensure_exists is a no-op")
            else:
                context.log.info(f"Created GCS bucket {component.bucket_name!r} in {component.location}")

            return dg.MaterializeResult(metadata={
                "bucket_name": dg.MetadataValue.text(component.bucket_name),
                "action": dg.MetadataValue.text(component.action),
                "already_existed": dg.MetadataValue.bool(already_existed),
                "location": dg.MetadataValue.text(component.location),
                "storage_class": dg.MetadataValue.text(component.storage_class),
            })

        return dg.Definitions(assets=[_gcs_bucket_asset])
