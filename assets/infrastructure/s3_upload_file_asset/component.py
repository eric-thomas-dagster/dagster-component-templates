"""S3UploadFileAssetComponent.

Upload a local file to S3 as a Dagster asset -- mirrors Airflow's `LocalFilesystemToS3Operator`
/ `S3Hook.load_file`. Verified against the real boto3 `upload_file` signature
(`Filename, Bucket, Key, ExtraArgs=None, Callback=None, Config=None`) before writing anything.
`replace=False` (default) checks existence first via `head_object` (a 404 `ClientError` means
"doesn't exist yet") and fails clearly if the destination key already exists -- matching
`S3Hook.load_file`'s own documented existence-check contract, not inventing new semantics.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


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


def _parse_s3_url(url: str) -> "tuple[str, str]":
    if not url.startswith("s3://"):
        raise ValueError(f"expected an 's3://bucket/key' URL, got {url!r}")
    rest = url[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"expected an 's3://bucket/key' URL, got {url!r}")
    return bucket, key


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


def _object_exists(s3, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise


class S3UploadFileAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upload a local file to S3 as a Dagster asset -- mirrors Airflow's
    `LocalFilesystemToS3Operator` / `S3Hook.load_file`."""

    asset_name: str = Field(description="Dagster asset key.")
    local_path: str = Field(description="Path (relative to the Dagster process working directory) to the local file to upload.")

    dest_s3_url: Optional[str] = Field(default=None, description="Destination object as 's3://bucket/key'. Set this or bucket+key.")
    bucket: Optional[str] = Field(default=None, description="Destination bucket name. Set with key, or use dest_s3_url instead.")
    key: Optional[str] = Field(default=None, description="Destination object key. Set with bucket, or use dest_s3_url instead.")

    replace: bool = Field(default=False, description="False (default, matches S3Hook.load_file): fail clearly if the destination key already exists, checked via head_object before uploading. True: always upload, overwriting any existing object.")
    extra_args: Optional[Dict[str, Any]] = Field(default=None, description="Passed to boto3's upload_file(ExtraArgs=...), e.g. {'ACL': 'private', 'ContentType': 'text/csv'}.")
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    resource_key: Optional[str] = Field(default=None, description="Optional key of a registered aws_s3_resource. When set, credentials/session come from that resource instead of a bare boto3.client('s3', region_name=...).")

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient S3/network errors.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name). The "
            "pool's own slot count is configured at the deployment level (dagster.yaml's "
            "concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Kill the wait and fail the run if the upload hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying boto3 call may continue running past the deadline (Python threads can't be forcibly killed); this bounds the step's wait, not the call itself.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_destination(self):
        if bool(self.dest_s3_url) == bool(self.bucket or self.key):
            raise ValueError("S3UploadFileAssetComponent: set exactly one of dest_s3_url or bucket+key.")
        if not self.dest_s3_url and (bool(self.bucket) != bool(self.key)):
            raise ValueError("S3UploadFileAssetComponent: bucket and key must be set together.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="S3 Upload File Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        required_resource_keys = {component.resource_key} if component.resource_key else set()
        dst_bucket, dst_key = (_parse_s3_url(component.dest_s3_url) if component.dest_s3_url else (component.bucket, component.key))

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(component.asset_name),
            description=component.description or f"Upload {component.local_path} -> s3://{dst_bucket}/{dst_key}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
            required_resource_keys=required_resource_keys,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _s3_upload_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if component.resource_key:
                s3 = getattr(context.resources, component.resource_key).get_client()
            else:
                import boto3
                s3 = boto3.client("s3", region_name=component.region_name)

            def _do_upload():
                if not component.replace and _object_exists(s3, dst_bucket, dst_key):
                    raise dg.Failure(
                        description=f"s3://{dst_bucket}/{dst_key} already exists and replace=False.",
                    )

                upload_kwargs: Dict[str, Any] = {}
                if component.extra_args:
                    upload_kwargs["ExtraArgs"] = component.extra_args
                s3.upload_file(component.local_path, dst_bucket, dst_key, **upload_kwargs)

            _run_with_timeout(_do_upload, component.timeout_seconds)
            context.log.info(f"Uploaded {component.local_path} -> s3://{dst_bucket}/{dst_key}")
            return dg.MaterializeResult(metadata={
                "local_path": dg.MetadataValue.path(component.local_path),
                "destination": dg.MetadataValue.text(f"s3://{dst_bucket}/{dst_key}"),
            })

        return dg.Definitions(assets=[_s3_upload_file])
