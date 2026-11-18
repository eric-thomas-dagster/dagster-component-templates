"""S3DeleteObjectsAssetComponent.

Delete a list of S3 objects as a Dagster asset -- mirrors Airflow's `S3DeleteObjectsOperator`.
Verified against the real boto3 DeleteObjects operation (via
`client.meta.service_model.operation_model('DeleteObjects')`) before writing anything:
`Bucket`/`Delete` ({Objects: [{Key: ...}, ...], Quiet: bool}) request, `Deleted`/`Errors`
response -- each DeleteObjects call accepts at most 1000 keys (a real S3 API limit, not
invented), chunked here; any `Errors` in any chunk's response fails the asset clearly.
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


class S3DeleteObjectsAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Delete a list of S3 objects as a Dagster asset -- mirrors Airflow's
    `S3DeleteObjectsOperator`."""

    asset_name: str = Field(description="Dagster asset key.")
    bucket: str = Field(description="S3 bucket name.")
    keys: Optional[List[str]] = Field(default=None, description="Static list of keys to delete. Set this or keys_asset_key (not both).")
    keys_asset_key: Optional[str] = Field(default=None, description="Upstream asset key whose materialized value is a list of keys to delete (e.g. the output of s3_list_keys_asset). Set this or keys (not both).")
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    resource_key: Optional[str] = Field(default=None, description="Optional key of a registered aws_s3_resource. When set, credentials/session come from that resource instead of a bare boto3.client('s3', region_name=...).")

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (in addition to keys_asset_key, which is added automatically).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_keys_source(self):
        if bool(self.keys) == bool(self.keys_asset_key):
            raise ValueError("S3DeleteObjectsAssetComponent: set exactly one of `keys` or `keys_asset_key`.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="S3 Delete Objects Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        deps_keys = [dg.AssetKey(d.split("/")) for d in (component.deps or [])]
        required_resource_keys = {component.resource_key} if component.resource_key else set()

        ins = None
        if component.keys_asset_key:
            ins = {"_keys_upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.keys_asset_key))}

        @dg.asset(
            key=dg.AssetKey.from_user_string(component.asset_name),
            ins=ins,
            description=component.description or f"Delete objects from s3://{component.bucket}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
            required_resource_keys=required_resource_keys,
        )
        def _s3_delete_objects(context: dg.AssetExecutionContext, **upstream_kwargs) -> dg.MaterializeResult:
            keys_to_delete: List[str] = component.keys or list(upstream_kwargs.get("_keys_upstream") or [])
            if component.resource_key:
                s3 = getattr(context.resources, component.resource_key).get_client()
            else:
                import boto3
                s3 = boto3.client("s3", region_name=component.region_name)

            deleted: List[str] = []
            errors: List[Dict[str, Any]] = []
            for i in range(0, len(keys_to_delete), 1000):  # real S3 DeleteObjects limit: 1000 keys/call
                chunk = keys_to_delete[i:i + 1000]
                response = s3.delete_objects(Bucket=component.bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
                deleted.extend(d["Key"] for d in response.get("Deleted", []))
                errors.extend(response.get("Errors", []))

            if errors:
                raise dg.Failure(
                    description=f"S3 DeleteObjects reported {len(errors)} error(s) out of {len(keys_to_delete)} key(s).",
                    metadata={"errors": dg.MetadataValue.json(errors)},
                )

            context.log.info(f"Deleted {len(deleted)} object(s) from s3://{component.bucket}")
            return dg.MaterializeResult(metadata={
                "deleted_count": dg.MetadataValue.int(len(deleted)),
                "deleted_keys": dg.MetadataValue.json(deleted),
            })

        return dg.Definitions(assets=[_s3_delete_objects])
