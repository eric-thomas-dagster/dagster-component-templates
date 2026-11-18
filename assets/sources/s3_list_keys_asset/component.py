"""S3ListKeysAssetComponent.

List S3 object keys under a bucket/prefix as a Dagster asset -- mirrors Airflow's
`S3ListOperator` (paginated `list_objects_v2`, `prefix`/`delimiter` support, returns the key
list -- `delimiter` also surfaces "common prefixes", i.e. one level of subfolder names, exactly
as the real boto3 paginator does)."""
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


class S3ListKeysAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """List S3 object keys under a bucket/prefix as a Dagster asset -- mirrors Airflow's
    `S3ListOperator`."""

    asset_name: str = Field(description="Dagster asset key.")
    bucket: str = Field(description="S3 bucket name.")
    prefix: str = Field(default="", description="Only list keys starting with this prefix.")
    delimiter: Optional[str] = Field(
        default=None,
        description="Group keys by this delimiter (e.g. '/'): only keys immediately under `prefix` are returned as `keys`, and one level of subfolder names is returned separately as `common_prefixes` -- matches boto3's real list_objects_v2(Delimiter=...) semantics (Airflow's S3ListOperator exposes the same split).",
    )
    region_name: Optional[str] = Field(default=None, description="AWS region.")
    resource_key: Optional[str] = Field(default=None, description="Optional key of a registered aws_s3_resource (resources/aws_s3_resource). When set, credentials/session come from that resource instead of a bare boto3.client('s3', region_name=...).")

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="S3 List Keys Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        required_resource_keys = {component.resource_key} if component.resource_key else set()

        @dg.asset(
            key=dg.AssetKey.from_user_string(component.asset_name),
            description=component.description or f"List of S3 keys under s3://{component.bucket}/{component.prefix}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
            required_resource_keys=required_resource_keys,
        )
        def _s3_list_keys(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if component.resource_key:
                s3 = getattr(context.resources, component.resource_key).get_client()
            else:
                import boto3
                s3 = boto3.client("s3", region_name=component.region_name)

            paginate_kwargs: Dict[str, Any] = {"Bucket": component.bucket, "Prefix": component.prefix}
            if component.delimiter:
                paginate_kwargs["Delimiter"] = component.delimiter

            keys: List[str] = []
            common_prefixes: List[str] = []
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(**paginate_kwargs):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
                for cp in page.get("CommonPrefixes", []):
                    common_prefixes.append(cp["Prefix"])

            context.log.info(f"Listed {len(keys)} key(s) under s3://{component.bucket}/{component.prefix}")
            return dg.MaterializeResult(metadata={
                "keys": dg.MetadataValue.json(keys),
                "common_prefixes": dg.MetadataValue.json(common_prefixes),
                "count": dg.MetadataValue.int(len(keys)),
            })

        return dg.Definitions(assets=[_s3_list_keys])
