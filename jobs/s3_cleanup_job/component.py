"""S3CleanupJobComponent.

Delete S3 objects by age, by an explicit key list, or both — cost control + compliance retention.
"""

import os
from typing import Optional

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


class S3CleanupJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Delete S3 objects by age, by an explicit key list, or both — cost control + compliance retention."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="Object prefix to scope cleanup")
    older_than_days: Optional[int] = Field(
        default=None,
        description="Delete objects older than this many days. Combines with `keys` (both apply) if both are set. At least one of older_than_days / keys / a per-run `keys` run-config override should be set for a run to delete anything.",
    )
    keys: Optional[List[str]] = Field(
        default=None,
        description="Static list of specific S3 keys to delete explicitly, regardless of age. Combines with older_than_days (both apply) if both are set.",
    )
    region_name: str = Field(default="us-east-1")
    aws_profile: Optional[str] = Field(default=None)
    dry_run: bool = Field(default=False, description="Report what would be deleted without deleting")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('keys',), csv=(), tags=('tags',))


    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='S3 Cleanup Job', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(
            name=f"{self.job_name}_op",
            config_schema={
                "keys": dg.Field(
                    [str], is_required=False, default_value=[],
                    description=(
                        "Per-run explicit S3 keys to delete, in ADDITION to the component's own "
                        "static `keys` field -- the mechanism for upstream-asset-driven deletion: "
                        "a sensor that reads an upstream asset's materialized output (e.g. a list "
                        "of file keys an ingestion asset just wrote) can launch this job with "
                        "run_config={'ops': {'<op_name>': {'config': {'keys': [...]}}}}."
                    ),
                ),
            },
        )
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import boto3, datetime as dt
            session = boto3.Session(profile_name=self.aws_profile, region_name=self.region_name)
            s3 = session.client("s3")

            explicit_keys = set(self.keys or []) | set(context.op_config.get("keys") or [])
            batch = [{"Key": k} for k in explicit_keys]
            scanned = 0

            if self.older_than_days is not None:
                cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=self.older_than_days)
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
                    for obj in page.get("Contents", []):
                        scanned += 1
                        if obj["LastModified"] < cutoff and obj["Key"] not in explicit_keys:
                            batch.append({"Key": obj["Key"]})

            deleted = 0
            for i in range(0, len(batch), 1000):
                chunk = batch[i:i + 1000]
                if not self.dry_run:
                    s3.delete_objects(Bucket=self.bucket, Delete={"Objects": chunk})
                deleted += len(chunk)
            context.log.info(
                f"scanned {scanned} (age-based), {len(explicit_keys)} explicit key(s), "
                f"{'would delete' if self.dry_run else 'deleted'} {deleted} total"
            )

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
