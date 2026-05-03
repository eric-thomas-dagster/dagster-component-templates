"""PerFileProcessorJobComponent.

List files in S3 / GCS / ADLS matching a pattern, run a Python callable per
file in parallel via DynamicOut. Optional archive-after-processing.

Use this for inbox-style flows: every 5 minutes, list new CSVs in S3 → parse
each → archive. No catalog clutter — files are ephemeral by nature.
"""

import fnmatch
import importlib
import os
from typing import Optional

import dagster as dg
from pydantic import Field


def _resolve(callable_path: str):
    module_path, fn_name = callable_path.split(":")
    mod = importlib.import_module(module_path)
    return getattr(mod, fn_name)


class PerFileProcessorJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """List files in cloud storage, fan out a callable per file."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule")
    default_status: str = Field(default="STOPPED")

    storage: str = Field(description="s3 | gcs | adls")

    # S3
    s3_bucket: Optional[str] = Field(default=None)
    s3_region: str = Field(default="us-east-1")
    s3_aws_profile: Optional[str] = Field(default=None)

    # GCS
    gcs_bucket: Optional[str] = Field(default=None)

    # ADLS
    adls_account_url: Optional[str] = Field(default=None, description="https://<account>.blob.core.windows.net")
    adls_container: Optional[str] = Field(default=None)

    prefix: str = Field(default="", description="Object prefix to scan")
    pattern: str = Field(default="*", description="Glob pattern for filename (e.g. '*.csv')")
    max_files_per_run: Optional[int] = Field(default=None, description="Cap number of files per run (None = no cap)")

    process_callable_path: str = Field(
        description="'module:function(file_info)' — file_info is dict with bucket/key/size/last_modified"
    )
    process_kwargs: Optional[dict] = Field(default=None)

    archive_prefix: Optional[str] = Field(
        default=None, description="If set, move each processed object to <archive_prefix>/<original_key>"
    )
    delete_after: bool = Field(default=False, description="Delete each object after successful processing")

    max_concurrent_tag_value: Optional[str] = Field(default=None)
    retry_max_retries: Optional[int] = Field(default=None)
    retry_delay_seconds: Optional[int] = Field(default=None)
    fail_on_empty: bool = Field(default=False)
    job_tags: Optional[dict] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        retry = None
        if self.retry_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_max_retries,
                delay=self.retry_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL,
            )
        op_tags = {"dagster/concurrency_key": self.max_concurrent_tag_value} if self.max_concurrent_tag_value else None

        def _list_s3():
            import boto3
            sess = boto3.Session(profile_name=_self.s3_aws_profile, region_name=_self.s3_region)
            client = sess.client("s3")
            files = []
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=_self.s3_bucket, Prefix=_self.prefix):
                for obj in page.get("Contents", []):
                    name = obj["Key"].rsplit("/", 1)[-1]
                    if fnmatch.fnmatch(name, _self.pattern):
                        files.append({
                            "storage": "s3", "bucket": _self.s3_bucket, "key": obj["Key"],
                            "size": obj["Size"], "last_modified": obj["LastModified"].isoformat(),
                        })
            return files

        def _list_gcs():
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(_self.gcs_bucket)
            files = []
            for blob in bucket.list_blobs(prefix=_self.prefix):
                name = blob.name.rsplit("/", 1)[-1]
                if fnmatch.fnmatch(name, _self.pattern):
                    files.append({
                        "storage": "gcs", "bucket": _self.gcs_bucket, "key": blob.name,
                        "size": blob.size,
                        "last_modified": blob.time_created.isoformat() if blob.time_created else None,
                    })
            return files

        def _list_adls():
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
            bsc = BlobServiceClient(account_url=_self.adls_account_url, credential=DefaultAzureCredential())
            cc = bsc.get_container_client(_self.adls_container)
            files = []
            for blob in cc.list_blobs(name_starts_with=_self.prefix):
                name = blob.name.rsplit("/", 1)[-1]
                if fnmatch.fnmatch(name, _self.pattern):
                    files.append({
                        "storage": "adls", "container": _self.adls_container, "key": blob.name,
                        "size": blob.size,
                        "last_modified": blob.creation_time.isoformat() if blob.creation_time else None,
                    })
            return files

        listers = {"s3": _list_s3, "gcs": _list_gcs, "adls": _list_adls}

        @dg.op(out=dg.DynamicOut())
        def _discover(context):
            lister = listers.get(_self.storage)
            if not lister:
                raise ValueError(f"unknown storage: {_self.storage}")
            files = lister()
            if _self.max_files_per_run:
                files = files[:_self.max_files_per_run]
            context.log.info(f"found {len(files)} file(s) matching {_self.prefix!r}/{_self.pattern!r}")
            if not files and _self.fail_on_empty:
                raise Exception("no files; fail_on_empty=True")
            for f in files:
                # mapping_key must be valid; replace separators
                key = f["key"].replace("/", "_").replace(".", "_")[:200]
                yield dg.DynamicOutput(f, mapping_key=key)

        @dg.op(retry_policy=retry, tags=op_tags)
        def _process(context, file_info):
            fn = _resolve(_self.process_callable_path)
            extra = _self.process_kwargs or {}
            try:
                result = fn(file_info, **extra)
                context.log.info(f"processed {file_info.get('key')} -> {str(result)[:200]}")
            except Exception as exc:
                context.log.error(f"failed to process {file_info.get('key')}: {exc}")
                raise

            # Post-process: archive or delete
            if _self.archive_prefix or _self.delete_after:
                if file_info["storage"] == "s3":
                    import boto3
                    sess = boto3.Session(profile_name=_self.s3_aws_profile, region_name=_self.s3_region)
                    s3 = sess.client("s3")
                    if _self.archive_prefix:
                        new_key = f"{_self.archive_prefix.rstrip('/')}/{file_info['key']}"
                        s3.copy_object(Bucket=file_info["bucket"], Key=new_key,
                                       CopySource={"Bucket": file_info["bucket"], "Key": file_info["key"]})
                        s3.delete_object(Bucket=file_info["bucket"], Key=file_info["key"])
                        context.log.info(f"archived to {new_key}")
                    elif _self.delete_after:
                        s3.delete_object(Bucket=file_info["bucket"], Key=file_info["key"])
                # gcs / adls archive/delete left as future work
            return {"key": file_info.get("key"), "result": str(result)[:1000]}

        @dg.op
        def _summary(context, results: list):
            context.log.info(f"completed {len(results)} file(s)")
            return {"processed": len(results)}

        @dg.job(name=self.job_name, tags=self.job_tags or None)
        def _the_job():
            files = _discover()
            processed = files.map(_process)
            _summary(processed.collect())

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
