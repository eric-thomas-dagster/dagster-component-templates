"""DagsterComputeLogsArchiveJobComponent.

Op-shaped job that archives Dagster compute logs (stdout/stderr per
step) from the primary compute log manager to cheaper cold storage,
then optionally deletes the original.

Practical shape: walks compute log dir (LocalComputeLogManager default
is `$DAGSTER_HOME/storage/<run_id>/compute_logs/*`), gzips files older
than `min_age_days`, uploads to S3 / GCS / ADLS / local dir, and if
`delete_after_archive=True` removes the local file.

Compute logs are typically the largest single storage bill in a
long-running Dagster deployment. Archiving old ones frees the primary
storage without losing the audit trail — cold storage retains
retrievable logs for compliance / debugging.
"""

import gzip
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DagsterComputeLogsArchiveJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that gzips + archives old Dagster compute log
    files, then optionally deletes them from primary storage.

    Supports three archive targets:
      - `local` — copy to another local directory (useful for testing
        or when primary storage is fast SSD + archive is a mounted
        cold-storage volume).
      - `s3` — upload to an S3 bucket (uses boto3).
      - `gcs` — upload to a GCS bucket (uses google-cloud-storage).
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="0 6 * * *", description="Cron schedule; default daily at 6am.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    source_dir: str = Field(
        default="",
        description=(
            "Root directory containing compute logs. Default (empty) resolves to "
            "$DAGSTER_HOME/storage — the standard LocalComputeLogManager location."
        ),
    )
    min_age_days: int = Field(
        default=7,
        ge=1,
        description="Archive files older than this many days.",
    )
    archive_target: str = Field(
        default="local",
        description="Archive destination type: local | s3 | gcs",
    )
    archive_local_dir: Optional[str] = Field(
        default=None,
        description="If archive_target=local, the destination directory.",
    )
    archive_bucket: Optional[str] = Field(
        default=None,
        description="If archive_target=s3|gcs, the bucket name.",
    )
    archive_prefix: str = Field(
        default="dagster-compute-logs-archive/",
        description="Key prefix inside the bucket (or subdir under archive_local_dir).",
    )
    delete_after_archive: bool = Field(
        default=False,
        description="If True, delete the primary log file after successful archive. Default False (safer).",
    )
    max_files_per_run: int = Field(
        default=1000,
        ge=1,
        description="Hard cap per tick.",
    )
    dry_run: bool = Field(
        default=True,
        description="If True (default), log what WOULD be archived without touching files.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _archive(context: dg.OpExecutionContext):
            source = _self.source_dir or os.path.join(
                os.environ.get("DAGSTER_HOME", ""), "storage"
            )
            if not source or not os.path.isdir(source):
                raise RuntimeError(
                    f"source_dir not resolvable: {source!r} (set DAGSTER_HOME or source_dir explicitly)"
                )

            cutoff = time.time() - (_self.min_age_days * 86400.0)

            # Walk source dir for compute log files.
            candidates: List[Path] = []
            for path in Path(source).rglob("compute_logs/*"):
                if not path.is_file():
                    continue
                try:
                    if path.stat().st_mtime < cutoff:
                        candidates.append(path)
                        if len(candidates) >= _self.max_files_per_run:
                            break
                except OSError:
                    continue

            context.log.info(
                f"Found {len(candidates)} compute log files older than {_self.min_age_days} days "
                f"(source={source}, target={_self.archive_target})"
            )

            if _self.dry_run or not candidates:
                return {
                    "dry_run": _self.dry_run,
                    "candidates": len(candidates),
                    "archived": 0,
                }

            archived = 0
            deleted = 0
            errors: List[str] = []

            # Set up the target-specific uploader once.
            uploader = _make_uploader(_self, source)

            for f in candidates:
                try:
                    # Gzip in place → new file with .gz suffix.
                    gz_path = f.with_suffix(f.suffix + ".gz")
                    with open(f, "rb") as fin, gzip.open(gz_path, "wb") as fout:
                        shutil.copyfileobj(fin, fout)
                    # Upload the .gz file.
                    relpath = f.relative_to(source)
                    uploader(gz_path, relpath)
                    archived += 1
                    # Clean up the local .gz.
                    gz_path.unlink()
                    if _self.delete_after_archive:
                        f.unlink()
                        deleted += 1
                except Exception as exc:  # noqa: BLE001
                    context.log.warning(f"Archive failed for {f}: {exc}")
                    errors.append(f"{f}: {exc}")

            context.log.info(
                f"Archived {archived}/{len(candidates)} files; deleted {deleted} originals; "
                f"{len(errors)} errors"
            )
            return {
                "dry_run": False,
                "candidates": len(candidates),
                "archived": archived,
                "deleted_originals": deleted,
                "errors": errors[:10],
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _archive()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            defs_kwargs["schedules"] = [dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )]
        return dg.Definitions(**defs_kwargs)


def _make_uploader(cfg, source_root):
    """Return a callable(local_gz_path, relpath) -> None per archive_target."""
    prefix = cfg.archive_prefix.strip("/")

    if cfg.archive_target == "local":
        if not cfg.archive_local_dir:
            raise ValueError("archive_target='local' requires archive_local_dir")
        dest_root = Path(cfg.archive_local_dir) / prefix

        def _upload(local_path, relpath):
            dest = dest_root / relpath.with_suffix(relpath.suffix + ".gz")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)
        return _upload

    if cfg.archive_target == "s3":
        if not cfg.archive_bucket:
            raise ValueError("archive_target='s3' requires archive_bucket")
        import boto3
        s3 = boto3.client("s3")

        def _upload(local_path, relpath):
            key = f"{prefix}/{relpath}.gz"
            s3.upload_file(str(local_path), cfg.archive_bucket, key)
        return _upload

    if cfg.archive_target == "gcs":
        if not cfg.archive_bucket:
            raise ValueError("archive_target='gcs' requires archive_bucket")
        from google.cloud import storage as _gcs
        client = _gcs.Client()
        bucket = client.bucket(cfg.archive_bucket)

        def _upload(local_path, relpath):
            blob = bucket.blob(f"{prefix}/{relpath}.gz")
            blob.upload_from_filename(str(local_path))
        return _upload

    raise ValueError(f"Unknown archive_target: {cfg.archive_target!r}")
