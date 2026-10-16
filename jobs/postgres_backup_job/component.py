"""PostgresBackupJobComponent.

Run pg_dump → upload to S3/GCS/local — backup as a job, no asset materialized.
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


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


class PostgresBackupJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run pg_dump → upload to S3/GCS/local — backup as a job, no asset materialized."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

    pg_host: str = Field(description="Postgres host")
    pg_port: int = Field(default=5432)
    pg_user_env: str = Field(default="PG_USER")
    pg_password_env: str = Field(default="PG_PASSWORD")
    pg_database: str = Field(description="Database name")
    pg_dump_bin: str = Field(default="pg_dump")
    output: str = Field(description="Destination: 'local:/path/to/file.dump' or 's3://bucket/key' (timestamp inserted before extension)")
    aws_profile: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import subprocess, os, datetime as dt, tempfile
            ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            # local path always — we either keep it or upload+delete
            with tempfile.NamedTemporaryFile(suffix=f"-{ts}.dump", delete=False) as f:
                tmp = f.name
            env = {**os.environ, "PGPASSWORD": _require_env(self.pg_password_env)}
            cmd = [self.pg_dump_bin, "-h", self.pg_host, "-p", str(self.pg_port),
                   "-U", _require_env(self.pg_user_env), "-Fc", "-f", tmp, self.pg_database]
            context.log.info(f"running pg_dump -> {tmp}")
            r = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if r.returncode != 0:
                raise Exception(f"pg_dump failed: {r.stderr[:500]}")
            size = os.path.getsize(tmp)
            if self.output.startswith("s3://"):
                import boto3
                sess = boto3.Session(profile_name=self.aws_profile)
                s3 = sess.client("s3")
                no_scheme = self.output.replace("s3://", "")
                bucket, _, key = no_scheme.partition("/")
                base, ext = (key.rsplit(".", 1) + [""])[:2]
                stamped = f"{base}-{ts}.{ext}" if ext else f"{key}-{ts}"
                s3.upload_file(tmp, bucket, stamped)
                os.unlink(tmp)
                context.log.info(f"uploaded {size} bytes to s3://{bucket}/{stamped}")
            else:
                local = self.output.replace("local:", "")
                base, ext = (local.rsplit(".", 1) + [""])[:2]
                stamped = f"{base}-{ts}.{ext}" if ext else f"{local}-{ts}"
                os.rename(tmp, stamped)
                context.log.info(f"wrote {size} bytes to {stamped}")

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
