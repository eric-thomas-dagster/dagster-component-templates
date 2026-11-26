"""WaitUntilAssetComponent.

Hold a single Dagster step open until a condition becomes true -- Airflow's sensor-mode
operators (`PythonSensor`, `BashSensor`, `FileSensor`, `TimeSensor`, `TimeDeltaSensor`), which
poll INSIDE one task rather than starting a separate run. Different shape from this package's
own `python_sensor`/`*_monitor`/`event_automation` components, which react from outside a run
and start a NEW one -- this component is the step itself, blocking the run it's already part of.
"""
import time
from datetime import datetime, timedelta, timezone as _dt_timezone
from typing import Any, Dict, List, Literal, Optional, Union

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


def _resolve_callable(callable_path: str):
    import importlib
    if ":" not in callable_path:
        raise ValueError(f"callable_path must be 'module.path:function_name', got {callable_path!r}")
    mod_path, fn_name = callable_path.split(":", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


class WaitUntilAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Hold this step open, polling a condition every `poll_seconds`, until it's true or
    `timeout_seconds` elapses (then `on_timeout` fails or skips). Airflow sensor-mode
    equivalent -- polls INSIDE the run, not from outside it."""

    asset_name: str = Field(description="Dagster asset name.")
    condition: Literal[
        "callable", "shell_exit_zero", "file_exists", "time_of_day", "delay",
        "sftp_file_exists", "s3_key_exists", "gcs_object_exists", "http_ok",
    ] = Field(
        description=(
            "'callable': callable_path() returns truthy. 'shell_exit_zero': command exits 0. "
            "'file_exists': a path matching file_pattern exists. 'time_of_day': now (in "
            "timezone) has passed time_of_day. 'delay': delay_seconds have passed since the "
            "run's reference time (logical_date run tag if set, else this step's own start). "
            "'sftp_file_exists': sftp_path exists on the SFTP server. 's3_key_exists': "
            "s3_key exists in s3_bucket. 'gcs_object_exists': gcs_blob_name exists in "
            "gcs_bucket. 'http_ok': a GET to http_url returns a 2xx status."
        ),
    )
    callable_path: Optional[str] = Field(default=None, description="condition='callable' only (required then): 'module.path:function_name', called with no arguments each poll, returning truthy/falsy.")
    command: Optional[str] = Field(default=None, description="condition='shell_exit_zero' only (required then): shell command run via bash -c each poll.")
    retry_exit_code: Optional[int] = Field(
        default=None,
        description="condition='shell_exit_zero' only: this exit code means 'not ready yet, keep polling' (Airflow BashSensor's retry_exit_code). Any OTHER non-zero exit code fails immediately instead of polling further. Unset: any non-zero exit code fails immediately.",
    )
    file_pattern: Optional[str] = Field(default=None, description="condition='file_exists' only (required then): a glob pattern (e.g. '/data/inbox/*.csv') checked each poll.")
    time_of_day: Optional[str] = Field(default=None, description="condition='time_of_day' only (required then): 'HH:MM' (24h) in `timezone`. Met once today's wall-clock time has passed it.")
    delay_seconds: Optional[float] = Field(default=None, description="condition='delay' only (required then): seconds after the run's reference time that must have passed.")
    timezone: Optional[str] = Field(default=None, description="IANA timezone for condition='time_of_day' (default UTC).")

    sftp_host_env_var: Optional[str] = Field(default=None, description="condition='sftp_file_exists' only (required then): env var with the SFTP host. Same convention as sftp_to_database_asset's host_env_var.")
    sftp_username_env_var: Optional[str] = Field(default=None, description="condition='sftp_file_exists' only (required then): env var with the SFTP username.")
    sftp_password_env_var: Optional[str] = Field(default=None, description="condition='sftp_file_exists': env var with the SFTP password. Set this OR sftp_private_key_env_var.")
    sftp_private_key_env_var: Optional[str] = Field(default=None, description="condition='sftp_file_exists': env var with a path to an SSH private key file. Set this OR sftp_password_env_var.")
    sftp_port: int = Field(default=22, description="condition='sftp_file_exists': SFTP port.")
    sftp_path: Optional[str] = Field(default=None, description="condition='sftp_file_exists' only (required then): exact remote file path checked via SFTP stat each poll (no glob support -- SFTP has no native wildcard listing the way local file_exists does).")

    s3_bucket: Optional[str] = Field(default=None, description="condition='s3_key_exists' only (required then): S3 bucket name.")
    s3_key: Optional[str] = Field(default=None, description="condition='s3_key_exists' only (required then): exact S3 object key checked via head_object each poll.")
    aws_region: Optional[str] = Field(default=None, description="condition='s3_key_exists': AWS region. Unset uses boto3's default credential/region chain.")

    gcs_bucket: Optional[str] = Field(default=None, description="condition='gcs_object_exists' only (required then): GCS bucket name.")
    gcs_blob_name: Optional[str] = Field(default=None, description="condition='gcs_object_exists' only (required then): exact GCS object name checked each poll.")
    gcs_credentials_env_var: Optional[str] = Field(default=None, description="condition='gcs_object_exists': env var with a path to a GCP service account JSON key file. Unset uses application-default credentials. Same convention as gcs_to_database_asset's credentials_env_var.")

    http_url: Optional[str] = Field(default=None, description="condition='http_ok' only (required then): URL checked via GET each poll. A 2xx response means met; anything else (including a connection error) means not yet met.")
    http_auth_type: Literal["none", "basic", "bearer"] = Field(default="none", description="condition='http_ok': auth to attach to the GET request. Same convention as per_row_http_fetcher's auth_type (oauth2 not offered here -- a poll check has no use for a short-lived token refresh loop).")
    http_auth_username_env_var: Optional[str] = Field(default=None, description="condition='http_ok', http_auth_type='basic' only (required then): env var with the basic-auth username.")
    http_auth_password_env_var: Optional[str] = Field(default=None, description="condition='http_ok', http_auth_type='basic' only (required then): env var with the basic-auth password.")
    http_auth_token_env_var: Optional[str] = Field(default=None, description="condition='http_ok', http_auth_type='bearer' only (required then): env var with the bearer token.")
    http_timeout_seconds: float = Field(default=10, description="condition='http_ok': request timeout in seconds for each poll's GET.")
    poll_seconds: float = Field(default=30, description="Seconds between condition checks.")
    timeout_seconds: float = Field(default=3600, description="Give up after this many seconds total.")
    on_timeout: Literal["fail", "skip"] = Field(
        default="fail",
        description="'fail' (default): raise and fail the run. 'skip': materialize successfully with metadata timed_out=true (Dagster has no native per-asset 'skipped' status; same convention as shell_command_asset's skip_on_exit_code).",
    )

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

    @model_validator(mode="after")
    def _validate_condition_fields(self):
        required = {
            "callable": ("callable_path",), "shell_exit_zero": ("command",),
            "file_exists": ("file_pattern",), "time_of_day": ("time_of_day",),
            "delay": ("delay_seconds",),
            "sftp_file_exists": ("sftp_host_env_var", "sftp_username_env_var", "sftp_path"),
            "s3_key_exists": ("s3_bucket", "s3_key"),
            "gcs_object_exists": ("gcs_bucket", "gcs_blob_name"),
            "http_ok": ("http_url",),
        }[self.condition]
        for field_name in required:
            if not getattr(self, field_name):
                raise ValueError(f"WaitUntilAssetComponent: condition={self.condition!r} requires `{field_name}`.")
        if self.condition == "sftp_file_exists" and not (self.sftp_password_env_var or self.sftp_private_key_env_var):
            raise ValueError("WaitUntilAssetComponent: condition='sftp_file_exists' requires `sftp_password_env_var` or `sftp_private_key_env_var`.")
        if self.condition == "http_ok" and self.http_auth_type == "basic" and not (self.http_auth_username_env_var and self.http_auth_password_env_var):
            raise ValueError("WaitUntilAssetComponent: condition='http_ok' with http_auth_type='basic' requires `http_auth_username_env_var` and `http_auth_password_env_var`.")
        if self.condition == "http_ok" and self.http_auth_type == "bearer" and not self.http_auth_token_env_var:
            raise ValueError("WaitUntilAssetComponent: condition='http_ok' with http_auth_type='bearer' requires `http_auth_token_env_var`.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Wait Until Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        @dg.asset(
            key=dg.AssetKey.from_user_string(component.asset_name),
            description=component.description or f"Wait until condition={component.condition!r} is met.",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
        )
        def _wait_until(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            deadline = time.monotonic() + component.timeout_seconds
            started_at = time.monotonic()
            reference_time = _resolve_reference_time(context)
            attempts = 0
            while True:
                attempts += 1
                met, hard_fail = _check_condition(component, reference_time)
                if hard_fail:
                    raise dg.Failure(description=f"wait_until: condition check failed: {hard_fail}")
                if met:
                    return dg.MaterializeResult(metadata={
                        "met": dg.MetadataValue.bool(True),
                        "attempts": dg.MetadataValue.int(attempts),
                        "waited_seconds": dg.MetadataValue.float(time.monotonic() - started_at),
                    })
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if component.on_timeout == "fail":
                        raise dg.Failure(
                            description=f"wait_until: condition={component.condition!r} not met after {component.timeout_seconds}s ({attempts} attempt(s))."
                        )
                    return dg.MaterializeResult(metadata={
                        "met": dg.MetadataValue.bool(False),
                        "timed_out": dg.MetadataValue.bool(True),
                        "attempts": dg.MetadataValue.int(attempts),
                    })
                context.log.info(f"wait_until: condition={component.condition!r} not yet met (attempt {attempts}), polling again in {min(component.poll_seconds, remaining):.1f}s")
                time.sleep(min(component.poll_seconds, remaining))

        return dg.Definitions(assets=[_wait_until])


def _resolve_reference_time(context: dg.AssetExecutionContext) -> datetime:
    """Priority 1: this run's `logical_date` tag (set by cron_schedule's/interval_schedule's
    add_logical_date_tag/add_interval_tags), matching the convention python_callable_asset/
    conditional_asset already use. Priority 2: this step's own start time."""
    tagged = (context.run.tags or {}).get("logical_date")
    if tagged:
        return datetime.fromisoformat(tagged)
    return datetime.now(_dt_timezone.utc)


def _check_condition(component, reference_time: datetime):
    """Returns (met: bool, hard_fail: Optional[str])."""
    if component.condition == "callable":
        fn = _resolve_callable(component.callable_path)
        try:
            return bool(fn()), None
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"

    if component.condition == "shell_exit_zero":
        import subprocess
        proc = subprocess.run(
            component.command, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        if proc.returncode == 0:
            return True, None
        if component.retry_exit_code is not None and proc.returncode == component.retry_exit_code:
            return False, None
        return False, f"command exited {proc.returncode}: {proc.stderr.strip()[:500]}"

    if component.condition == "file_exists":
        import glob
        return bool(glob.glob(component.file_pattern)), None

    if component.condition == "time_of_day":
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(component.timezone) if component.timezone else _dt_timezone.utc
        now = datetime.now(tz)
        hh, mm = (int(p) for p in component.time_of_day.split(":"))
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return now >= target, None

    if component.condition == "delay":
        target = reference_time + timedelta(seconds=component.delay_seconds)
        now = datetime.now(reference_time.tzinfo or _dt_timezone.utc)
        return now >= target, None

    if component.condition == "sftp_file_exists":
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs: dict = {
            "username": _require_env(component.sftp_username_env_var),
            "port": component.sftp_port,
        }
        if component.sftp_private_key_env_var:
            connect_kwargs["key_filename"] = _require_env(component.sftp_private_key_env_var)
        elif component.sftp_password_env_var:
            connect_kwargs["password"] = _require_env(component.sftp_password_env_var)
        try:
            ssh.connect(_require_env(component.sftp_host_env_var), **connect_kwargs)
            sftp = ssh.open_sftp()
            try:
                sftp.stat(component.sftp_path)
                return True, None
            except FileNotFoundError:
                return False, None
            finally:
                sftp.close()
        except FileNotFoundError:
            return False, None
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"
        finally:
            ssh.close()

    if component.condition == "s3_key_exists":
        import boto3
        from botocore.exceptions import ClientError

        s3_client_kwargs = {}
        if component.aws_region:
            s3_client_kwargs["region_name"] = component.aws_region
        s3_client = boto3.client("s3", **s3_client_kwargs)
        try:
            s3_client.head_object(Bucket=component.s3_bucket, Key=component.s3_key)
            return True, None
        except ClientError as exc:
            status = exc.response.get("Error", {}).get("Code")
            if status in ("404", "NoSuchKey", "NotFound"):
                return False, None
            return False, f"{type(exc).__name__}: {exc}"

    if component.condition == "gcs_object_exists":
        import os

        from google.cloud import storage

        if component.gcs_credentials_env_var:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _require_env(component.gcs_credentials_env_var))
        try:
            gcs_client = storage.Client()
            blob = gcs_client.bucket(component.gcs_bucket).blob(component.gcs_blob_name)
            return blob.exists(), None
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"

    if component.condition == "http_ok":
        import requests

        session = requests.Session()
        if component.http_auth_type == "basic":
            session.auth = (_require_env(component.http_auth_username_env_var), _require_env(component.http_auth_password_env_var))
        elif component.http_auth_type == "bearer":
            session.headers["Authorization"] = f"Bearer {_require_env(component.http_auth_token_env_var)}"
        try:
            resp = session.get(component.http_url, timeout=component.http_timeout_seconds)
            return 200 <= resp.status_code < 300, None
        except requests.RequestException:
            return False, None

    return False, f"unknown condition: {component.condition!r}"
