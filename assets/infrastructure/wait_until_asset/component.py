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


def _sanitize_identifier(s: str) -> str:
    """Turn an arbitrary string (e.g. a slash-delimited asset key) into a valid Python
    identifier, so it can be used to build a unique op/graph name per instance."""
    import re as _re
    out = _re.sub(r"\W", "_", s)
    if not out or out[0].isdigit():
        out = "_" + out
    return out


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
    sftp_path: Optional[str] = Field(default=None, description="condition='sftp_file_exists' only (required then): remote file path checked via SFTP each poll. Exact path for match_mode='exact'; for 'prefix'/'wildcard' this is the pattern matched against a directory listing (see sftp_match_mode).")
    sftp_match_mode: Literal["exact", "prefix", "wildcard"] = Field(default="exact", description="condition='sftp_file_exists': 'exact' stats sftp_path directly. 'prefix' lists sftp_path's directory and succeeds if any entry's name starts with sftp_path's basename. 'wildcard' lists the same directory and succeeds if any entry matches sftp_path's basename as an fnmatch glob (e.g. '*.csv').")

    s3_bucket: Optional[str] = Field(default=None, description="condition='s3_key_exists' only (required then): S3 bucket name.")
    s3_key: Optional[str] = Field(default=None, description="condition='s3_key_exists' only (required then): checked each poll. Exact object key for match_mode='exact'; for 'prefix'/'wildcard' this is the pattern matched via list_objects_v2.")
    s3_match_mode: Literal["exact", "prefix", "wildcard"] = Field(default="exact", description="condition='s3_key_exists': 'exact' calls head_object on s3_key directly. 'prefix' lists objects under s3_key as a prefix and succeeds if at least one exists. 'wildcard' lists objects under the portion of s3_key before its first '*'/'?' and succeeds if any listed key matches the full s3_key pattern via fnmatch.")
    aws_region: Optional[str] = Field(default=None, description="condition='s3_key_exists': AWS region. Unset uses boto3's default credential/region chain.")

    gcs_bucket: Optional[str] = Field(default=None, description="condition='gcs_object_exists' only (required then): GCS bucket name.")
    gcs_blob_name: Optional[str] = Field(default=None, description="condition='gcs_object_exists' only (required then): checked each poll. Exact object name for match_mode='exact'; for 'prefix'/'wildcard' this is the pattern matched via list_blobs.")
    gcs_match_mode: Literal["exact", "prefix", "wildcard"] = Field(default="exact", description="condition='gcs_object_exists': 'exact' checks gcs_blob_name directly via blob.exists(). 'prefix' lists blobs under gcs_blob_name as a prefix and succeeds if at least one exists. 'wildcard' lists blobs under the portion of gcs_blob_name before its first '*'/'?' and succeeds if any listed name matches the full gcs_blob_name pattern via fnmatch.")
    gcs_credentials_env_var: Optional[str] = Field(default=None, description="condition='gcs_object_exists': env var with a path to a GCP service account JSON key file. Unset uses application-default credentials. Same convention as gcs_to_database_asset's credentials_env_var.")

    http_url: Optional[str] = Field(default=None, description="condition='http_ok' only (required then): URL checked each poll. A response whose status is in http_success_status_codes (default 2xx) means met; anything else (including a connection error) means not yet met.")
    http_method: str = Field(default="GET", description="condition='http_ok': HTTP method used for each poll (GET, POST, HEAD, ...).")
    http_headers: Optional[Dict[str, str]] = Field(default=None, description="condition='http_ok': extra headers sent with each poll request, on top of any auth header from http_auth_type.")
    http_success_status_codes: Optional[List[int]] = Field(default=None, description="condition='http_ok': explicit list of status codes that count as met (e.g. [200] if 202/404 should mean 'not yet' rather than success). Unset defaults to any 2xx status, matching prior behavior.")
    http_auth_type: Literal["none", "basic", "bearer"] = Field(default="none", description="condition='http_ok': auth to attach to the request. Same convention as per_row_http_fetcher's auth_type (oauth2 not offered here -- a poll check has no use for a short-lived token refresh loop).")
    http_auth_username_env_var: Optional[str] = Field(default=None, description="condition='http_ok', http_auth_type='basic' only (required then): env var with the basic-auth username.")
    http_auth_password_env_var: Optional[str] = Field(default=None, description="condition='http_ok', http_auth_type='basic' only (required then): env var with the basic-auth password.")
    http_auth_token_env_var: Optional[str] = Field(default=None, description="condition='http_ok', http_auth_type='bearer' only (required then): env var with the bearer token.")
    http_timeout_seconds: float = Field(default=10, description="condition='http_ok': request timeout in seconds for each poll.")
    poll_seconds: float = Field(default=30, description="Seconds between condition checks.")
    timeout_seconds: float = Field(default=3600, description="Give up after this many seconds total.")
    on_timeout: Literal["fail", "skip"] = Field(
        default="fail",
        description="'fail' (default): raise and fail the run. 'skip': a real Dagster structural skip (AssetOut(is_required=False), the output is never yielded) -- this asset and every downstream asset depending on it are marked skipped, not materialized, matching Airflow's soft_fail semantics. Same mechanism conditional_asset uses for its gate/branch outputs.",
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
        asset_key = dg.AssetKey.from_user_string(component.asset_name)

        # `on_timeout='skip'` needs a REAL Dagster structural skip -- AssetOut(is_required=False)
        # plus not yielding an Output on timeout -- so this and every downstream asset are marked
        # skipped (not materialized), matching Airflow's soft_fail. Same mechanism
        # conditional_asset uses for its gate/branch outputs. 'fail' mode always yields or raises,
        # so is_required=True there behaves identically to the previous plain @dg.asset.
        out = dg.AssetOut(
            key=asset_key,
            description=component.description or f"Wait until condition={component.condition!r} is met.",
            is_required=(component.on_timeout != "skip"),
            owners=component.owners or None,
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
        )

        op_name = "_wait_until_" + _sanitize_identifier("_".join(asset_key.path))

        @dg.multi_asset(
            name=op_name,
            outs={"result": out},
            group_name=component.group_name,
            deps=deps_keys,
        )
        def _wait_until(context: dg.AssetExecutionContext):
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
                    yield dg.Output(
                        None, output_name="result",
                        metadata={
                            "met": dg.MetadataValue.bool(True),
                            "attempts": dg.MetadataValue.int(attempts),
                            "waited_seconds": dg.MetadataValue.float(time.monotonic() - started_at),
                        },
                    )
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if component.on_timeout == "fail":
                        raise dg.Failure(
                            description=f"wait_until: condition={component.condition!r} not met after {component.timeout_seconds}s ({attempts} attempt(s))."
                        )
                    context.log.info(
                        f"wait_until: condition={component.condition!r} not met after {component.timeout_seconds}s "
                        f"({attempts} attempt(s)) -- on_timeout='skip', this asset and its downstream will be skipped."
                    )
                    return
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
        import fnmatch
        import posixpath

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
                if component.sftp_match_mode == "exact":
                    sftp.stat(component.sftp_path)
                    return True, None
                dirname = posixpath.dirname(component.sftp_path) or "."
                basename = posixpath.basename(component.sftp_path)
                entries = sftp.listdir(dirname)
                if component.sftp_match_mode == "prefix":
                    return any(e.startswith(basename) for e in entries), None
                return any(fnmatch.fnmatch(e, basename) for e in entries), None
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
        import fnmatch
        import re

        import boto3
        from botocore.exceptions import ClientError

        s3_client_kwargs = {}
        if component.aws_region:
            s3_client_kwargs["region_name"] = component.aws_region
        s3_client = boto3.client("s3", **s3_client_kwargs)
        try:
            if component.s3_match_mode == "exact":
                s3_client.head_object(Bucket=component.s3_bucket, Key=component.s3_key)
                return True, None
            if component.s3_match_mode == "prefix":
                resp = s3_client.list_objects_v2(Bucket=component.s3_bucket, Prefix=component.s3_key, MaxKeys=1)
                return resp.get("KeyCount", 0) > 0, None
            literal_prefix = re.split(r"[*?]", component.s3_key, maxsplit=1)[0]
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=component.s3_bucket, Prefix=literal_prefix):
                for obj in page.get("Contents", []):
                    if fnmatch.fnmatch(obj["Key"], component.s3_key):
                        return True, None
            return False, None
        except ClientError as exc:
            status = exc.response.get("Error", {}).get("Code")
            if status in ("404", "NoSuchKey", "NotFound"):
                return False, None
            return False, f"{type(exc).__name__}: {exc}"

    if component.condition == "gcs_object_exists":
        import fnmatch
        import os
        import re

        from google.cloud import storage

        if component.gcs_credentials_env_var:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _require_env(component.gcs_credentials_env_var))
        try:
            gcs_client = storage.Client()
            bucket = gcs_client.bucket(component.gcs_bucket)
            if component.gcs_match_mode == "exact":
                return bucket.blob(component.gcs_blob_name).exists(), None
            if component.gcs_match_mode == "prefix":
                return next(iter(gcs_client.list_blobs(bucket, prefix=component.gcs_blob_name, max_results=1)), None) is not None, None
            literal_prefix = re.split(r"[*?]", component.gcs_blob_name, maxsplit=1)[0]
            for blob in gcs_client.list_blobs(bucket, prefix=literal_prefix):
                if fnmatch.fnmatch(blob.name, component.gcs_blob_name):
                    return True, None
            return False, None
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
            resp = session.request(
                component.http_method, component.http_url,
                headers=component.http_headers, timeout=component.http_timeout_seconds,
            )
            if component.http_success_status_codes is not None:
                return resp.status_code in component.http_success_status_codes, None
            return 200 <= resp.status_code < 300, None
        except requests.RequestException:
            return False, None

    return False, f"unknown condition: {component.condition!r}"
