"""GeoIP Database Updater Component.

Downloads and refreshes local MaxMind GeoLite2 / GeoIP2 `.mmdb` database
files (City, Country, ASN, and their paid GeoIP2-* equivalents) from
MaxMind's documented GeoIP Update web service
(https://dev.maxmind.com/geoip/updating-databases), skipping the download
when the server's build is unchanged from the local file.

LICENSING: MaxMind GeoLite2 databases require a free MaxMind account and
EULA acceptance (required since 2019-12-30); GeoIP2 databases are a paid
product. This component downloads whatever your account_id/license_key
are entitled to -- it does not bundle, mirror, or redistribute any
database, and does not create a MaxMind account for you.

Implements the same metadata-then-conditional-download-then-md5-verify
protocol as MaxMind's official `geoipupdate` tool (and its Python
reimplementation, `pygeoipupdate` -- both were read directly to verify
the exact endpoints/auth/response-shape used below; neither is vendored
or imported here, this is an independent, synchronous implementation
using only `requests`, matching this registry's existing dependency
footprint instead of pygeoipupdate's own aiohttp/tenacity/filelock stack
and its Python >=3.11 floor, which would be stricter than this repo's
>=3.10 support).

Scheduling: this component only maintains the asset; pair it with a
CronScheduleComponent targeting its asset_name on MaxMind's own release
cadence (GeoLite2 is typically rebuilt Tuesdays and Fridays), e.g.
`cron_schedule: "0 6 * * 2,5"`. See example.yaml.
"""

import gzip
import hashlib
import os
import tarfile
import tempfile
from typing import Any, Dict, List, Optional

from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


_VALID_EDITION_ID_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"
)


def _local_mmdb_md5(path: str) -> str:
    """MD5 of the current local .mmdb file, or "" if it doesn't exist yet
    (which can never equal a real server md5, so a missing file always
    triggers a download)."""
    if not os.path.exists(path):
        return ""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _maxmind_get_metadata(session, host, auth, edition_id, timeout):
    """GET {host}/geoip/updates/metadata?edition_id=... -> (date, md5) for
    the edition's current build on the server. Documented at
    https://dev.maxmind.com/geoip/updating-databases -- verified directly
    against pygeoipupdate's client.py (same endpoint/params/auth)."""
    r = session.get(
        f"{host}/geoip/updates/metadata",
        params={"edition_id": edition_id},
        auth=auth,
        timeout=timeout,
    )
    if r.status_code == 401:
        raise RuntimeError(
            "MaxMind rejected the account_id/license_key (401) -- check the "
            "env vars named by account_id_env_var/license_key_env_var."
        )
    r.raise_for_status()
    data = r.json()
    databases = data.get("databases") or []
    if not databases:
        raise RuntimeError(
            f"MaxMind metadata response did not include edition '{edition_id}' -- "
            "check the edition name and that your account is entitled to it."
        )
    db = databases[0]
    return db["date"], db["md5"]


def _maxmind_download_and_extract(session, host, auth, edition_id, date_str, dest_path, timeout, expected_md5):
    """Download the edition's tar.gz, extract the single .mmdb member to a
    temp file, verify its MD5 against the server's advertised md5 BEFORE
    the atomic rename into dest_path -- a mismatch raises and leaves
    dest_path untouched, never replacing a good file with a bad one."""
    date_param = date_str.replace("-", "")
    r = session.get(
        f"{host}/geoip/databases/{edition_id}/download",
        params={"date": date_param, "suffix": "tar.gz"},
        auth=auth,
        timeout=timeout,
        stream=True,
    )
    if r.status_code == 401:
        raise RuntimeError(
            "MaxMind rejected the account_id/license_key (401) -- check the "
            "env vars named by account_id_env_var/license_key_env_var."
        )
    r.raise_for_status()

    dest_dir = os.path.dirname(dest_path) or "."
    gz_fd, gz_path = tempfile.mkstemp(suffix=".tar.gz", prefix=f"{edition_id}_", dir=dest_dir)
    try:
        with os.fdopen(gz_fd, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

        mmdb_fd, mmdb_tmp_path = tempfile.mkstemp(suffix=".mmdb.tmp", prefix=f"{edition_id}_", dir=dest_dir)
        try:
            md5_hash = hashlib.md5()
            extracted = False
            with os.fdopen(mmdb_fd, "wb") as out_f:
                with gzip.open(gz_path, "rb") as gz, tarfile.open(fileobj=gz, mode="r|") as tar:
                    for member in tar:
                        if not member.name.endswith(".mmdb"):
                            continue
                        member_f = tar.extractfile(member)
                        if member_f is None:
                            continue
                        while True:
                            chunk = member_f.read(65536)
                            if not chunk:
                                break
                            out_f.write(chunk)
                            md5_hash.update(chunk)
                        extracted = True
                        break
            if not extracted:
                raise RuntimeError(
                    f"Downloaded archive for '{edition_id}' did not contain an .mmdb file"
                )
            actual_md5 = md5_hash.hexdigest()
            if actual_md5.lower() != expected_md5.lower():
                raise RuntimeError(
                    f"MD5 mismatch for '{edition_id}': server advertised {expected_md5}, "
                    f"extracted file hashed to {actual_md5} -- download may be corrupt, not writing."
                )
            os.replace(mmdb_tmp_path, dest_path)
            return actual_md5
        except BaseException:
            if os.path.exists(mmdb_tmp_path):
                os.unlink(mmdb_tmp_path)
            raise
    finally:
        if os.path.exists(gz_path):
            os.unlink(gz_path)


class GeoIpDatabaseUpdaterComponent(Component, Model, Resolvable):
    """Downloads and refreshes MaxMind GeoLite2/GeoIP2 `.mmdb` database
    files on demand, skipping unchanged builds via MaxMind's own metadata
    endpoint. Pair with CronScheduleComponent for a recurring refresh --
    see the module docstring and README for the licensing requirements
    and recommended cadence.

    Example:
        ```yaml
        type: dagster_community_components.GeoIpDatabaseUpdaterComponent
        attributes:
          asset_name: geoip_databases
          editions:
            - GeoLite2-City
            - GeoLite2-ASN
          database_directory: /var/lib/geoip
          account_id_env_var: MAXMIND_ACCOUNT_ID
          license_key_env_var: MAXMIND_LICENSE_KEY
          group_name: analytics
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    editions: List[str] = Field(
        default=["GeoLite2-City"],
        description=(
            "MaxMind edition IDs to download and keep refreshed, e.g. "
            "'GeoLite2-City', 'GeoLite2-Country', 'GeoLite2-ASN', or the "
            "paid 'GeoIP2-City' / 'GeoIP2-Country' / 'GeoIP2-ASN' / "
            "'GeoIP2-ISP' equivalents -- your account_id/license_key must "
            "be entitled to each one requested. Each edition is written to "
            "'{database_directory}/{edition_id}.mmdb'."
        ),
    )

    database_directory: str = Field(
        description="Local directory the .mmdb files are written to and read from (created if missing)."
    )

    account_id_env_var: str = Field(
        default="MAXMIND_ACCOUNT_ID",
        description="Environment variable holding your MaxMind account ID.",
    )

    license_key_env_var: str = Field(
        default="MAXMIND_LICENSE_KEY",
        description="Environment variable holding your MaxMind license key.",
    )

    host: str = Field(
        default="https://updates.maxmind.com",
        description="MaxMind GeoIP Update service host. Override only for testing against a mock server.",
    )

    request_timeout_seconds: int = Field(
        default=60,
        description="Timeout for each metadata/download HTTP request.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['python']. Defaults to ['python'] if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 2,5' (the update check cadence).",
    )
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps", "editions"), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="GeoIP Database Updater", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        editions = self.editions or ["GeoLite2-City"]
        database_directory = self.database_directory
        account_id_env_var = self.account_id_env_var
        license_key_env_var = self.license_key_env_var
        host = self.host.rstrip("/")
        timeout = self.request_timeout_seconds
        group_name = self.group_name

        _all_tags = dict(self.asset_tags or {})
        for _kind in (self.kinds or ["python"]):
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        owners = self.owners or []

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or (
                f"Maintains local MaxMind .mmdb files for: {', '.join(editions)}"
            ),
            retry_policy=_retry_policy,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def geoip_database_updater_asset(context) -> MaterializeResult:
            import requests
            from requests.auth import HTTPBasicAuth

            account_id = os.environ.get(account_id_env_var)
            license_key = os.environ.get(license_key_env_var)
            if not account_id or not license_key:
                raise ValueError(
                    f"Set {account_id_env_var} and {license_key_env_var} with your "
                    "MaxMind account ID and license key (https://www.maxmind.com/en/accounts/current/license-key) "
                    "before running this component."
                )

            os.makedirs(database_directory, exist_ok=True)
            auth = HTTPBasicAuth(str(account_id), license_key)
            session = requests.Session()

            results: List[Dict[str, Any]] = []
            any_updated = False
            for edition_id in editions:
                if not edition_id or any(c not in _VALID_EDITION_ID_CHARS for c in edition_id):
                    raise ValueError(f"Invalid edition id: {edition_id!r}")
                dest_path = os.path.join(database_directory, f"{edition_id}.mmdb")
                server_date, server_md5 = _maxmind_get_metadata(session, host, auth, edition_id, timeout)
                local_md5 = _local_mmdb_md5(dest_path)

                if local_md5 and local_md5.lower() == server_md5.lower():
                    context.log.info(f"{edition_id}: up to date (build {server_date})")
                    results.append({
                        "edition_id": edition_id, "updated": False,
                        "build_date": server_date, "md5": server_md5,
                    })
                    continue

                actual_md5 = _maxmind_download_and_extract(
                    session, host, auth, edition_id, server_date, dest_path, timeout, server_md5
                )
                context.log.info(f"{edition_id}: updated to build {server_date}")
                any_updated = True
                results.append({
                    "edition_id": edition_id, "updated": True,
                    "build_date": server_date, "md5": actual_md5,
                })

            return MaterializeResult(metadata={
                "editions": MetadataValue.json(results),
                "any_updated": MetadataValue.bool(any_updated),
                "database_directory": MetadataValue.path(database_directory),
            })

        return Definitions(assets=[geoip_database_updater_asset])
