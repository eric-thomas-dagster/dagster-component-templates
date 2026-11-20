"""McpTokenRefresherComponent -- the ONE thing that refreshes a hosted-MCP OAuth token stored via
`token_store='state'` and writes a new version to Dagster's own `defs_state_storage`.

Everything else (`HostedMcpServerComponent`'s `build_defs`/stdio subprocess, or any asset reading
a state-backed token directly) only ever reads the latest version and uses the access token as
given -- it never refreshes. This is deliberate, not a missing feature: a hosted-MCP server's
refresh token is typically REPLACED on every refresh (a stale copy held elsewhere dies after
first use), and `defs_state_storage`'s own `set_latest_version` is last-writer-wins -- two
processes refreshing the same key at once would invalidate each other's result.

Emits a plain `@dg.job` (an `@dg.op`), not an asset: a token refresh has no durable data product
or catalog lineage worth tracking -- it's a side-effecting maintenance action, the same shape as
this repo's other op/job-based maintenance components (e.g. `warm_scheduled_job`), not an
asset-reconciliation one. Set `cron_schedule` (an inline field here, not the usual "pair with
`CronScheduleComponent`" convention -- that component only targets asset selections, not plain
jobs) to a cadence comfortably inside the access token's own lifetime (commonly ~1-8h); the
emitted schedule defaults to RUNNING (unlike most schedules in this repo), since an un-started
refresher silently defeats this component's entire purpose.

**No portable way to prune old versions** -- checked, not assumed: Dagster's `DefsStateStorage`
interface has no generic list-versions/delete-version method for a key (confirmed from the
abstract base class's own docstring on `set_latest_version`: "Stored bytes for prior versions
are not cleaned up by this call; backends that care about orphan cleanup should handle it
out-of-band."). Configure a lifecycle/retention policy at the storage backend itself (e.g. an S3
bucket lifecycle rule under the `__state__/<key>/` prefix) if you need this -- this component
cannot portably do it itself.

**This is a secret with the same blast radius as the refresh token.** The stored blob grants
everything the signed-in person can see on the target MCP server. Your `defs_state_storage`'s
own backend needs the same access control you'd give any other secret store. Dagster's own
`UPathDefsStateStorage` explicitly does not support a LOCAL FILESYSTEM `base_path` when state is
read from a different process than wrote it (confirmed: it raises a clear error naming this
exact case) -- use a real remote blob backend (`s3://`, `gs://`, `azure://`, `abfs://`) if the
refresher and the components reading this key might ever run in different processes/machines,
which is the normal case in any real deployment.
"""
import time
from typing import Any, Dict, Literal, Optional

import dagster as dg
from pydantic import Field


# BEGIN mcp state token store (inlined per component; keep copies identical — see tests/test_mcp_state_token_store_drift.py)
def _read_mcp_state_token_blob(state_key: str) -> Dict[str, Any]:
    """Reads the current {access_token, expires_at, refresh_token, client_id, client_secret,
    token_endpoint} blob for state_key from the Dagster instance's configured
    defs_state_storage. Only ever called from build_defs (component load time) or a refresher's
    own run body -- never from the stdio subprocess itself, which gets a resolved access token
    baked into its env instead (see token_store='state' in the module docstring)."""
    from dagster._core.storage.defs_state.base import DefsStateStorage

    storage = DefsStateStorage.get()
    if storage is None:
        raise RuntimeError(
            "token_store='state' requires a defs_state_storage configured on this Dagster "
            "instance (dagster.yaml's defs_state_storage: section) -- none is configured here."
        )
    version = storage.get_latest_version(state_key)
    if version is None:
        raise RuntimeError(
            f"No token found in defs_state_storage for state_key={state_key!r}. Sign in first: "
            f"python -m dagster_community_components.resources.mcp_oauth_login --url <mcp url> "
            f"--state-key {state_key!r}"
        )
    import json as _json
    import tempfile as _tempfile
    from pathlib import Path as _Path

    with _tempfile.TemporaryDirectory() as tmp_dir:
        path = _Path(tmp_dir) / "blob.json"
        storage.download_state_to_path(state_key, version, path)
        return _json.loads(path.read_text())


def _write_mcp_state_token_blob(state_key: str, blob: Dict[str, Any]) -> None:
    """The ONE place this should ever be called from -- a scheduled refresher
    (McpTokenRefresherComponent), per this component family's 'one writer' contract (everything
    else only reads the latest version). upload_state_from_path also sets the latest-version
    pointer (confirmed against Dagster's own UPathDefsStateStorage implementation -- it calls
    set_latest_version itself after writing the blob)."""
    from dagster._core.storage.defs_state.base import DefsStateStorage

    storage = DefsStateStorage.get()
    if storage is None:
        raise RuntimeError(
            "token_store='state' requires a defs_state_storage configured on this Dagster "
            "instance (dagster.yaml's defs_state_storage: section) -- none is configured here."
        )
    import json as _json
    import tempfile as _tempfile
    import uuid as _uuid
    from pathlib import Path as _Path

    with _tempfile.TemporaryDirectory() as tmp_dir:
        path = _Path(tmp_dir) / "blob.json"
        path.write_text(_json.dumps(blob))
        storage.upload_state_from_path(state_key, version=str(_uuid.uuid4()), path=path)
# END mcp state token store


def _refresh_token_blob(blob: Dict[str, Any], state_key: str) -> Dict[str, Any]:
    """Performs the refresh_token grant against blob['token_endpoint'] and returns a NEW blob
    (same shape, access_token/refresh_token/expires_at updated). Raises dg.Failure with a clear
    'sign in again' message on a rejected refresh token -- never a raw HTTP/auth exception."""
    import requests

    refresh_token = blob.get("refresh_token")
    if not refresh_token:
        raise dg.Failure(
            description=(
                f"No refresh_token stored for state_key={state_key!r} -- sign in again: "
                f"python -m dagster_community_components.resources.mcp_oauth_login --url <mcp "
                f"url> --state-key {state_key!r}"
            ),
        )
    token_endpoint = blob.get("token_endpoint")
    if not token_endpoint:
        raise dg.Failure(description=f"No token_endpoint stored for state_key={state_key!r} -- sign in again.")

    resp = requests.post(
        token_endpoint,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": blob.get("client_id"),
            "client_secret": blob.get("client_secret") or "",
        },
        headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if resp.status_code in (400, 401):
        raise dg.Failure(
            description=(
                f"Refresh token for state_key={state_key!r} was rejected (HTTP "
                f"{resp.status_code}). Sign in again: python -m "
                f"dagster_community_components.resources.mcp_oauth_login --url <mcp url> "
                f"--state-key {state_key!r}"
            ),
        )
    resp.raise_for_status()
    data = resp.json()
    return {
        **blob,
        "access_token": data["access_token"],
        "refresh_token": data.get("refresh_token") or refresh_token,
        "expires_at": time.time() + data.get("expires_in", 3600),
    }


class McpTokenRefresherComponent(dg.Component, dg.Model, dg.Resolvable):
    """The one writer for a `token_store='state'` hosted-MCP OAuth token: refreshes via the
    stored refresh_token and uploads a new version to `defs_state_storage`. Emits a plain job
    (not an asset -- see module docstring for why) with an optional inline `cron_schedule`
    firing every few hours -- see module docstring for why this must be the ONLY thing
    refreshing this `state_key`."""

    job_name: str = Field(description="Dagster job name for the refresher.")
    state_key: str = Field(description="The defs_state_storage key to refresh -- must match the --state-key used at sign-in (mcp_oauth_login) and the consuming component's own state_key (e.g. HostedMcpServerComponent).")
    cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Optional cron expression (e.g. '0 */4 * * *' for every 4 hours). When set, a "
            "ScheduleDefinition fires this job on this cadence -- pick something comfortably "
            "inside the access token's own lifetime (commonly ~1-8h). Leave unset to trigger "
            "this job some other way (your own schedule/sensor)."
        ),
    )
    default_status: Literal["running", "stopped"] = Field(
        default="running",
        description=(
            "Status of the emitted schedule (cron_schedule only). Defaults to 'running', unlike "
            "most schedules in this repo -- an un-started refresher silently defeats this "
            "component's entire purpose (the token just expires with nothing catching it), "
            "unlike a stopped informational/optional schedule elsewhere."
        ),
    )
    description: Optional[str] = Field(default=None, description="Job description.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="MCP Token Refresher", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        state_key = self.state_key

        @dg.op(
            name=f"{self.job_name}_op",
            description=self.description or f"Refreshes the hosted-MCP OAuth token stored under state_key={state_key!r} -- the ONE writer for this key.",
        )
        def _refresh_op(context: dg.OpExecutionContext) -> None:
            blob = _read_mcp_state_token_blob(state_key)
            old_expires_at = blob.get("expires_at")
            new_blob = _refresh_token_blob(blob, state_key)
            _write_mcp_state_token_blob(state_key, new_blob)
            context.log.info(
                f"Refreshed token for state_key={state_key!r}; new expiry in "
                f"{new_blob['expires_at'] - time.time():.0f}s (old_expires_at={old_expires_at})."
            )

        @dg.job(name=self.job_name, description=self.description)
        def _refresh_job():
            _refresh_op()

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                job=_refresh_job,
                cron_schedule=self.cron_schedule,
                default_status=dg.DefaultScheduleStatus.RUNNING if self.default_status == "running" else dg.DefaultScheduleStatus.STOPPED,
            ))

        return dg.Definitions(jobs=[_refresh_job], schedules=schedules)

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
