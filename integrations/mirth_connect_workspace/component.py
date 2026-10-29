"""Mirth Connect Workspace Component.

Wraps **NextGen Connect / Mirth Connect** -- the dominant open-source HL7
interface engine for hospitals and labs -- as a Dagster workspace component.
One Dagster asset per configured HL7 channel. With `action: deploy`,
materializing a channel's asset deploys + starts it and confirms the real
resulting state by polling (Mirth's deploy/start/stop endpoints return 204
unconditionally -- success AND failure -- so a 204 alone never proves
anything happened; this is a documented, widely-reported gotcha, not an
assumption). An optional shared polling sensor turns each channel's
cumulative message counters into per-tick deltas and emits AssetObservation
only when something actually moved.

Verified against NextGen's own REST API docs (docs.nextgen.com, "Mirth
Connect REST API") plus the community's own documented operational gotchas
(nirmitee.io's "Mirth Connect REST API Gotchas" writeup, the Mirth community
wiki's Statistics/Status controller page, and mirthpy's route table) -- not
guessed:

    POST {base}/users/_login              form: username, password -> session cookie (not used here; see below)
    GET  {base}/channels                  -> channel list (id, name, ...)
    GET  {base}/channels/statuses         -> [{channelId, name, state}]  (state: STARTED/STOPPED/PAUSED/...)
    GET  {base}/channels/{id}/statistics  -> {channelId, serverId, received, filtered, queued, sent, error}
    POST {base}/channels/{id}/_deploy     -> 204 always
    POST {base}/channels/{id}/_undeploy   -> 204 always
    POST {base}/channels/{id}/_start      -> 204 always
    POST {base}/channels/{id}/_stop       -> 204 always

Gotchas baked into the transport, each one a documented real-world failure
mode, not speculative defensiveness:

  - Every call needs an `X-Requested-With` header (any value) or Mirth
    returns a bare 403 with no explanation -- it only checks the header is
    present. Easy to lose if someone "simplifies" this transport later, so
    it's hardcoded, not a configurable option.
  - Deploy/start/stop/undeploy all return 204 unconditionally. The only way
    to know whether a channel actually reached the state you asked for is
    polling `/channels/statuses` afterward -- `wait_for_state` below exists
    because of this, not as generic polling-for-polling's-sake.
  - Mirth ships with a self-signed TLS certificate by default, so
    `verify_ssl` defaults True but most lab/demo installs need it False
    (same shape as `ssis_workspace`'s `trust_server_certificate`).
  - Basic auth (not session-cookie `_login`) is this transport's default --
    the community's own documented recommendation for unattended/CI
    automation, since it has no session lifecycle to manage between calls
    from a Dagster run that may not execute back-to-back.
  - A channel must already be `enabled` in Mirth Administrator before
    `action: deploy` can bring it to STARTED -- this component does not
    manage that flag (a separate endpoint, `PUT /server/channelMetadata`,
    out of scope here) -- see the README.

Statistics are lifetime cumulative counters (no time-bounded variant exists
on this endpoint), so the polling sensor below diffs successive snapshots
into per-tick deltas itself, and `error_rate_threshold` is a cumulative
ratio, not a windowed one -- documented, not hidden, in both places.
"""
import time
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


_RUNNING_STATE = "STARTED"
_STATS_KEYS = ("received", "filtered", "queued", "sent", "error")


def _unwrap_list(payload: Any) -> List[Dict[str, Any]]:
    """Mirth's REST layer has inconsistently wrapped list responses across
    endpoints/versions in community reports (bare array, {"channel": [...]},
    or a single un-listed object when there's exactly one result). Normalize
    defensively instead of assuming one shape."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]
    return []


class MirthConnectTransport:
    """The one place HTTP calls to Mirth Connect happen. Swap for a fake
    implementing these methods in tests (or a future demo mode) instead of
    patching `requests` calls scattered through the component."""

    def __init__(self, base_url: str, username: str, password: str, verify_ssl: bool = True):
        self._base = base_url.rstrip("/")
        self._auth = (username, password)
        self._verify = verify_ssl
        # X-Requested-With: Mirth 403s with no explanation if this is missing
        # on ANY call -- it only checks presence, not value (see module docstring).
        self._headers = {"X-Requested-With": "dagster-community-components", "Accept": "application/json"}

    def list_channels(self) -> List[Dict[str, Any]]:
        import requests

        resp = requests.get(f"{self._base}/channels", headers=self._headers, auth=self._auth, verify=self._verify, timeout=30)
        resp.raise_for_status()
        return _unwrap_list(resp.json())

    def get_statuses(self) -> Dict[str, Dict[str, Any]]:
        import requests

        resp = requests.get(f"{self._base}/channels/statuses", headers=self._headers, auth=self._auth, verify=self._verify, timeout=30)
        resp.raise_for_status()
        rows = _unwrap_list(resp.json())
        return {row["channelId"]: row for row in rows if row.get("channelId")}

    def get_statistics(self, channel_id: str) -> Dict[str, Any]:
        import requests

        resp = requests.get(
            f"{self._base}/channels/{channel_id}/statistics", headers=self._headers, auth=self._auth, verify=self._verify, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):  # observed both bare-object and list-wrapped across versions
            data = data[0] if data else {}
        return data

    def deploy_channel(self, channel_id: str) -> None:
        import requests

        resp = requests.post(f"{self._base}/channels/{channel_id}/_deploy", headers=self._headers, auth=self._auth, verify=self._verify, timeout=60)
        resp.raise_for_status()

    def undeploy_channel(self, channel_id: str) -> None:
        import requests

        resp = requests.post(
            f"{self._base}/channels/{channel_id}/_undeploy", headers=self._headers, auth=self._auth, verify=self._verify, timeout=60
        )
        resp.raise_for_status()

    def start_channel(self, channel_id: str) -> None:
        import requests

        resp = requests.post(f"{self._base}/channels/{channel_id}/_start", headers=self._headers, auth=self._auth, verify=self._verify, timeout=30)
        resp.raise_for_status()

    def stop_channel(self, channel_id: str) -> None:
        import requests

        resp = requests.post(f"{self._base}/channels/{channel_id}/_stop", headers=self._headers, auth=self._auth, verify=self._verify, timeout=30)
        resp.raise_for_status()

    def wait_for_state(self, channel_id: str, target_states, poll_interval_seconds: int, timeout_seconds: int) -> str:
        """deploy/start/stop all return 204 unconditionally -- this is the only
        way to confirm the channel actually reached the state asked for."""
        deadline = time.time() + timeout_seconds
        state = self.get_statuses().get(channel_id, {}).get("state")
        while state not in target_states:
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Mirth channel {channel_id} did not reach {target_states} within {timeout_seconds}s (last state: {state})"
                )
            time.sleep(poll_interval_seconds)
            state = self.get_statuses().get(channel_id, {}).get("state")
        return state


class MirthChannelSpec(dg.Model, dg.Resolvable):
    """One Mirth Connect HL7 channel wrapped as a Dagster asset."""

    channel_id: str = Field(description="Mirth channel id (UUID -- Administrator > Channel > Edit Channel, or GET /channels).")
    asset_key: str = Field(description="Dagster asset key for this channel.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group (defaults to the component's group_name).")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys this channel depends on.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags, merged onto the component's defaults.")


class MirthConnectWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """N Mirth Connect HL7 channels as Dagster assets from one component
    instance, with one shared observation sensor and optional per-channel
    health checks.

    Example:
        ```yaml
        type: dagster_community_components.MirthConnectWorkspaceComponent
        attributes:
          base_url_env_var: MIRTH_BASE_URL       # e.g. https://mirth-host:8443/api
          username_env_var: MIRTH_USERNAME
          password_env_var: MIRTH_PASSWORD
          verify_ssl: false                       # stock self-signed cert
          channels:
            - channel_id: b5bda28f-7c29-4773-8bea-2eb5f17aa710
              asset_key: hl7/adt_feed
            - channel_id: 1a2b3c4d-5e6f-7890-abcd-ef1234567890
              asset_key: hl7/lab_results_feed
          action: deploy
          polling_sensor: true
          require_running: true
          error_rate_threshold: 0.05
        ```
    """

    base_url_env_var: str = Field(
        default="MIRTH_BASE_URL",
        description="Env var holding the Administrator REST API base URL, e.g. https://mirth-host:8443/api. The /api suffix is required -- it is the REST root, not the Administrator UI's own path.",
    )
    username_env_var: str = Field(default="MIRTH_USERNAME", description="Env var with the Mirth username (Basic auth).")
    password_env_var: str = Field(default="MIRTH_PASSWORD", description="Env var with the Mirth password (Basic auth).")
    verify_ssl: bool = Field(
        default=True,
        description="Mirth ships with a self-signed TLS certificate by default -- set false for lab/demo installs still using the stock cert (same shape as ssis_workspace's trust_server_certificate).",
    )

    channels: List[MirthChannelSpec] = Field(default_factory=list, description="HL7 channels to wrap as Dagster assets, one entry per channel.")
    assets_by_channel: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the asset spec for specific channels by channel_id. Mirrors the official "
            "dagster-databricks `DatabricksWorkspaceComponent.assets_by_task_key` shape (same "
            "convention `snowflake_workspace.assets_by_name` / `qlik_cloud_workspace.assets_by_app` "
            "use in this registry).\n\nPer-channel keys (all optional): key, group_name, description, "
            "deps (merged with the channel's own deps), metadata (merged), tags (merged), kinds "
            "(replaces), owners (replaces)."
        ),
    )

    action: str = Field(
        default="noop",
        description=(
            "noop (default) = external asset, declare-only, materialize does nothing. "
            "deploy = materialize deploys + starts the channel, confirmed by polling "
            "/channels/statuses (deploy/start both return 204 unconditionally per Mirth's own "
            "documented behavior -- a 204 alone never proves success). The channel must already "
            "be `enabled` in Mirth Administrator; this component does not manage that flag."
        ),
    )
    poll_interval_seconds: int = Field(default=5, description="Seconds between status polls after deploy/start. Only used when action=deploy.")
    timeout_seconds: int = Field(default=120, description="Max seconds to wait for a channel to reach STARTED before failing the run. Only used when action=deploy.")

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds ONE sensor (not one per channel) that polls each channel's cumulative "
            "message counters (received/filtered/queued/sent/error) and emits AssetObservation "
            "only for channels whose counters moved since the last tick -- diffed into per-tick "
            "deltas, since Mirth's statistics endpoint only exposes lifetime totals, not a "
            "windowed count."
        ),
    )
    sensor_name: str = Field(default="mirth_connect_workspace_observer", description="Sensor name. Only used when polling_sensor=True.")
    check_interval_seconds: int = Field(default=60, description="Seconds between sensor polls. Only used when polling_sensor=True.")

    require_running: bool = Field(
        default=False,
        description="If true, attach a `channel_running` asset_check per channel that fails when the channel's dashboard state isn't STARTED.",
    )
    error_rate_threshold: Optional[float] = Field(
        default=None,
        description=(
            "If set, attach a `channel_error_rate` asset_check per channel that fails when "
            "error / max(received, 1) exceeds this fraction. Computed from the channel's "
            "CUMULATIVE lifetime counters -- Mirth's statistics endpoint has no time-bounded "
            "variant, so this is a lifetime ratio, not a windowed rate."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group for channels without their own group_name.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags applied to every channel asset (merged with each channel's own tags).")
    owners: Optional[List[str]] = Field(default=None, description="Default asset owners for channels without their own.")

    demo_mode: bool = Field(
        default=False,
        description=(
            "Skip the real Mirth server entirely. Deploy/start/stop, channel statuses, and "
            "statistics all return synthetic, deterministic values instead of calling the REST "
            "API. Use to smoke-test the whole shape (assets_by_channel, the sensor, the checks) "
            "with no Mirth instance available. `channels:` is still declared explicitly as usual."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners",), csv=(), tags=("tags",))

    def _apply_asset_overrides(self, channel_id: str, base_kwargs: dict) -> dict:
        """Merge per-channel overrides from `assets_by_channel` into @asset kwargs.
        Mirrors `snowflake_workspace._apply_asset_overrides` / `qlik_cloud_workspace`'s copy exactly."""
        if not self.assets_by_channel:
            return base_kwargs
        override = self.assets_by_channel.get(channel_id)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result.pop("key", None)
            result["key"] = dg.AssetKey.from_user_string(str(override["key"]))
        if "group_name" in override:
            result["group_name"] = override["group_name"]
        if "description" in override:
            result["description"] = override["description"]
        if "deps" in override:
            existing = list(result.get("deps") or [])
            existing.extend(dg.AssetKey.from_user_string(d) for d in override["deps"])
            result["deps"] = existing
        if "metadata" in override:
            existing = dict(result.get("metadata") or {})
            existing.update(override["metadata"])
            result["metadata"] = existing
        if "tags" in override:
            existing = dict(result.get("tags") or {})
            existing.update(override["tags"])
            result["tags"] = existing
        if "kinds" in override:
            result["kinds"] = set(override["kinds"])
        if "owners" in override:
            result["owners"] = override["owners"]
        return result

    def _demo_stats(self, channel_id: str, tick: int) -> Dict[str, Any]:
        """Deterministic, monotonically-increasing synthetic counters -- seeded
        on channel_id so different channels drift apart like real traffic would."""
        base = (abs(hash(channel_id)) % 50) + 1
        received = base * tick
        error = tick // 5  # occasional errors, never more than ~20% of received
        return {
            "channelId": channel_id,
            "serverId": "demo-server",
            "received": received,
            "filtered": tick,
            "queued": 0,
            "sent": received - error,
            "error": error,
        }

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        _self = self

        def _transport() -> MirthConnectTransport:
            base_url = os.environ.get(_self.base_url_env_var, "")
            username = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var, "")
            if not base_url or not username:
                raise ValueError(f"{_self.base_url_env_var} and {_self.username_env_var} must both be set.")
            return MirthConnectTransport(base_url, username, password, verify_ssl=_self.verify_ssl)

        assets_list: List[dg.AssetsDefinition] = []
        for channel in self.channels:

            def _make_asset(_ch=channel):
                base_kwargs: Dict[str, Any] = {
                    "key": dg.AssetKey.from_user_string(_ch.asset_key),
                    "group_name": _ch.group_name or _self.group_name,
                    "description": _ch.description or f"Mirth Connect HL7 channel {_ch.channel_id}",
                    "deps": [dg.AssetKey.from_user_string(d) for d in (_ch.deps or [])],
                    "owners": _ch.owners or _self.owners,
                    "tags": {**(_self.tags or {}), **(_ch.tags or {})} or None,
                    "kinds": {"mirth", "hl7"},
                }
                kwargs = _self._apply_asset_overrides(_ch.channel_id, base_kwargs)

                @dg.asset(**kwargs)
                def _channel_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                    if _self.action == "noop":
                        return dg.MaterializeResult(metadata={"mirth/action": "noop", "mirth/channel_id": _ch.channel_id})

                    if _self.demo_mode:
                        context.log.info(f"[MIRTH DEMO] deploy + start {_ch.channel_id} -> STARTED")
                        return dg.MaterializeResult(
                            metadata={"mirth/channel_id": _ch.channel_id, "mirth/state": "STARTED", "mirth/demo_mode": True}
                        )

                    transport = _transport()
                    transport.deploy_channel(_ch.channel_id)
                    transport.start_channel(_ch.channel_id)
                    state = transport.wait_for_state(_ch.channel_id, {_RUNNING_STATE}, _self.poll_interval_seconds, _self.timeout_seconds)
                    return dg.MaterializeResult(metadata={"mirth/channel_id": _ch.channel_id, "mirth/state": state})

                return _channel_asset

            assets_list.append(_make_asset())

        sensors_list: List[Any] = []
        if self.polling_sensor and self.channels:
            channels_by_id = {ch.channel_id: ch for ch in self.channels}

            @dg.sensor(
                name=self.sensor_name,
                minimum_interval_seconds=self.check_interval_seconds,
                asset_selection=dg.AssetSelection.assets(*(dg.AssetKey.from_user_string(c.asset_key) for c in self.channels)),
            )
            def _workspace_observer(context: dg.SensorEvaluationContext):
                import json as _json

                last_snapshot: Dict[str, Dict[str, int]] = _json.loads(context.cursor) if context.cursor else {}
                new_snapshot: Dict[str, Dict[str, int]] = dict(last_snapshot)
                events = []

                demo_tick = int(last_snapshot.get("__tick", 0)) + 1 if _self.demo_mode else None

                for channel in channels_by_id.values():
                    try:
                        stats = _self._demo_stats(channel.channel_id, demo_tick) if _self.demo_mode else _transport().get_statistics(channel.channel_id)
                    except Exception as e:
                        context.log.error(f"Mirth statistics query failed for channel {channel.channel_id}: {e}")
                        continue

                    current = {k: int(stats.get(k) or 0) for k in _STATS_KEYS}
                    previous = last_snapshot.get(channel.channel_id)
                    new_snapshot[channel.channel_id] = current

                    if previous is None:
                        continue  # first sighting -- record baseline, nothing to diff against yet

                    deltas = {k: current[k] - previous.get(k, 0) for k in _STATS_KEYS}
                    if not any(deltas.values()):
                        continue

                    events.append(
                        dg.AssetObservation(
                            asset_key=dg.AssetKey.from_user_string(channel.asset_key),
                            metadata={
                                "mirth/channel_id": channel.channel_id,
                                **{f"mirth/{k}_total": current[k] for k in _STATS_KEYS},
                                **{f"mirth/{k}_delta": deltas[k] for k in _STATS_KEYS},
                            },
                        )
                    )

                if _self.demo_mode:
                    new_snapshot["__tick"] = demo_tick  # type: ignore[assignment]
                new_cursor = _json.dumps(new_snapshot)

                if events:
                    return dg.SensorResult(asset_events=events, cursor=new_cursor)
                return dg.SensorResult(cursor=new_cursor, skip_reason="No channel counter changes since last tick")

            sensors_list.append(_workspace_observer)

        checks_list: List[Any] = []
        if self.require_running:
            for channel in self.channels:

                def _make_running_check(_ch=channel):
                    @dg.asset_check(asset=dg.AssetKey.from_user_string(_ch.asset_key), name="channel_running")
                    def _check(context):
                        if _self.demo_mode:
                            return dg.AssetCheckResult(passed=True, description="[MIRTH DEMO] state=STARTED")
                        state = _transport().get_statuses().get(_ch.channel_id, {}).get("state")
                        return dg.AssetCheckResult(
                            passed=state == _RUNNING_STATE,
                            description=f"state={state}",
                            metadata={"mirth/state": state or "UNKNOWN"},
                        )

                    return _check

                checks_list.append(_make_running_check())

        if self.error_rate_threshold is not None:
            threshold = self.error_rate_threshold
            for channel in self.channels:

                def _make_error_rate_check(_ch=channel):
                    @dg.asset_check(asset=dg.AssetKey.from_user_string(_ch.asset_key), name="channel_error_rate")
                    def _check(context):
                        if _self.demo_mode:
                            return dg.AssetCheckResult(passed=True, description="[MIRTH DEMO] error_rate=0.0", metadata={"mirth/error_rate": 0.0})
                        stats = _transport().get_statistics(_ch.channel_id)
                        received = int(stats.get("received") or 0)
                        error = int(stats.get("error") or 0)
                        rate = error / max(received, 1)
                        return dg.AssetCheckResult(
                            passed=rate <= threshold,
                            description=f"error_rate={rate:.4f} (threshold={threshold}), lifetime cumulative",
                            metadata={"mirth/error_rate": rate, "mirth/error_total": error, "mirth/received_total": received},
                        )

                    return _check

                checks_list.append(_make_error_rate_check())

        return dg.Definitions(assets=assets_list, sensors=sensors_list, asset_checks=checks_list)
