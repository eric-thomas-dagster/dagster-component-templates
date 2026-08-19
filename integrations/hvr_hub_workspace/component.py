"""HVR Hub Workspace Component (Fivetran-shape for standalone HVR Hub 6.x).

Full Fivetran-shape workspace component:
- `HvrHubResource(ConfigurableResource)` — connection auth
- `workspace: HvrHubResource` — canonical `{{ env.XXX }}`-templated auth block
- `channel_selector:` — include/exclude filters (mirrors FivetranWorkspace connector_selector)
- `translation:` callable — per-asset customization hook
- `polling_sensor` (alias `generate_sensor`) — opt-in observation sensor
- `StateBackedComponent` — discovery cached to disk via `write_state_to_path`;
  code-location reloads are instant. Refresh via `dg utils refresh-defs-state`.
- `action:` field — `noop` (default; HVR CDC is continuous) OR `refresh`
  (materialize triggers `POST /channels/{c}/refresh` + polls, Fivetran-style).

Emits one Dagster asset per (channel × target-location × table). Optional
observation sensor polls integrate-lag per job and emits ObservationEvents.

Aligns with the same conventions as SnowflakeWorkspaceComponent /
MLflowWorkspaceComponent / QlikReplicateWorkspaceComponent / FivetranAccountComponent.

Auth: HVR 6.x REST API uses bearer JWT (`POST /auth/v1/password`).

Endpoints exercised:
    POST /auth/v1/password                                — bearer JWT
    GET  /api/{ver}/hubs/{hub}/definition/channels        — list channels
    GET  /api/{ver}/hubs/{hub}/definition/channels/{c}/tables       — tables
    GET  /api/{ver}/hubs/{hub}/definition/channels/{c}/loc_groups   — locations
    GET  /api/{ver}/hubs/{hub}/jobs?fetch=latency         — integrate lag per job
    POST /api/{ver}/hubs/{hub}/channels/{c}/refresh       — optional trigger (action=refresh)
"""
import fnmatch
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    Resolver,
)
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import (
    TranslationFn,
    TranslationFnResolver,
)
from dagster_shared.record import record
from pydantic import Field


# ── Translator props ─────────────────────────────────────────────────


@record
class HvrObjectProps:
    """Data passed to translation callables for each imported HVR object.

    Mirrors `FivetranConnectorTableProps` / `QlikReplicateObjectProps` /
    `SnowflakeObjectProps` — a single record describing the object so
    `translation:` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'table' (currently the only enumerated kind).
        object_name: The table name.
        channel: The HVR channel name.
        target_loc: The HVR target-side location-group name.
        hub_name: The HVR hub name (surfaces multi-hub deployments).
        extra: Kind-specific metadata (column info at discovery, etc.).
    """
    object_kind: str
    object_name: str
    channel: Optional[str] = None
    target_loc: Optional[str] = None
    hub_name: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


# ── Resource ─────────────────────────────────────────────────────────


class HvrHubResource(dg.ConfigurableResource):
    """HVR Hub Server connection.

    Mirrors `FivetranWorkspace` / `QlikReplicateResource` / `PowerBIWorkspace` /
    `SnowflakeResource` — a `ConfigurableResource` holding just the connection
    fields. Values typically arrive via `{{ env.XXX }}` templating from YAML.
    """

    hub_url: str = Field(
        description=(
            "Base URL of the HVR Hub Server, e.g. `http://hvr-hub.internal:4340` "
            "(4340 is the default HVR HTTP port). No trailing slash, no `/api` suffix."
        ),
    )
    hub_name: str = Field(
        description="HVR hub name as declared on the Hub Server (e.g. `prod_hub`).",
    )
    username: str = Field(description="HVR admin username.")
    password: str = Field(description="HVR admin password.")
    api_version: str = Field(
        default="latest",
        description="REST API path version. Default `latest`; pin (e.g. `v6.1.5.2`) for stability across upgrades.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="TLS cert verification. Set false for self-signed dev environments.",
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request timeout for HVR API calls.",
    )

    def client(self) -> "HvrHubClient":
        return HvrHubClient(
            hub_url=self.hub_url.rstrip("/"),
            hub_name=self.hub_name,
            username=self.username,
            password=self.password,
            api_version=self.api_version,
            verify_ssl=self.verify_ssl,
            timeout=self.request_timeout_seconds,
        )


# ── Selector ─────────────────────────────────────────────────────────


@dataclass
class ChannelSelector(dg.Resolvable):
    """Selector for filtering HVR channels.

    Mirrors the FivetranWorkspace `connector_selector` shape:

        channel_selector:
          by_name: [sales_cdc, orders_stream]     # exact names to include
          by_pattern: [sales_*, orders_*]         # globs to include
          exclude_by_name: [test_channel]         # exact names to exclude
          exclude_by_pattern: [*_deprecated, *_test]

    Empty `by_name` + empty `by_pattern` = include all channels.
    `exclude_by_*` always wins over `by_*`.
    """
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, channel_name: str) -> bool:
        if self.exclude_by_name and channel_name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(channel_name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and channel_name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(channel_name, p) for p in self.by_pattern):
            return True
        return False


# ── Translator ───────────────────────────────────────────────────────


class HvrHubComponentTranslator:
    """Base translator that turns HvrObjectProps into an AssetSpec.

    Mirrors FivetranAccountComponent / QlikReplicateComponentTranslator —
    user's `translation:` callable wraps this via TranslationFn.
    """
    def __init__(self, component: "HvrHubWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: HvrObjectProps) -> AssetSpec:
        prefix = self._component.asset_key_prefix or ["hvr", props.hub_name or "hub"]
        return AssetSpec(
            key=AssetKey([*prefix, props.channel or "?", props.target_loc or "?", props.object_name]),
            description=(
                f"HVR-replicated table `{props.object_name}` in channel "
                f"`{props.channel}` → target location `{props.target_loc}`."
            ),
            group_name=self._component.group_name,
            metadata={
                "hvr/channel": props.channel or "",
                "hvr/target_loc": props.target_loc or "",
                "hvr/table": props.object_name,
                "hvr/hub_name": props.hub_name or "",
                **({"hvr/columns": ", ".join(props.extra.get("columns", []))} if props.extra and props.extra.get("columns") else {}),
            },
            kinds=set(self._component.kinds or ["hvr", "cdc"]),
            tags=dict(self._component.tags or {}),
            owners=self._component.owners,
        )


# ── Component ────────────────────────────────────────────────────────


@public
class HvrHubWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Auto-emit one Dagster asset per HVR-replicated table.

    Fivetran-shape: one YAML wires the whole HVR Hub. On
    `write_state_to_path`, enumerate every channel + table across the Hub
    via REST (with optional `channel_selector` filtering). On
    `build_defs_from_state`, read the cached snapshot and emit one asset
    per (channel × target_loc × table). Materializing an asset either
    no-ops (default — HVR CDC is continuous) OR triggers a channel
    refresh (`action: refresh`) and polls to completion.

    Complements — does NOT overlap — the official `dagster-fivetran`
    package, which reaches Fivetran-SaaS-managed HVR connectors. This
    component is for customers running standalone HVR Hub on their own
    hardware.

    Example (canonical `workspace:` block, mirrors dagster-fivetran):

        ```yaml
        type: dagster_community_components.HvrHubWorkspaceComponent
        attributes:
          workspace:
            hub_url:  "{{ env.HVR_HUB_URL }}"
            hub_name: "{{ env.HVR_HUB_NAME }}"
            username: "{{ env.HVR_USERNAME }}"
            password: "{{ env.HVR_PASSWORD }}"
            verify_ssl: true
          channel_selector:
            by_pattern: [sales_*, orders_*]
            exclude_by_pattern: [*_test]
          group_name: hvr
          action: noop                      # or 'refresh' to trigger channel refresh on materialize
          polling_sensor: true              # emit integrate-lag observations
          freshness_lag_threshold_seconds: 900
        ```
    """

    # ── Connection ────────────────────────────────────────────────────
    workspace: Annotated[
        HvrHubResource,
        Resolver(
            lambda context, model: HvrHubResource(
                **resolve_fields(model, HvrHubResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "HVR Hub connection as an HvrHubResource (hub_url + hub_name + "
            "username + password + verify_ssl). Secrets typically arrive via "
            "`{{ env.XXX }}` Jinja templating in defs.yaml."
        ),
    )

    # ── Translation hook ──────────────────────────────────────────────
    translation: Annotated[
        Optional[TranslationFn[HvrObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate HVR object properties into Dagster asset "
            "specs. Called for each imported table. If unset, the base translator's "
            "default AssetSpec is used."
        ),
    )

    # ── Filters ───────────────────────────────────────────────────────
    channel_selector: Optional[ChannelSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for channel names.",
    )

    # ── Catalog / governance ──────────────────────────────────────────
    asset_key_prefix: Optional[List[str]] = Field(
        default=None,
        description=(
            "Asset key prefix parts. Default: `['hvr', <hub_name>]`. Every emitted "
            "asset gets `[<prefix>..., <channel>, <target_loc>, <table>]`."
        ),
    )
    group_name: Optional[str] = Field(
        default="hvr",
        description="Dagster asset group for all emitted assets.",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['hvr', 'cdc'].",
    )
    tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra tags applied to every emitted asset.",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners applied to every emitted asset.",
    )

    # ── Materialization behavior ──────────────────────────────────────
    action: str = Field(
        default="noop",
        description=(
            "What each asset does when materialized: `noop` (default — HVR CDC "
            "is continuous; there's nothing to trigger) or `refresh` (POST "
            "`/channels/{c}/refresh` + poll until integrate catches up)."
        ),
    )
    wait_for_completion: bool = Field(
        default=True,
        description="If `action: refresh` and this is true, poll until the refresh completes.",
    )
    poll_interval_seconds: int = Field(
        default=30,
        description="Poll interval while waiting for refresh completion.",
    )
    timeout_seconds: int = Field(
        default=3600,
        description="Give up waiting after this many seconds (asset materialization fails).",
    )

    # ── Observation sensor ────────────────────────────────────────────
    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor `{hub_name}_hvr_observer` that polls "
            "`GET /jobs?fetch=latency` and emits AssetObservation events with "
            "`integrate_lag_seconds`, `state`, `job_name`, `observed_at` metadata "
            "per asset. Matches the `polling_sensor` convention on "
            "FivetranAccountComponent / QlikReplicateWorkspaceComponent."
        ),
    )
    observation_interval_seconds: int = Field(
        default=300,
        description="Polling sensor cadence.",
    )
    freshness_lag_threshold_seconds: Optional[int] = Field(
        default=None,
        description=(
            "If set, emit an asset check `integrate_lag_within_sla` per asset — "
            "PASSES when the most recent `integrate_lag_seconds` observation ≤ "
            "threshold. Wire into an asset-checks-first schedule / alert."
        ),
    )

    # ── State backend ─────────────────────────────────────────────────
    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for Dagster Cloud."
        ),
    )

    # ── Translator hook ───────────────────────────────────────────────
    @public
    def get_asset_spec(self, props: HvrObjectProps) -> AssetSpec:
        """Generate an AssetSpec for a given HVR object.

        Override in a subclass to customize how HVR objects are converted
        to Dagster asset specs. Default delegates to the configured
        translator, which respects the `translation:` field.
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> HvrHubComponentTranslator:
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = HvrHubComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on hub_url + hub_name so multiple Hubs don't collide in a
        # shared local-filesystem state dir.
        composite = f"{self.workspace.hub_url}::{self.workspace.hub_name}"
        url_hash = hashlib.sha256(composite.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{url_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    # ── State-backed: discover + emit ─────────────────────────────────
    async def write_state_to_path(self, state_path: Path) -> None:
        """Discover channels + tables via REST + write to state_path as JSON."""
        client = self.workspace.client()
        rows: List[Dict[str, Any]] = []
        for channel_name in client.list_channels():
            if self.channel_selector and not self.channel_selector.matches(channel_name):
                continue
            try:
                tables = client.list_tables(channel_name)
                loc_groups = client.list_loc_groups(channel_name)
            except Exception:  # noqa: BLE001
                # Skip channel with a broken definition rather than fail the
                # whole discovery. Log-worthy, but not fatal.
                continue
            target_locs = _target_locations(loc_groups) or list(loc_groups.keys())
            for target_loc in target_locs:
                for table_name in tables:
                    rows.append({
                        "channel": channel_name,
                        "target_loc": target_loc,
                        "table": table_name,
                        "columns": [],  # column enumeration is a future enhancement
                    })
        snapshot = {
            "hub_url": self.workspace.hub_url,
            "hub_name": self.workspace.hub_name,
            "rows": rows,
            "polled_at": time.time(),
        }
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())
        rows = state.get("rows", [])
        hub_name = state.get("hub_name", self.workspace.hub_name)

        assets: List[Any] = []
        specs: List[AssetSpec] = []
        for row in rows:
            spec = self._spec_for_row(row, hub_name)
            specs.append(spec)
            assets.append(self._build_asset_or_spec(row, spec))

        checks: List[Any] = []
        if self.freshness_lag_threshold_seconds is not None and specs:
            checks.append(self._build_lag_check(specs))

        sensors: List[Any] = []
        if self.polling_sensor and rows:
            sensors.append(self._build_observation_sensor(rows))

        return Definitions(assets=assets, asset_checks=checks, sensors=sensors)

    def _spec_for_row(self, row: Dict[str, Any], hub_name: str) -> AssetSpec:
        props = HvrObjectProps(
            object_kind="table",
            object_name=row["table"],
            channel=row["channel"],
            target_loc=row["target_loc"],
            hub_name=hub_name,
            extra={"columns": row.get("columns") or []},
        )
        return self.get_asset_spec(props)

    def _build_asset_or_spec(self, row: Dict[str, Any], spec: AssetSpec):
        """Return an `@asset` when `action=refresh` (materializable), else the
        AssetSpec itself (external asset)."""
        if self.action == "noop":
            return spec

        # action = refresh → materializable asset that triggers channel refresh.
        channel = row["channel"]
        action = self.action

        @dg.asset(
            key=spec.key,
            description=spec.description,
            group_name=spec.group_name,
            tags=dict(spec.tags or {}),
            kinds=set(spec.kinds or []),
            metadata=dict(spec.metadata or {}),
            owners=list(spec.owners or []),
            required_resource_keys={"hvr_hub"},
        )
        def _asset(context: dg.AssetExecutionContext):
            hub: HvrHubResource = context.resources.hvr_hub  # type: ignore[attr-defined]
            client = hub.client()
            context.log.info(f"[hvr_hub] {action} channel={channel!r}")
            client.trigger_refresh(channel)
            if self.wait_for_completion:
                deadline = time.time() + self.timeout_seconds
                while time.time() < deadline:
                    time.sleep(self.poll_interval_seconds)
                    # Simple completion signal: the refresh job clears from `jobs`
                    # once it finishes. Better signals exist in stats/metrics; keep
                    # this coarse for v1.
                    jobs = client.list_jobs_with_latency()
                    refresh_running = any(
                        (j.get("name") or "").startswith(f"{channel}-refr") for j in jobs
                    )
                    if not refresh_running:
                        context.log.info(f"[hvr_hub] refresh for {channel!r} completed")
                        return dg.MaterializeResult(
                            metadata={"channel": channel, "action": action}
                        )
                raise dg.Failure(description=f"HVR refresh for {channel!r} timed out after {self.timeout_seconds}s")
            return dg.MaterializeResult(metadata={"channel": channel, "action": action, "waited": False})

        return _asset

    def _build_lag_check(self, specs: List[AssetSpec]):
        threshold = int(self.freshness_lag_threshold_seconds)  # type: ignore[arg-type]
        keys = [s.key for s in specs]

        @dg.multi_asset_check(
            specs=[
                dg.AssetCheckSpec(
                    name="integrate_lag_within_sla",
                    asset=k,
                    description=f"Integrate lag ≤ {threshold}s SLA.",
                )
                for k in keys
            ],
        )
        def _lag_check(context: dg.AssetCheckExecutionContext):
            for k in keys:
                lag: Optional[float] = None
                latest = context.instance.get_latest_data_version_record(k)
                if latest and latest.asset_observation is not None:
                    md = latest.asset_observation.metadata or {}
                    v = md.get("integrate_lag_seconds")
                    if v is not None:
                        lag = float(getattr(v, "value", v))
                if lag is None:
                    yield dg.AssetCheckResult(
                        asset_key=k, check_name="integrate_lag_within_sla",
                        passed=False, severity=dg.AssetCheckSeverity.WARN,
                        description="no observation event yet",
                    )
                else:
                    yield dg.AssetCheckResult(
                        asset_key=k, check_name="integrate_lag_within_sla",
                        passed=lag <= threshold, severity=dg.AssetCheckSeverity.ERROR,
                        metadata={"observed_lag_seconds": lag, "threshold_seconds": threshold},
                    )

        return _lag_check

    def _build_observation_sensor(self, rows: List[Dict[str, Any]]):
        sensor_name = f"{self.workspace.hub_name}_hvr_observer"
        interval = self.observation_interval_seconds
        _self = self

        job_to_keys: Dict[str, List[AssetKey]] = {}
        hub_name = _self.workspace.hub_name
        for row in rows:
            job_key = _integrate_job_name(row["channel"], row["target_loc"])
            spec = _self._spec_for_row(row, hub_name)
            job_to_keys.setdefault(job_key, []).append(spec.key)

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=interval,
            description=f"Polls HVR Hub {hub_name!r} for integrate-lag per job every {interval}s.",
        )
        def _observer(context: dg.SensorEvaluationContext):
            try:
                client = _self.workspace.client()
                jobs = client.list_jobs_with_latency()
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"[hvr_observer] poll failed: {e}")
                return dg.SkipReason(f"HVR poll failed: {e}")

            n_obs = 0
            observed_at = time.time()
            for job in jobs:
                job_name = job.get("name") or job.get("job") or ""
                lag = job.get("latency")
                state = job.get("state") or job.get("status")
                keys = job_to_keys.get(job_name, [])
                for key in keys:
                    context.instance.report_runless_asset_event(
                        dg.AssetObservation(
                            asset_key=key,
                            metadata={
                                "integrate_lag_seconds": float(lag) if lag is not None else -1.0,
                                "state": str(state) if state is not None else "unknown",
                                "job_name": job_name,
                                "observed_at": observed_at,
                            },
                        )
                    )
                    n_obs += 1
            return dg.SkipReason(f"emitted {n_obs} observation(s)" if n_obs else "no matching jobs")

        return _observer


# ── HTTP client ───────────────────────────────────────────────────────


class HvrHubClient:
    """Thin HVR Hub REST client — auth + discovery + refresh trigger."""

    def __init__(
        self,
        *,
        hub_url: str,
        hub_name: str,
        username: str,
        password: str,
        api_version: str = "latest",
        verify_ssl: bool = True,
        timeout: int = 30,
    ):
        self.hub_url = hub_url.rstrip("/")
        self.hub_name = hub_name
        self.username = username
        self.password = password
        self.api_version = api_version
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def _auth(self) -> str:
        if self._token and time.time() < (self._token_expires_at - 30):
            return self._token
        import requests
        r = requests.post(
            f"{self.hub_url}/auth/v1/password",
            json={"username": self.username, "password": self.password, "refresh": "token"},
            timeout=self.timeout, verify=self.verify_ssl,
        )
        r.raise_for_status()
        body = r.json()
        self._token = body["access_token"]
        self._token_expires_at = time.time() + int(body.get("expires_in", 900))
        return self._token

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        import requests
        r = requests.get(
            f"{self.hub_url}{path}",
            headers={"Authorization": f"Bearer {self._auth()}"},
            params=params, timeout=self.timeout, verify=self.verify_ssl,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        import requests
        r = requests.post(
            f"{self.hub_url}{path}",
            headers={"Authorization": f"Bearer {self._auth()}"},
            json=body or {}, timeout=self.timeout, verify=self.verify_ssl,
        )
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return {}

    def _hub_path(self, tail: str) -> str:
        return f"/api/{self.api_version}/hubs/{self.hub_name}{tail}"

    def list_channels(self) -> List[str]:
        body = self._get(self._hub_path("/definition/channels"))
        if isinstance(body, dict):
            return sorted(body.keys())
        if isinstance(body, list):
            return sorted(str(x.get("name", x)) if isinstance(x, dict) else str(x) for x in body)
        return []

    def list_tables(self, channel: str) -> List[str]:
        body = self._get(self._hub_path(f"/definition/channels/{channel}/tables"))
        if isinstance(body, dict):
            return sorted(body.keys())
        if isinstance(body, list):
            return sorted(str(x.get("name", x)) if isinstance(x, dict) else str(x) for x in body)
        return []

    def list_loc_groups(self, channel: str) -> Dict[str, Dict[str, Any]]:
        body = self._get(self._hub_path(f"/definition/channels/{channel}/loc_groups"))
        if isinstance(body, dict):
            return body
        if isinstance(body, list):
            return {str(x.get("name", x)): x for x in body if isinstance(x, dict)}
        return {}

    def list_jobs_with_latency(self) -> List[Dict[str, Any]]:
        body = self._get(self._hub_path("/jobs"), params={"fetch": "latency"})
        if isinstance(body, dict):
            return [{"name": name, **(job if isinstance(job, dict) else {})} for name, job in body.items()]
        if isinstance(body, list):
            return body
        return []

    def trigger_refresh(self, channel: str) -> Any:
        return self._post(self._hub_path(f"/channels/{channel}/refresh"))


# ── Helpers ───────────────────────────────────────────────────────────


def _target_locations(loc_groups: Dict[str, Dict[str, Any]]) -> List[str]:
    """Best-effort identification of target-side location groups. HVR marks
    targets via a `Target: y` action on the loc_group; falls back to naming
    heuristics when action metadata isn't in the loc_groups response."""
    targets: List[str] = []
    for name, body in loc_groups.items():
        role = None
        if isinstance(body, dict):
            role = body.get("role") or body.get("Role")
            if role and str(role).lower() in ("target", "tgt"):
                targets.append(name)
                continue
        low = name.lower()
        if low.startswith(("t_", "tgt", "target")) or "_dw" in low or "_target" in low:
            targets.append(name)
    return sorted(set(targets))


def _integrate_job_name(channel: str, target_loc: str) -> str:
    """HVR integrate-job naming convention: `{channel}-integ-{target_loc}`.
    Customer can override via Job_Name action; this covers stock naming."""
    return f"{channel}-integ-{target_loc}"
