# `MirthConnectWorkspaceComponent`

Wrap **NextGen Connect / Mirth Connect** — the dominant open-source HL7 interface engine for hospitals, labs, and other healthcare systems — behind a Dagster workspace-shape component. One Dagster asset per HL7 channel. With `action: deploy`, materializing a channel deploys + starts it against the Administrator REST API and confirms it actually got there.

## When to use this

- You run Mirth Connect (or NextGen Connect) channels moving HL7 v2 messages between an EHR, LIS, RIS, or other hospital systems, and want Dagster to own their health/throughput observability — or their deploy lifecycle too.
- You want HL7 channels to appear in the same asset graph as the downstream warehouse tables an ADT or lab-results feed lands in, with real lineage (`deps:`) between them.
- You want per-channel alerting on "is this feed still flowing" and "is its error rate climbing" without hand-rolling REST polling.

## Real API, real gotchas (verified, not guessed)

Confirmed against NextGen's own REST API docs plus the community's own documented operational experience with this API (not assumed):

```
POST {base}/channels/{id}/_deploy      -> 204 always (success AND failure)
POST {base}/channels/{id}/_undeploy    -> 204 always
POST {base}/channels/{id}/_start       -> 204 always
POST {base}/channels/{id}/_stop        -> 204 always
GET  {base}/channels/statuses          -> [{channelId, name, state}]
GET  {base}/channels/{id}/statistics   -> {channelId, serverId, received, filtered, queued, sent, error}
```

Three gotchas this component bakes in, each one a documented, real-world failure mode:

1. **Every call needs an `X-Requested-With` header** (any value) or Mirth returns a bare 403 with no explanation — it only checks the header is present. Hardcoded in the transport, not a config option, since there's no reason anyone would want to omit it.
2. **Deploy/start/stop/undeploy all return 204 unconditionally.** A 204 never proves the channel actually reached the state you asked for — `action: deploy` always follows up by polling `/channels/statuses` until the channel is `STARTED` (or `timeout_seconds` elapses), rather than trusting the 204.
3. **Mirth ships with a self-signed TLS certificate by default.** `verify_ssl` defaults `true`, but most lab/demo installs still on the stock cert need it set `false` — same shape as `ssis_workspace`'s `trust_server_certificate`.

**Out of scope, by design:** this component does not manage a channel's `enabled` flag (a separate endpoint, `PUT /server/channelMetadata`) — a channel must already be enabled in Mirth Administrator before `action: deploy` can bring it to `STARTED`. It also doesn't fetch or ingest message *content* — statistics and status only. For parsing the raw HL7 v2 text itself once you've landed it somewhere (e.g. via a channel's own destination writing to a file/table Dagster also observes), pair this with the registry's `hl7_v2_parser` transform component.

## Auth

Basic auth (not Mirth's session-cookie `/users/_login` flow) is this transport's default — the community's own documented recommendation for unattended/CI automation, since Basic auth has no session lifecycle to manage between a Dagster run's deploy call and the polling that follows it.

```yaml
base_url_env_var: MIRTH_BASE_URL       # e.g. https://mirth-host:8443/api
username_env_var: MIRTH_USERNAME
password_env_var: MIRTH_PASSWORD
verify_ssl: false                       # true once you've replaced the stock self-signed cert
```

## Channels

```yaml
channels:
  - channel_id: b5bda28f-7c29-4773-8bea-2eb5f17aa710   # Mirth channel id (UUID)
    asset_key: hl7/adt_feed
    description: Admit-Discharge-Transfer feed from the EHR
  - channel_id: 1a2b3c4d-5e6f-7890-abcd-ef1234567890
    asset_key: hl7/lab_results_feed
    deps: [hl7/adt_feed]
```

Find a channel's id in Mirth Administrator (Channel > Edit Channel) or via `GET /channels`.

## assets_by_channel — per-channel overrides

```yaml
assets_by_channel:
  b5bda28f-7c29-4773-8bea-2eb5f17aa710:
    group_name: hl7_prod
    tags: {tier: critical}
```

Keyed by `channel_id`. `key` / `group_name` / `description` / `kinds` / `owners` replace; `deps` / `metadata` / `tags` merge onto what the channel's own fields already set. Mirrors the official `dagster-databricks` `DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention `snowflake_workspace.assets_by_name` and `qlik_cloud_workspace.assets_by_app` use elsewhere in this registry).

## Actions

- **`action: noop` (default)** — external asset shape. Every declared channel becomes a Dagster asset, but materialize does nothing. Use this when Dagster only observes Mirth (via `polling_sensor` / `require_running` / `error_rate_threshold`) without controlling its deploy lifecycle.
- **`action: deploy`** — materialize calls `POST /channels/{id}/_deploy` then `POST /channels/{id}/_start`, then polls `/channels/statuses` until the channel reaches `STARTED` or `timeout_seconds` elapses (raising on timeout). The channel must already be `enabled` in Mirth Administrator.

## Polling sensor — delta-only observation

Mirth's statistics endpoint exposes lifetime cumulative counters only (no windowed/rate variant), so `polling_sensor: true` adds **one** shared sensor (not one per channel) that snapshots each channel's `received` / `filtered` / `queued` / `sent` / `error` counts every tick, diffs them against the previous snapshot, and emits `AssetObservation` **only for channels whose counters actually moved** — never a no-op event on every single poll.

```yaml
polling_sensor: true
check_interval_seconds: 60
```

The first tick a channel is seen only records a baseline (nothing to diff against yet); it never emits a spurious bootstrap event.

## Checks

```yaml
require_running: true          # channel_running check: fails if dashboard state isn't STARTED
error_rate_threshold: 0.05     # channel_error_rate check: fails if lifetime error/received exceeds 5%
```

`channel_error_rate` is a **lifetime cumulative** ratio (`error / max(received, 1)`), not a windowed rate — Mirth's statistics endpoint has no time-bounded count, so that limitation is surfaced in the metadata rather than hidden behind a rate-looking number.

## Demo mode — no Mirth server required

`demo_mode: true` replaces every real API call with synthetic, deterministic values — deploy/start always "succeed" to `STARTED`, statistics grow monotonically per sensor tick (seeded per channel so different channels drift apart like real traffic would), and checks always pass. `channels:` is still declared explicitly as usual; only the transport is swapped. Use it to exercise the whole shape — `assets_by_channel`, the sensor's delta logic, both checks — with no real Mirth instance available.

```yaml
demo_mode: true
channels:
  - channel_id: demo-adt-feed
    asset_key: hl7/adt_feed
  - channel_id: demo-lab-feed
    asset_key: hl7/lab_results_feed
polling_sensor: true
require_running: true
error_rate_threshold: 0.05
```

## Companion components

- **`hl7_v2_parser`** — parses raw pipe-delimited HL7 v2 messages into a DataFrame (MSH/PID/OBX/ORC/OBR/PV1/EVN/DG1/AL1 segments). The natural downstream once a channel's own destination lands message content somewhere Dagster can read it.
- **`fhir_resource_normalizer`** — for shops migrating from HL7 v2 to FHIR alongside Mirth.
- **`snowflake_workspace` / other warehouse sinks** — where an ADT or lab-results feed ultimately lands.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url_env_var` | `str` | `"MIRTH_BASE_URL"` | Env var holding the Administrator REST API base URL, e.g. https://mirth-host:8443/api. The /api suffix is required -- it is the REST root, not the Administrator UI's own path. |
| `username_env_var` | `str` | `"MIRTH_USERNAME"` | Env var with the Mirth username (Basic auth). |
| `password_env_var` | `str` | `"MIRTH_PASSWORD"` | Env var with the Mirth password (Basic auth). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `5` | Seconds between status polls after deploy/start. Only used when action=deploy. |
| `timeout_seconds` | `int` | `120` | Max seconds to wait for a channel to reach STARTED before failing the run. Only used when action=deploy. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group for channels without their own group_name. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Catalog tags applied to every channel asset (merged with each channel's own tags). |
| `owners` | `List[str]` | — | Default asset owners for channels without their own. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `"mirth_connect_workspace_observer"` | Sensor name. Only used when polling_sensor=True. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `verify_ssl` | `bool` | `true` | Mirth ships with a self-signed TLS certificate by default -- set false for lab/demo installs still using the stock cert (same shape as ssis_workspace's trust_server_certificate). |
| `channels` | `List[MirthChannelSpec]` | `list()` | HL7 channels to wrap as Dagster assets, one entry per channel. |
| `assets_by_channel` | `Dict[str, Dict[str, Any]]` | — | Override the asset spec for specific channels by channel_id. Mirrors the official dagster-databricks `DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention `snowflake_workspace.assets_by_name` / `qlik_c… _(full docs in schema.json + component README)_ |
| `action` | `str` | `"noop"` | noop (default) = external asset, declare-only, materialize does nothing. deploy = materialize deploys + starts the channel, confirmed by polling /channels/statuses (deploy/start both return 204 unconditionally per Mirth'… _(full docs in schema.json + component README)_ |
| `polling_sensor` | `bool` | `false` | If true, adds ONE sensor (not one per channel) that polls each channel's cumulative message counters (received/filtered/queued/sent/error) and emits AssetObservation only for channels whose counters moved since the last… _(full docs in schema.json + component README)_ |
| `check_interval_seconds` | `int` | `60` | Seconds between sensor polls. Only used when polling_sensor=True. |
| `require_running` | `bool` | `false` | If true, attach a `channel_running` asset_check per channel that fails when the channel's dashboard state isn't STARTED. |
| `error_rate_threshold` | `float` | — | If set, attach a `channel_error_rate` asset_check per channel that fails when error / max(received, 1) exceeds this fraction. Computed from the channel's CUMULATIVE lifetime counters -- Mirth's statistics endpoint has no… _(full docs in schema.json + component README)_ |
| `demo_mode` | `bool` | `false` | Skip the real Mirth server entirely. Deploy/start/stop, channel statuses, and statistics all return synthetic, deterministic values instead of calling the REST API. Use to smoke-test the whole shape (assets_by_channel, t… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
