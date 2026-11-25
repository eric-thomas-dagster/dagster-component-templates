# `StreamSetsEngineComponent`

Wrap a **standalone StreamSets Data Collector (SDC) engine** — no Control Hub involved — as Dagster assets. One asset per configured pipeline, with an optional status-change observation sensor.

## When to use this vs. `StreamSetsWorkspaceComponent`

Some StreamSets customers run only bare SDC engine(s), authoring and running pipelines directly against one engine's own REST API — no fleet orchestration, no "Job" concept. **This component is for that deployment shape.** `StreamSetsWorkspaceComponent` wraps a genuinely different product — Control Hub's Job Runner API (SaaS, or Control Hub's own self-managed on-prem install) — for customers whose StreamSets pipelines are managed as Jobs across a fleet of engines. If you're not sure which your prospect runs, check whether they have a Control Hub login at all; if not, this is the component.

## Live-verified, not guessed

Every endpoint path, the auth model, and the exact pipeline-status enum below were confirmed by running a real `streamsets/datacollector:6.4.1` Docker container and reading its own served OpenAPI spec (`GET /rest/swagger.json`) and hitting it directly — not sourced from blog posts.

**Confirmed live** (both the path exists AND the live container actually behaves this way):
- Auth is **HTTP Basic** (`security: [{"basic": []}]` in the live spec), credentials from the engine's own realm file. `X-Requested-By` is the documented convention but wasn't actually enforced for GET calls in this version when tested without it — sent anyway for safety.
- `GET /rest/v1/pipelines` — lists **every** pipeline on the engine. Real, working, live-confirmed. This is a genuine discovery endpoint `StreamSetsWorkspaceComponent`'s Control Hub API does **not** have.
- `GET /rest/v1/pipeline/{id}/status` — confirmed live. Returns 200 (not 404) even for a nonexistent pipeline id, with an empty body — treat a missing `status` key as "not found," not the HTTP status code.
- The full 20-value pipeline status enum (`EDITED`, `STARTING`, `STARTING_ERROR`, `START_ERROR`, `RUNNING`, `RUNNING_ERROR`, `RUN_ERROR`, `FINISHING`, `FINISHED`, `RETRY`, `KILLED`, `STOPPING`, `STOPPED`, `STOPPING_ERROR`, `STOP_ERROR`, `DISCONNECTING`, `DISCONNECTED`, `CONNECTING`, `CONNECT_ERROR`, `DELETED`) was read directly from the live engine's own `PipelineStateJson` schema definition.
- `GET /rest/v1/pipeline/{id}/metrics` — a real, reachable metrics endpoint (confirmed live: 200, empty body for a pipeline with no run history). Schema is a standard Dropwizard metrics registry (`gauges`/`counters`/`histograms`/`meters`/`timers`, each keyed by metric name) — again read from the live spec, not guessed. **Control Hub's sibling component has no confirmed metrics endpoint at all** — this is a genuine capability advantage of the engine API.

**Honest limitation — a real environmental wall, not a guess:** every state-*changing* call (create a pipeline, start, stop) returned HTTP 403 against this Docker image, regardless of which realm user/role was used — including a dedicated `creator`-role user whose role literally appears in the endpoint's own declared "allowed roles" list, which rules out a simple role mismatch. The container ships `streamsets-activation-no-trial-*.jar`, and `GET /rest/v1/activation` confirmed `"valid": false`: this specific Docker image has **no trial grace period** and requires a real StreamSets-issued activation code (requested via StreamSets' own Support portal, tied to the running instance's `sdcId`) before any write action is allowed — independent of role. Pure read endpoints (status, metrics, pipelines list, system info) were **not** subject to this gate and were fully exercised live. `start_pipeline`/`stop_pipeline`/`wait_for_status`'s request-building and response-parsing are exercised by this component's test suite against a real local HTTP server shaped like this API — but a real engine's actual RUNNING transition after a real start call was not observed first-hand. If you have a licensed instance (or Control Hub-registered engine, which doesn't hit this gate), that's the first thing to validate before relying on `action: start/stop` in production.

## Pipelines — explicit list, per-pipeline overrides

```yaml
pipelines:
  - pipeline_id: "orders_cdc_pipelineAbCd12"
    asset_key: streamsets_engine/orders_cdc_pipeline
  - pipeline_id: "clickstream_pipelineEfGh34"
    asset_key: streamsets_engine/clickstream_pipeline
    deps: [streamsets_engine/orders_cdc_pipeline]

assets_by_pipeline:
  "orders_cdc_pipelineAbCd12":
    key: cdc/orders
    group_name: cdc
```

Find `pipeline_id` via the engine's own Pipelines UI, or `GET /rest/v1/pipelines` directly (a real, working endpoint — see above). `assets_by_pipeline` mirrors `StreamSetsWorkspaceComponent`'s `assets_by_job` shape exactly: `key`/`group_name`/`description`/`kinds`/`owners` replace; `deps`/`metadata`/`tags` merge.

## Actions

- **`action: noop` (default)** — external asset shape, materialize does nothing.
- **`action: start`** — materialize calls the engine's start endpoint, then polls `status` until it reaches `RUNNING` (raising if it instead lands on any `*_ERROR`/`KILLED` status, or times out while in a transition state like `STARTING`).
- **`action: stop`** — materialize calls `stop` (or `forceStop` if `force_stop: true`), then polls until `STOPPED`.

## Metrics

```yaml
expose_metrics: true
```

When set, a successful `action: start` also fetches the real metrics endpoint and attaches the raw Dropwizard registry as asset metadata. Exact metric names depend on the pipeline's own stages — this passes them through best-effort rather than parsing specific counters, since real populated values couldn't be observed live (see the honest limitation above).

## Observing status changes — and, optionally, live metrics

```yaml
polling_sensor: true
check_interval_seconds: 60
observe_metrics_in_sensor: true   # optional
```

One shared sensor polls every declared pipeline's `status` and emits `AssetObservation` only when it actually changed since the last tick (first sighting is baseline-only).

With `observe_metrics_in_sensor: true`, the sensor ALSO fetches a live metrics snapshot (the same Dropwizard registry `expose_metrics` attaches at start time) and attaches it to the observation for every pipeline currently `RUNNING` **on every tick**, regardless of whether status changed — status-change-only observation would otherwise miss the common case of watching throughput climb while a pipeline stays `RUNNING` tick after tick. A `STOPPED` pipeline with unchanged status still isn't observed every tick; only a `RUNNING` one gets the continuous metrics feed.

Live-verified: the status/metrics GET calls themselves are read-only and confirmed to work without engine activation (unlike `action: start`/`stop`, see above) — the sensor's real transport code was run against a genuinely live bare engine and correctly handled a real (non-mocked) error response without crashing. The actual metrics-growing-over-time behavior (confirmed in this component's test suite against a real local HTTP server, not a plain mock) could not be observed against a truly live, running pipeline, since creating/starting one on a bare unregistered engine needs the same real activation code `action: start`/`stop` are blocked on.

## Health check

```yaml
require_running: true
```

Attaches a `pipeline_running` asset_check per pipeline that fails when the pipeline's current status isn't `RUNNING`.

## Demo mode — no engine required

`demo_mode: true` replaces every real REST call with a deterministic synthetic equivalent — start always lands on `RUNNING`, and the polling sensor occasionally simulates a `STARTING` blip (seeded per pipeline_id). Use it to smoke-test `assets_by_pipeline`, the sensor, and the check with no real engine available.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url_env_var` | `str` | `"STREAMSETS_ENGINE_URL"` | Env var holding the SDC engine's base URL, e.g. http://localhost:18630 (no Control Hub involved). |
| `username_env_var` | `str` | `"STREAMSETS_USERNAME"` | Env var with the engine login username (HTTP Basic -- confirmed live against this engine's own OpenAPI spec). |
| `password_env_var` | `str` | `"STREAMSETS_PASSWORD"` | Env var with the engine login password. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `5` | Seconds between status polls. Only used when action=start/stop. |
| `timeout_seconds` | `int` | `120` | Max seconds to wait for the target status before failing the run. Only used when action=start/stop. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group for pipelines without their own group_name. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Catalog tags applied to every pipeline asset (merged with each pipeline's own tags). |
| `owners` | `List[str]` | — | Default asset owners for pipelines without their own. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `"streamsets_engine_observer"` | Sensor name. Only used when polling_sensor=True. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `pipelines` | `List[StreamSetsPipelineSpec]` | `list()` | SDC pipelines to wrap as Dagster assets, one entry per pipeline. Find pipeline_id via the engine's Pipelines list UI or GET /rest/v1/pipelines (a real, working discovery endpoint on this engine -- unlike Control Hub's Job Runner API). |
| `assets_by_pipeline` | `Dict[str, Dict[str, Any]]` | — | Override the asset spec for specific pipelines by pipeline_id. Mirrors StreamSetsWorkspaceComponent's assets_by_job shape. Per-pipeline keys (all optional): key, group_name, description, deps (merged with the pipeline's… _(full docs in schema.json + component README)_ |
| `action` | `str` | `"noop"` | noop (default) = external asset, declare-only, materialize does nothing. start = materialize starts the pipeline and polls status until RUNNING (or raises if it instead lands on a *_ERROR/KILLED status, meaning the start… _(full docs in schema.json + component README)_ |
| `force_stop` | `bool` | `false` | When action=stop, use forceStop instead of a graceful stop. Ignored for action=start/noop. |
| `polling_sensor` | `bool` | `false` | If true, adds ONE sensor (not one per pipeline) that polls each pipeline's status and emits AssetObservation only when the status changed since the last tick. |
| `check_interval_seconds` | `int` | `60` | Seconds between sensor polls. Only used when polling_sensor=True. |
| `observe_metrics_in_sensor` | `bool` | `false` | If true (only used when polling_sensor=True), the sensor ALSO emits an AssetObservation carrying a live metrics snapshot (streamsets/metrics, same Dropwizard registry shape as expose_metrics) on every tick for each pipel… _(full docs in schema.json + component README)_ |
| `require_running` | `bool` | `false` | If true, attach a `pipeline_running` asset_check per pipeline that fails when the pipeline's current status isn't RUNNING. |
| `expose_metrics` | `bool` | `false` | If true, action=start fetches /rest/v1/pipeline/{id}/metrics after reaching RUNNING and attaches the raw Dropwizard metrics registry (gauges/counters/histograms/meters/timers, keyed by metric name) as asset metadata. A r… _(full docs in schema.json + component README)_ |
| `demo_mode` | `bool` | `false` | Skip the real engine connection entirely. Start/stop and status all return synthetic, deterministic values instead of calling the REST API. Use to smoke-test the whole shape with no SDC engine available. `pipelines:` is… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
