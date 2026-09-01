# Manifest Conventions

This document defines conventions for entries in `manifest.json`. If you're adding a new component, follow these.

The manifest is the source of truth for the catalog UI, agent planners, and downstream tooling. Fields must accurately describe what a component does — not what it might do someday.

---

## `produces` — what kinds of Dagster objects the component emits

Values:

| Value | Meaning |
|---|---|
| `asset` | Emits exactly ONE `@dg.asset` or equivalent — a single asset key. |
| `multi_asset` | Emits N assets from ONE `@dg.multi_asset` or `@dg.graph_multi_asset`. Use ALSO when the component emits multiple SEPARATE `@dg.asset` decorators in one `Definitions` — the intent is "one component, many assets." |
| `sensor` | Emits `@dg.sensor`. Often paired with `asset` or `multi_asset`. |
| `job` | Emits `@dg.job` (op-shaped, not asset-graph-shaped). |
| `resource` | Emits a `dg.ConfigurableResource` for other components to depend on. |
| `check` | Emits `@dg.asset_check` (asset checks decoupled from asset materializations). |
| `io_manager` | Emits an `IOManager` implementation. |

A single component can emit multiple kinds; use a list. Example:

```json
"produces": ["multi_asset", "sensor"]
```

`slack_approval_gate` emits one asset + one sensor → `["asset", "sensor"]`.
`agentic_pipeline` emits multiple assets from one `@multi_asset` → `["multi_asset"]`.

**Do NOT use `produces: ["asset"]` for components that actually emit N assets.** The catalog UI and agent planners rely on this distinction to know whether to expect one or many asset keys.

## `emitted_assets` — WHICH assets a `multi_asset` component emits

`produces: ["multi_asset"]` says "this component emits multiple assets" but doesn't say which. For discoverability, add the `emitted_assets` field alongside:

```json
"produces": ["multi_asset"],
"emitted_assets": {
  "pattern": "Free-form description for humans + agent readers. Explains what determines the emitted asset keys.",
  "source_field": "dot-notation path in the YAML config that lists the assets (e.g. `outputs.assets`, `steps[].id`, `sinks[].asset_name`).",
  "key_template": "Template showing how emitted asset keys are derived (e.g. `{asset_name_prefix}_{asset_id}`)."
}
```

Examples in the registry:

```json
// agentic_pipeline
"emitted_assets": {
  "pattern": "One Dagster asset per step listed in `outputs.assets`. Asset key = `{asset_name_prefix}_{asset_id}`.",
  "source_field": "outputs.assets",
  "key_template": "{asset_name_prefix}_{asset_id}"
}

// warehouse_pipeline
"emitted_assets": {
  "pattern": "One Dagster asset per entry in `sinks[]`. Asset key = each sink's `asset_name`.",
  "source_field": "sinks[].asset_name",
  "key_template": "{sinks[i].asset_name}"
}

// snowflake_workspace
"emitted_assets": {
  "pattern": "One Dagster asset per Snowflake object (tables, views, stages, streams, tasks, alerts, openflows) discovered under the specified database + schemas.",
  "source_field": "snowflake_information_schema.[tables|views|...]",
  "key_template": "{schema}.{object_name}"
}
```

The `pattern` field is the primary human-readable + agent-readable doc. `source_field` + `key_template` are best-effort machine-parseable hints — omit them if the emission logic is too dynamic to represent cleanly (e.g. `planned_catalog_agent`'s picks-depend-on-LLM shape).

## `validation.level` — how confident are we this component works?

| Level | Meaning |
|---|---|
| `code` | Passes `dg check defs`, unit tests, or component-level self-checks. No end-to-end run against a real backend. |
| `infra` | End-to-end validated against a MOCK / localstack / dev backend. |
| `live` | End-to-end validated against a REAL production-shape backend with real credentials. |

The catalog UI's "Trust & feedback" badges read from this field. Bump the level as you gain evidence.

## `agent_hints` — what a planning agent needs to know

Fields (all optional but strongly recommended for high-value components):

- `inputs`: what upstream inputs the component consumes.
- `outputs`: what shape the emitted asset(s) take (dict / DataFrame / passthrough).
- `side_effects`: what non-asset side effects happen (writes files, sends emails, makes API calls).
- `anti_uses`: shapes an agent might mistake for a fit — clarify WHEN NOT to pick.
- `requires_resources`: named Dagster resources the component depends on.
- `requires_pip`: pip packages needed at compute time (in addition to `requirements.txt`).
- `output_type`: single word capturing the asset value shape (`dataframe`, `passthrough`, `dict`, `path`, `asset+sensor`).

Agent planners (like `PlannedCatalogAgentComponent`) read these to make better picks.
