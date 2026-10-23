# Manifest Conventions

This document defines conventions for entries in `manifest.json`. If you're adding a new component, follow these.

The manifest is the source of truth for the catalog UI, agent planners, and downstream tooling. Fields must accurately describe what a component does — not what it might do someday.

---

## `produces` — what kinds of Dagster objects the component emits

The values are Dagster's own component kinds (`ComponentTypeSpec(produces=...)`, validated against `ComponentProducesKind`), plus four the manifest adds. The single source for the list is `tools/produces_vocabulary.py`; `tests/test_manifest_produces_vocabulary.py` fails on anything else.

Dagster-native:

| Value | Meaning |
|---|---|
| `asset` | Emits exactly ONE `@dg.asset` or equivalent — a single asset key. |
| `asset_check` | Emits `@dg.asset_check` (asset checks decoupled from asset materializations). |
| `schedule` | Emits `@dg.schedule`. |
| `sensor` | Emits `@dg.sensor`. Often paired with `asset` or `multi_asset`. |
| `job` | Emits `@dg.job` (op-shaped, not asset-graph-shaped). |
| `resource` | Emits a `dg.ConfigurableResource` for other components to depend on. |

Manifest-only (no Dagster equivalent):

| Value | Meaning |
|---|---|
| `multi_asset` | Refines `asset`: emits N assets from ONE `@dg.multi_asset` or `@dg.graph_multi_asset`. Use ALSO when the component emits multiple SEPARATE `@dg.asset` decorators in one `Definitions` — the intent is "one component, many assets." Dagster itself only has `asset`. |
| `io_manager` | Emits an `IOManager` implementation. |
| `partitions_def` | Emits a partitions definition for other components to use. |
| `other` | None of the above (e.g. a patch or a compute log manager). |

`to_native_produces()` in `tools/produces_vocabulary.py` maps a manifest list to Dagster's own kinds (`multi_asset` becomes `asset`, manifest-only values are dropped). `consumes` is manifest-only; Dagster has no equivalent.

The older spellings `check` (now `asset_check`) and `external_asset` (now `asset`) are not valid.

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

## Genie planner fields — which builders offer a component, and which checks apply

Dagster Designer's planner (Genie) reads three optional top-level fields. Without them it falls back to defaults baked into the designer, so a new component only shows up in a guided builder after a designer release. Declare them and it shows up as soon as the manifest does. (Not to be confused with `agent_hints.requires_resources`, which is free-form documentation for planning agents; the field below is machine-read.)

- `builders`: list of builder ids whose planning pool should include the component. Known: `agents_pipelines` (the Agent Builder), `rag` (RAG & Vector Search).
- `genie_checks`: deterministic checks the designer runs on a plan that picks this component. Known: `model_provider` (every step that names a model also needs `model` + `api_key_env_var`), `per_row_misuse` (reject a plain per-row chain forced into a whole-pipeline component), `context_pipeline` (knowledge-base builds: first step `chunk`, `execution_mode: sql` limits, required models/keys). Use `[]` to switch the designer's defaults off for a component. Only add a check if your component's `steps` use the op names it assumes.
- `requires_resources`: list of `{component_id, config_path, reason}`. When the planned config has a string at `config_path` (dotted, e.g. `receipts.resource_key`), the plan lists `component_id` as a resource that must be installed (resources are installed from the Resources tab, never planned as picks). `config_path` must exist in the component's `schema.json`.

```json
"builders": ["rag", "agents_pipelines"],
"genie_checks": ["context_pipeline"],
"requires_resources": [
  {"component_id": "context_receipt_store", "config_path": "receipts.resource_key", "reason": "reads retrieval receipts back into every rebuild"}
]
```

`tests/test_manifest_genie_fields.py` checks the shapes and that each `requires_resources` entry names a real resource component and a real config path.

