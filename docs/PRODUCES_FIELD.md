# The `produces` field — spec + Dagster-side ask

## What is it

Optional metadata on each entry in `manifest.json` that declares which Dagster
primitives a component instance creates when loaded.

```jsonc
{
  "id": "snowflake_workspace",
  "produces": ["multi_asset"],
  "stateful": true
}
```

Schema (all fields optional):

- **`produces`** — list of Dagster primitives the component emits. Enum:
  `asset` · `multi_asset` · `asset_check` · `job` · `schedule` · `sensor` ·
  `resource` · `io_manager` · `partitions_def` · `other`.
  Always a list. `multi_asset` supersedes `asset` (fanout implies asset shape).
- **`consumes`** — list of primitives read from. Enum: `asset` · `asset_check` · `resource`.
- **`stateful`** — `true` when the component holds state beyond its YAML
  (defs_state cache, live-refreshed workspace catalog, etc.).

## Coverage today

Backfilled deterministically via AST inspection of every `component.py` in the
registry (see `tools/infer_produces.py`). Confidence distribution across 944
components: 943 @ 1.00 (explicit decorator or `Definitions(...)` kwarg match),
1 @ 0.80 (base-class inference). Zero low-confidence rows.

Primitive counts:

| Primitive | Count |
|---|---:|
| asset | 632 |
| asset_check | 217 |
| resource | 138 |
| sensor | 100 |
| multi_asset | 43 |
| job | 41 |
| schedule | 40 |

## Why it matters (downstream)

- **Dagster Designer** (and any similar visual authoring tool) can route users
  to the right surface — "Add schedule" from the Schedules tab, "Add asset"
  from Graph view — instead of a context-free "Add component" firehose over
  900+ entries.
- **Docs** — auto-generate "components that create X" indexes for landing pages.
- **AI assistants** — much better at inferring what to install from natural
  intent ("I want a daily schedule" → `produces` contains `schedule`).
- **`dagster-component search --produces <primitive>`** ships in the CLI today.

## Ask for the Dagster team (separate from this repo, non-blocking)

**Extend `componentTypesForLocationOrError` GraphQL response** to include an
optional `produces` array for Dagster+-managed component types.

Motivating shape:

```graphql
type ComponentTypeMetadata {
  # ...existing fields...
  produces: [String!]      # ["asset", "multi_asset", "asset_check", "job", "schedule", "sensor", "resource", "io_manager", "partitions_def", "other"]
  stateful: Boolean
  consumes: [String!]
}
```

Why this is worth doing on the Dagster side:

- Dagster+'s built-in component picker could adopt the same routing logic
  without duplicating the community manifest.
- Officially-maintained `dagster-<vendor>` packages can populate it once —
  probably via a class-level constant or a `@dg.component_metadata(produces=...)`
  decorator. Downstream tools then don't have to keep a parallel enum.
- Additive. Never breaks existing consumers that ignore the field.

Population strategy: even without a decorator, the framework could derive
`produces` at type-registration time by inspecting the `Definitions` object
the component builds against a stub context (same technique used in this
community registry's `tools/infer_produces.py`, but at import time).

## Contact

If someone on the Dagster team wants to pick this up, `tools/infer_produces.py`
in the community-templates repo has the reference implementation and heuristic
notes.
