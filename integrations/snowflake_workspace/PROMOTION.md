# `snowflake_workspace` — internal promotion brief

*One-pager for circulating to Dagster engineering + product before opening
the promotion PR into `dagster-snowflake`.*

**Author:** Eric Thomas
**Component:** `SnowflakeWorkspaceComponent` (community, v2.0.0)
**Target home:** `dagster_snowflake` (official)
**Status:** Alignment complete on `main` in [`dagster-community-components-cli`](https://github.com/eric-thomas-dagster/dagster-component-templates). Ready to open PR.

---

## The pitch

Two active POCs (Loves + Novatus, both this week) asked variations of the
same question: *"where does Dagster fit when Snowflake already owns tasks,
streams, dynamic tables, pipes, alerts?"*

Answer: **Snowflake stays external, Dagster orchestrates on top.** One YAML
declaration turns every Snowflake primitive into a Dagster asset with real
lineage, event-driven automation, and cross-tool orchestration. Materialize
→ `EXECUTE TASK` / `REFRESH PIPE` / `ALTER DT REFRESH` runs server-side.

`snowflake_workspace` is the component that does this. It's the most-asked
and most-mature workspace component in the community registry — the same
questions from Loves + Novatus have been coming in from prospects for
months. Promoting it into `dagster-snowflake` is the right long-term home.

## Coverage today

**11+ Snowflake object types** enumerable under one YAML:

- Tasks
- Stored procedures
- Dynamic tables (external + asset modes)
- Streams
- Snowpipes
- Stages
- Materialized views
- External tables
- Alerts
- OpenFlow flows
- Tables + views (with an explicit "NOT recommended" callout since they
  lack a native trigger surface — see the field docstrings)

Every object gets:
- An `AssetSpec` in the Dagster catalog with `kinds={"snowflake", <object_kind>}`
- Metadata tags for FQN + database + schema
- Server-side materialization on click (`EXECUTE TASK`, `ALTER … REFRESH`, etc.)
- Runtime query-perf metadata (duration, credits, rows produced, spill bytes)

Optional built-in polling sensor observes Snowflake-native events
(TASK_HISTORY, DYNAMIC_TABLE_REFRESH_HISTORY, PIPE_STATUS) so
Snowflake-scheduled runs surface as materialization events without any
Dagster-side scheduler involvement.

## API convention parity

The 3-commit refactor sweep landed on `main` this week to match the shape
used by `dagster-databricks` / `dagster-fivetran` / `dagster-powerbi`:

| Requirement | Reference (official) | `snowflake_workspace` |
|---|---|---|
| `@public` on component class | Fivetran + PowerBI | ✓ |
| Top-level `workspace: <VendorResource>` field | All three | ✓ |
| `Annotated[..., Resolver(...)]` + `resolve_fields()` shape | Fivetran | ✓ |
| `translation: TranslationFn[<Props>]` field | Fivetran + PowerBI | ✓ (12 object kinds, 14 wiring sites) |
| `@public get_asset_spec(props)` override hook | Fivetran + PowerBI | ✓ |
| `<Vendor>ComponentTranslator` companion class | Fivetran + PowerBI | ✓ |
| `polling_sensor: bool` convention | Fivetran | ✓ (with `generate_sensor` alias for BC) |
| `defs_state: ResolvedDefsStateConfig` field | Fivetran + PowerBI | ✓ |
| `defs_state_config` property returning `DefsStateConfig.from_args(...)` | Fivetran + PowerBI | ✓ |
| `StateBackedComponent` inheritance | Fivetran + PowerBI + Databricks | ✓ |
| `async write_state_to_path` + `def build_defs_from_state` | Fivetran + PowerBI + Databricks | ✓ |

The commits landing this parity, in order:

1. [`dbcb70bb`](https://github.com/eric-thomas-dagster/dagster-component-templates/commit/dbcb70bb) — step 1 mechanical alignment (`@public`, resolver wrap, translation field declared, translator classes, polling_sensor rename, defs_state)
2. [`c033acf2`](https://github.com/eric-thomas-dagster/dagster-component-templates/commit/c033acf2) — step 2 `StateBackedComponent` split (`build_defs` refactored into `write_state_to_path` + `build_defs_from_state`)
3. [`8206c995`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/commit/8206c995) — step 3 translator wired at every asset-emission site (14 sites, 12 object kinds)

## What already exists in `dagster-snowflake`

`snowflake_workspace` sits **on top of** `dagster_snowflake.SnowflakeResource`
today — that's how it inherits every auth mode the official resource supports
(password / keypair / SSO / OAuth / JWT / MFA). The `workspace:` block IS a
`SnowflakeResource`. When Snowflake adds a new auth mechanism upstream, the
workspace lights it up without a component change.

That means the promotion PR is **additive** — no changes needed to the
existing `SnowflakeResource` or any of the existing `dagster_snowflake`
Pipes / IO manager surface. The component slots in next to what's already
there.

## Backward compatibility

- `generate_sensor: bool` (legacy field name) is preserved as an alias on the
  renamed `polling_sensor: bool` field via Pydantic's `alias=`. Existing
  `defs.yaml` files continue to resolve without edits.
- `assets_by_name` per-asset overrides (used by every real customer today) are
  fully preserved — the merger runs before `_apply_translation` at every
  asset-emission site.
- `build_defs` → `write_state_to_path` + `build_defs_from_state` is invisible
  to customers since it's a `StateBackedComponent` internal restructure —
  materialization behavior is byte-identical.

Anyone with a working `defs.yaml` in production today loses nothing on the
promotion cutover.

## What the promotion PR needs to answer

The alignment is done. What we need direction on before opening the PR:

1. **Package path.** `dagster_snowflake.components.workspace_component.component`? Some other convention? The Fivetran / PowerBI packages both use `<vendor>/components/workspace_component/`.

2. **Naming.** Keep `SnowflakeWorkspaceComponent` or standardize on
   Fivetran's `<Vendor>AccountComponent` shape? Snowflake accounts don't
   have workspaces in Snowflake terminology — the current name is a
   Dagster convention, not a Snowflake one.

3. **`@scaffold_with(SnowflakeAccountComponentScaffolder)`.** Fivetran ships
   a scaffolder that emits a starter `defs.yaml`. PowerBI + Databricks
   don't. Should the snowflake promotion PR include one?

4. **`assets_by_name` vs. `translation` for per-asset customization.**
   Currently both work in parallel — `assets_by_name` for warehouse
   overrides / query tags / deps wiring / freshness policies, and
   `translation` for renaming / retagging / lineage-graph shape. Should
   these consolidate before shipping? Or is the dual-mechanism the right
   default (translation for shape, assets_by_name for per-object semantic
   overrides)?

5. **Docs.** The current field docstrings are heavy (11+ object types,
   several with 2-3 modeling modes). Reviewers may want to strip inline
   docstrings and move the long-form guidance into `dagster_snowflake`'s
   Sphinx doc site. Willing to do either.

6. **Companion single-object components.** The community registry also
   ships `snowflake_task` / `snowflake_dynamic_table` / `snowflake_stream` /
   `snowflake_snowpipe` / `snowflake_iceberg_table` / etc. — one-per-object
   variants for customers who want a subset. Do those come along in the
   promotion PR too, or stay in community?

## Suggested next steps

1. Circulate this doc + the 3 alignment commits internally. Feedback within
   3 business days.
2. Once path + naming + companion-component scope are settled, open the PR
   against `dagster/dagster`.
3. I'll write the PR body (with the parity table + BC notes above), a
   migration guide for existing users (basically: "no action required"),
   and a "how to promote future community workspace components" runbook
   using this PR as the template.

## Reference materials

- **The component:** [`integrations/snowflake_workspace/component.py`](https://github.com/eric-thomas-dagster/dagster-component-templates/blob/main/integrations/snowflake_workspace/component.py)
- **The 3 alignment commits:** `dbcb70bb`, `c033acf2`, `8206c995` (all on `main`)
- **The customer-facing tour:** [Blog: One YAML, every Snowflake object](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/blog/snowflake-workspace-external-orchestration.md)
- **The end-to-end walkthrough:** [`examples/snowflake_workspace.md`](https://dagster-component-ui.vercel.app/examples/snowflake_workspace) — deployable to Dagster+ Serverless as-is
- **The reviewer taxonomy:** the shape parity table above is derived from a cross-check across the three official components (`dagster_fivetran.components.workspace_component`, `dagster_powerbi.components.power_bi_workspace`, `dagster_databricks.components.workspace_component`).
