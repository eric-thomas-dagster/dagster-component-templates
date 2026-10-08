# Bulk Freshness Policies

Bulk sibling of `freshness_check` (`FreshnessPolicyComponent`) — one config block declares freshness policies for many assets at once, grouped by cadence tier and selected by **tag, group, kind, or the full Dagster selection DSL** — not a hand-listed array of asset keys.

Built for the real case `freshness_check` doesn't scale to: a replication feed with hundreds of landing tables on wildly different arrival cadences. The tiers are the real structure (a handful of cadences — hourly, daily, rare), not the individual tables, so this groups by tier and lets each tier's membership be *derived* from how assets are already tagged/grouped elsewhere in the project, instead of repeating a whole component block — or a hand-maintained asset-key list — per table.

```yaml
type: dagster_component_templates.BulkFreshnessPoliciesComponent
attributes:
  groups:
    - name: hourly_tables
      policy_type: time_window
      fail_window_hours: 2
      selection: "tag:cadence=hourly"
    - name: daily_tables
      policy_type: time_window
      fail_window_hours: 30
      selection: "tag:cadence=daily"
    - name: weekly_tables
      policy_type: cron
      deadline_cron: "0 6 * * 1"
      lower_bound_delta_hours: 24
      selection: "group:sap_rare"
```

Each group becomes one real `build_last_update_freshness_checks` call (the same Dagster-native factory `freshness_check` uses) — Dagster's own API already accepts a list of asset keys per policy, so a tier with 80 tables costs one call, not 80. Onboarding a new table is whatever already tags or groups it correctly elsewhere in the project — no edit to this component's YAML at all.

## `selection` syntax

Same selection language this repo's other bulk components already use (`enhanced_data_quality_checks`, `automation_condition_applicator`), resolved against sibling assets in the same defs folder:

| Form | Example | Notes |
|---|---|---|
| Explicit list | `["sap/bseg", "sap/konv"]` | No discovery needed — exact keys. |
| Everything | `"*"` | All discovered sibling assets. |
| Tag | `"tag:cadence=hourly"` | |
| Group | `"group:sap_landing"` | Hierarchical groups need quotes: `'group:"marketing/*"'`. |
| Kind | `"kind:snowflake"` | |
| Type filter | `"is:external"` / `"is:materializable"` | |
| Boolean composition | `"group:sap_landing and tag:cadence=hourly"` | |
| Bare glob (fallback) | `"sap/*"` | Only tried if the string isn't valid selection syntax. |

Resolution requires sibling assets to actually exist in the same defs folder (or a parent folder reachable via component discovery) — an empty match raises a clear error naming the group and how many sibling assets were found, rather than silently applying no policy.

## Fields

`groups` (required) — list of `{name, policy_type, selection, ...policy fields}`:

- `name` — unique label for the tier (for readability; not itself meaningful to Dagster).
- `policy_type` — `time_window` (rolling-window SLA) or `cron` (deadline-based SLA). Same two modes as `freshness_check`.
- `time_window`: requires `fail_window_hours`.
- `cron`: requires `deadline_cron` + `lower_bound_delta_hours`, optional `timezone`.
- `selection` — see table above. An asset may be matched by exactly one group — validated at load time.

## When to use this vs. `freshness_check`

| | `freshness_check` | `bulk_freshness_policies` |
|---|---|---|
| Scope | One asset per component instance | Many assets, grouped by shared cadence, selected by tag/group/kind |
| Good for | A handful of important, individually-tuned assets | A large replication feed where most tables fall into a few cadence tiers, already tagged/grouped elsewhere |

They produce the same underlying Dagster freshness checks — pick based on whether your assets' expected cadences are genuinely per-asset or fall into a few shared tiers.

## Sister components

- `freshness_check` — single-asset sibling, same policy shape.
- `bigquery_table_freshness_check` — BigQuery-specific single-asset freshness check.
- `enhanced_data_quality_checks` / `automation_condition_applicator` — this repo's other selection-DSL-powered bulk components; same resolution mechanism.
