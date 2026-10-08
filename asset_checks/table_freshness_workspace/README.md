# Table Freshness Workspace

Bulk sibling of `freshness_check` (`FreshnessPolicyComponent`) — one config block declares freshness policies for many assets at once, grouped by cadence tier, instead of one component instance per asset.

Built for the real case `freshness_check` doesn't scale to: a replication feed with hundreds of landing tables on wildly different arrival cadences. The tiers are the real structure (a handful of cadences — hourly, daily, rare), not the individual tables, so this groups by tier instead of repeating a whole component block per table.

```yaml
type: dagster_component_templates.TableFreshnessWorkspaceComponent
attributes:
  groups:
    - name: hourly_tables
      policy_type: time_window
      fail_window_hours: 2
      asset_keys: [sap_ecc/bseg, sap_ecc/konv, sap_ecc/vbap]
    - name: daily_tables
      policy_type: time_window
      fail_window_hours: 30
      asset_keys: [sap_ecc/material_master, sap_ecc/vendor_master]
    - name: weekly_tables
      policy_type: cron
      deadline_cron: "0 6 * * 1"
      lower_bound_delta_hours: 24
      asset_keys: [sap_ecc/company_codes]
```

Each group becomes one real `build_last_update_freshness_checks` call (the same Dagster-native factory `freshness_check` uses) — Dagster's own API already accepts a list of asset keys per policy, so a tier with 80 tables costs one call, not 80. Onboarding a new table is a one-line addition to the right tier's `asset_keys:` — still plain git-tracked config, no code, no per-table hand-authored schedule.

## Fields

`groups` (required) — list of `{name, policy_type, asset_keys, ...policy fields}`:

- `name` — unique label for the tier (for readability; not itself meaningful to Dagster).
- `policy_type` — `time_window` (rolling-window SLA) or `cron` (deadline-based SLA). Same two modes as `freshness_check`.
- `time_window`: requires `fail_window_hours`.
- `cron`: requires `deadline_cron` + `lower_bound_delta_hours`, optional `timezone`.
- `asset_keys` — list of `"a/b/c"`-style asset key strings. An asset key may appear in exactly one group — validated at load time.

## When to use this vs. `freshness_check`

| | `freshness_check` | `table_freshness_workspace` |
|---|---|---|
| Scope | One asset per component instance | Many assets, grouped by shared cadence |
| Good for | A handful of important, individually-tuned assets | A large replication feed where most tables fall into a few cadence tiers |

They produce the same underlying Dagster freshness checks — pick based on whether your assets' expected cadences are genuinely per-asset or fall into a few shared tiers.

## Sister components

- `freshness_check` — single-asset sibling, same policy shape.
- `bigquery_table_freshness_check` — BigQuery-specific single-asset freshness check.
