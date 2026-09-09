# Ask: add `dg api catalog-view` + `dg api custom-metric`

Customer wants to manage Catalog Views and Custom Insights Metrics at
scale via GitOps rather than clicking through the UI. `dg api` covers
alert policies today but not these two.

Same shape as `dg api alert-policy sync` — YAML manifest → upsert.
Backend GraphQL mutations already exist, just need the CLI wrappers.

Working reference scripts (verified end-to-end against a live Dagster+
deployment):

- <https://github.com/eric-thomas-dagster/dagster-component-templates/blob/main/cli/sync_catalog_views.py>
- <https://github.com/eric-thomas-dagster/dagster-component-templates/blob/main/cli/sync_custom_metrics.py>

Lift the mutation names + input shapes directly — everything was
introspected against live prod, not guessed.

— eric.thomas@dagsterlabs.com
