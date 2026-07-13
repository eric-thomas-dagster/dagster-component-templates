# WandbWorkspaceComponent

Auto-emit one Dagster asset per **Weights & Biases project** for a given entity (user or team). `StateBackedComponent` — discovery cached to disk. Materializing an asset fetches recent runs via the GraphQL API.

## Example

```yaml
type: dagster_community_components.WandbWorkspaceComponent
attributes:
  api_key_env_var: WANDB_API_KEY
  entity_env_var: WANDB_ENTITY
  project_selector:
    by_pattern: ["prod-*"]
  runs_limit: 100
```

## Self-hosted W&B

Override `base_url` to point at your W&B Server: `base_url: https://wandb.acme.internal`.

## Related

- `wandb_resource`
- `wandb_reader` / `wandb_asset` — single-project counterparts
