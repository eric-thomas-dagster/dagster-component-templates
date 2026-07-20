# MLflowModelPromotionComponent

Materializable asset that transitions a registered MLflow model version between stages (e.g. **Staging → Production**). Every materialization is a Dagster event with the transition captured in metadata — auditable ML CD.

Right for: making "promote model to Production" a first-class Dagster asset with lineage, retries, on-call alerts, and RBAC — instead of a manual click in the MLflow UI or a hand-rolled script.

## Selection modes

- `source_stage: Staging` — promote the LATEST version at that stage
- `model_version: "7"` — pin a specific version

Mutually exclusive.

## Example (promote latest Staging → Production)

```yaml
type: dagster_community_components.MLflowModelPromotionComponent
attributes:
  asset_name: promote_churn_to_production
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  model_name: churn_model
  source_stage: Staging
  target_stage: Production
  archive_existing_target: true
  group_name: mlflow_cd
```

## Example (pin version to Production)

```yaml
attributes:
  asset_name: force_promote_churn_v7
  model_version: "7"
  target_stage: Production
```

## Idempotency

If the resolved source version is already at `target_stage`, the asset materializes as a no-op with `no_op: true` in metadata — no unnecessary MLflow API call.

## Related

- `mlflow_model_sensor` — trigger downstream on new promotion
- `mlflow_model_version_check` — gate before/after promotion
- `mlflow_model_inference` — score with the promoted model
