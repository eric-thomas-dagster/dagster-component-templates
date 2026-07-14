# SparkK8sOperatorAssetComponent

Submit a **SparkApplication** CRD to the Kubernetes Spark Operator ([`spark-on-k8s-operator`](https://github.com/kubeflow/spark-operator)) and materialize on terminal state.

Right for organizations that wrap PySpark ETLs behind a config-driven framework and submit them as SparkApplication resources — Dagster wraps each submission as one asset with lineage, retry policy, and materialization history.

## Distinct from `pyspark_pipeline`

- **`pyspark_pipeline`** — runs PySpark in-process (in the Dagster op worker). Right for lightweight transforms.
- **`spark_k8s_operator_asset` (this)** — submits a full SparkApplication CRD to your cluster. Right for production ETL with dedicated Spark driver + executor pods.

## Example (typical config-driven "data ingest" wrapper)

```yaml
type: dagster_community_components.SparkK8sOperatorAssetComponent
attributes:
  asset_name: nightly_orders_etl
  group_name: data_ingest
  namespace: spark-jobs
  spark_application_name: orders-etl
  spark_application_body:
    type: Python
    pythonVersion: "3"
    mode: cluster
    image: harbor.acme.internal/data-ingest/orders-etl:v1.2.3
    mainApplicationFile: local:///opt/spark/work-dir/orders_etl.py
    sparkVersion: "3.5.0"
    driver:
      cores: 2
      memory: 4g
      serviceAccount: spark-driver
    executor:
      cores: 2
      instances: 6
      memory: 8g
  labels:
    domain: sales
    team: data-eng
  poll_interval_seconds: 15
  timeout_seconds: 7200
```

## Lifecycle

1. Delete any leftover SparkApplication with the same `metadata.name` (idempotent re-runs).
2. `kubectl apply -f` the composed CRD YAML.
3. Poll `kubectl get sparkapplication -o json` every `poll_interval_seconds`.
4. On `applicationState.state == COMPLETED`: record driver pod + executor count + web UI URL as asset metadata, optionally delete the CRD.
5. On `FAILED` / `SUBMISSION_FAILED`: raise with the error message from the CRD status.
6. On timeout: raise with the last observed state.

## Requirements

- `kubectl` on PATH
- Access to the k8s cluster (`~/.kube/config` OR in-cluster service account if the Dagster agent runs inside the cluster)
- The Spark Operator installed in the cluster ([Helm chart](https://github.com/kubeflow/spark-operator/tree/master/charts/spark-operator))
- A service account with `sparkapplications` RBAC in the target namespace

## Related

- `pyspark_pipeline` — in-process PySpark alternative
- `k8s_job_asset` — generic k8s Job (not a SparkApplication)
- `docker_container_asset` — for non-Spark containerized jobs
