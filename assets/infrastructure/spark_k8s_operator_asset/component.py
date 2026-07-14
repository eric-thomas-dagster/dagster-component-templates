"""Spark on Kubernetes Operator Asset Component.

Submit a SparkApplication CRD to the Kubernetes Spark Operator
(spark-on-k8s-operator by kubeflow) and poll to terminal state
(COMPLETED / FAILED). Right for organizations that wrap PySpark ETLs
behind a config-driven framework and submit them as SparkApplication
resources — Dagster wraps each submission as an asset with lineage,
retry policy, and materialization history.

Distinct from `pyspark_pipeline` which runs Spark in-process. This
component talks to k8s, applies the CRD, and monitors the status.

Requires: kubectl on PATH OR kubernetes Python client (auto-uses
in-cluster config if running inside the cluster, otherwise ~/.kube/config).
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class SparkK8sOperatorAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a SparkApplication to the Spark Operator and materialize on completion.

    Common pattern — a config-driven Spark ETL framework generates
    SparkApplication YAML per job; Dagster wraps each submission as one
    asset with retry + lineage. Set `spark_application_body` to your
    templated CRD spec (below `spec:`); the component prepends the
    metadata/apiVersion/kind wrapper.

    Example:

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
            imagePullPolicy: Always
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
          poll_interval_seconds: 15
          timeout_seconds: 7200
        ```
    """

    asset_name: str = Field(description="Dagster asset name.")
    group_name: str = Field(description="Dagster asset group.")

    namespace: str = Field(description="k8s namespace where the SparkApplication is submitted.")
    spark_application_name: str = Field(description="metadata.name for the SparkApplication CRD.")
    spark_application_body: Dict[str, Any] = Field(
        description="The `spec:` block of the SparkApplication CRD. Component prepends apiVersion/kind/metadata.",
    )
    api_version: str = Field(default="sparkoperator.k8s.io/v1beta2", description="CRD apiVersion.")

    poll_interval_seconds: int = Field(default=15, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=7200, description="Deadline for the SparkApplication to reach terminal state.")

    labels: Optional[Dict[str, str]] = Field(default=None, description="Optional metadata.labels for tagging + team lookup.")
    annotations: Optional[Dict[str, str]] = Field(default=None, description="Optional metadata.annotations.")

    kube_context: Optional[str] = Field(default=None, description="Optional kubectl context name (uses current-context if unset).")
    delete_on_success: bool = Field(
        default=True,
        description="Delete the SparkApplication CRD after successful completion (keeps CRD count from ballooning).",
    )
    deps: Optional[List[str]] = Field(default=None, description="Optional upstream asset dependencies.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        upstream_deps = [dg.AssetDep(dg.AssetKey(k)) for k in (self.deps or [])]

        @dg.asset(
            name=_self.asset_name,
            group_name=_self.group_name,
            compute_kind="spark_on_k8s",
            deps=upstream_deps,
        )
        def _the_asset(context: dg.AssetExecutionContext):
            import json
            import subprocess
            import time
            import tempfile

            # Build the full SparkApplication CRD.
            spark_app = {
                "apiVersion": _self.api_version,
                "kind": "SparkApplication",
                "metadata": {
                    "name": _self.spark_application_name,
                    "namespace": _self.namespace,
                    "labels": _self.labels or {},
                    "annotations": _self.annotations or {},
                },
                "spec": _self.spark_application_body,
            }

            # Write to a tempfile so kubectl apply -f can read it.
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
                # PyYAML is optional — fall back to JSON, which kubectl also accepts.
                try:
                    import yaml
                    yaml.safe_dump(spark_app, f, sort_keys=False)
                except ImportError:
                    json.dump(spark_app, f)
                spec_path = f.name

            kubectl_ctx = ["--context", _self.kube_context] if _self.kube_context else []

            # Delete any leftover SparkApp with the same name (idempotent).
            subprocess.run(
                ["kubectl", *kubectl_ctx, "-n", _self.namespace, "delete", "sparkapplication",
                 _self.spark_application_name, "--ignore-not-found=true", "--wait=true"],
                capture_output=True, timeout=60,
            )

            # Apply the fresh spec.
            apply = subprocess.run(
                ["kubectl", *kubectl_ctx, "apply", "-f", spec_path],
                capture_output=True, text=True, timeout=60,
            )
            if apply.returncode != 0:
                raise Exception(f"kubectl apply failed: {apply.stderr[:400]}")
            context.log.info(f"Submitted SparkApplication {_self.namespace}/{_self.spark_application_name}")

            # Poll status.
            deadline = time.time() + _self.timeout_seconds
            terminal = {"COMPLETED", "FAILED", "SUBMISSION_FAILED", "UNKNOWN"}
            last_state = None
            while time.time() < deadline:
                time.sleep(_self.poll_interval_seconds)
                q = subprocess.run(
                    ["kubectl", *kubectl_ctx, "-n", _self.namespace, "get", "sparkapplication",
                     _self.spark_application_name, "-o", "json"],
                    capture_output=True, text=True, timeout=30,
                )
                if q.returncode != 0:
                    continue
                try:
                    body = json.loads(q.stdout)
                except Exception:  # noqa: BLE001
                    continue
                state = ((body.get("status") or {}).get("applicationState") or {}).get("state", "")
                if state and state != last_state:
                    context.log.info(f"SparkApplication state: {state}")
                    last_state = state
                if state in terminal:
                    if state == "COMPLETED":
                        # Capture executor + driver pod info as metadata.
                        driver_info = (body.get("status") or {}).get("driverInfo") or {}
                        executor_state = (body.get("status") or {}).get("executorState") or {}
                        context.add_output_metadata({
                            "final_state": state,
                            "driver_pod": dg.MetadataValue.text(str(driver_info.get("podName", ""))),
                            "executor_count": dg.MetadataValue.int(len(executor_state)),
                            "start_time": dg.MetadataValue.text(str((body.get("status") or {}).get("submissionTime", ""))),
                            "completion_time": dg.MetadataValue.text(str((body.get("status") or {}).get("terminationTime", ""))),
                            "web_ui_ingress": dg.MetadataValue.text(str(driver_info.get("webUIIngressAddress", ""))),
                        })
                        if _self.delete_on_success:
                            subprocess.run(
                                ["kubectl", *kubectl_ctx, "-n", _self.namespace, "delete",
                                 "sparkapplication", _self.spark_application_name, "--wait=false"],
                                capture_output=True, timeout=30,
                            )
                        return
                    else:
                        err_msg = ((body.get("status") or {}).get("applicationState") or {}).get("errorMessage") or state
                        raise Exception(f"SparkApplication ended in {state}: {err_msg[:400]}")

            raise Exception(
                f"SparkApplication did not reach terminal state within {_self.timeout_seconds}s "
                f"(last state={last_state}, namespace={_self.namespace}, name={_self.spark_application_name})"
            )

        return dg.Definitions(assets=[_the_asset])
