"""DataprocResource.

Google Cloud Dataproc cluster lifecycle + job submission, as a reusable
resource -- the operator-equivalent of Airflow's Dataproc provider
(`DataprocCreateClusterOperator`/`DataprocDeleteClusterOperator`/
`DataprocSubmitJobOperator`), callable from any op or asset rather than
baked into a single dedicated asset component.

Request/response shapes verified directly against the installed
`google-cloud-dataproc` v5 client (`google.cloud.dataproc_v1`), not from
memory: `ClusterControllerClient.create_cluster`/`delete_cluster` take
`project_id`/`region`/`cluster` (a `Cluster(project_id, cluster_name,
config=ClusterConfig(...))`) and return a long-running `Operation`;
`get_cluster` raises `google.api_core.exceptions.NotFound` when the cluster
doesn't exist. `JobControllerClient.submit_job` takes `project_id`/`region`/
`job` and returns the accepted `Job` synchronously (not yet complete -- poll
`get_job` until `job.status.state` is terminal). The regional client
requires `client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}`.
"""
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field

_TERMINAL_SUCCESS = {"DONE"}
_TERMINAL_FAILURE = {"ERROR", "CANCELLED"}


def _build_cluster_config(
    master_machine_type="n2-standard-4",
    worker_machine_type="n2-standard-4",
    num_workers=2,
    master_boot_disk_size_gb=None,
    worker_boot_disk_size_gb=None,
    zone=None,
    service_account=None,
    network_tags=None,
    image_version=None,
    software_properties=None,
    idle_delete_ttl_seconds=None,
) -> Dict[str, Any]:
    from google.protobuf import duration_pb2

    gce_cluster_config: Dict[str, Any] = {}
    if zone:
        gce_cluster_config["zone_uri"] = zone
    if service_account:
        gce_cluster_config["service_account"] = service_account
    if network_tags:
        gce_cluster_config["tags"] = list(network_tags)

    master_disk: Dict[str, Any] = {}
    if master_boot_disk_size_gb:
        master_disk["boot_disk_size_gb"] = master_boot_disk_size_gb
    worker_disk: Dict[str, Any] = {}
    if worker_boot_disk_size_gb:
        worker_disk["boot_disk_size_gb"] = worker_boot_disk_size_gb

    config: Dict[str, Any] = {
        "gce_cluster_config": gce_cluster_config or None,
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": master_machine_type,
            "disk_config": master_disk or None,
        },
        "worker_config": {
            "num_instances": num_workers,
            "machine_type_uri": worker_machine_type,
            "disk_config": worker_disk or None,
        },
    }
    if image_version or software_properties:
        config["software_config"] = {"image_version": image_version, "properties": software_properties or {}}
    if idle_delete_ttl_seconds:
        config["lifecycle_config"] = {"idle_delete_ttl": duration_pb2.Duration(seconds=idle_delete_ttl_seconds)}
    return config


def _build_job_body(cluster_name: str, job_type: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
    job: Dict[str, Any] = {"placement": {"cluster_name": cluster_name}}
    labels = job_config.get("labels")
    if labels:
        job["labels"] = dict(labels)

    if job_type == "pyspark":
        main_python_file_uri = job_config.get("main_python_file_uri")
        if not main_python_file_uri:
            raise ValueError("job_type='pyspark' requires main_python_file_uri.")
        job["pyspark_job"] = {
            "main_python_file_uri": main_python_file_uri,
            "args": job_config.get("args") or [],
            "python_file_uris": job_config.get("python_file_uris") or [],
            "jar_file_uris": job_config.get("jar_file_uris") or [],
            "file_uris": job_config.get("file_uris") or [],
            "archive_uris": job_config.get("archive_uris") or [],
            "properties": job_config.get("properties") or {},
        }
    elif job_type == "spark":
        main_jar_file_uri = job_config.get("main_jar_file_uri")
        main_class = job_config.get("main_class")
        if not main_jar_file_uri and not main_class:
            raise ValueError("job_type='spark' requires main_jar_file_uri or main_class.")
        spark_job: Dict[str, Any] = {
            "args": job_config.get("args") or [],
            "jar_file_uris": job_config.get("jar_file_uris") or [],
            "file_uris": job_config.get("file_uris") or [],
            "archive_uris": job_config.get("archive_uris") or [],
            "properties": job_config.get("properties") or {},
        }
        if main_jar_file_uri:
            spark_job["main_jar_file_uri"] = main_jar_file_uri
        if main_class:
            spark_job["main_class"] = main_class
        job["spark_job"] = spark_job
    elif job_type in ("hive", "pig"):
        query_file_uri = job_config.get("query_file_uri")
        query_list = job_config.get("query_list")
        if not query_file_uri and not query_list:
            raise ValueError(f"job_type={job_type!r} requires query_file_uri or query_list.")
        type_job: Dict[str, Any] = {
            "continue_on_failure": job_config.get("continue_on_failure", False),
            "script_variables": job_config.get("script_variables") or {},
            "properties": job_config.get("properties") or {},
            "jar_file_uris": job_config.get("jar_file_uris") or [],
        }
        if query_file_uri:
            type_job["query_file_uri"] = query_file_uri
        else:
            type_job["query_list"] = {"queries": list(query_list)}
        job[f"{job_type}_job"] = type_job
    else:
        raise ValueError(f"job_type must be 'pyspark', 'spark', 'hive', or 'pig', got {job_type!r}")
    return job


class DataprocResource(dg.ConfigurableResource):
    """Ensure a Dataproc cluster is present/absent, and submit+wait a job on one.

    No built-in dagster_gcp Dataproc resource offers this idempotent-lifecycle
    / submit-and-poll behavior, so this resource implements it directly
    against `google.cloud.dataproc_v1`, the same way `resources/http_resource`
    hand-writes its own `HTTPResource` methods rather than only wrapping an
    official client.
    """

    gcp_credentials_env_var: Optional[str] = None

    def _credentials(self):
        if not self.gcp_credentials_env_var:
            return None
        import json
        import os

        from google.oauth2 import service_account

        raw = os.environ.get(self.gcp_credentials_env_var)
        if not raw:
            raise ValueError(f"Env var {self.gcp_credentials_env_var!r} (gcp_credentials_env_var) is not set.")
        return service_account.Credentials.from_service_account_info(json.loads(raw))

    def _cluster_client(self, region: str):
        from google.cloud import dataproc_v1

        kwargs: Dict[str, Any] = {"client_options": {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}}
        creds = self._credentials()
        if creds is not None:
            kwargs["credentials"] = creds
        return dataproc_v1.ClusterControllerClient(**kwargs)

    def _job_client(self, region: str):
        from google.cloud import dataproc_v1

        kwargs: Dict[str, Any] = {"client_options": {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}}
        creds = self._credentials()
        if creds is not None:
            kwargs["credentials"] = creds
        return dataproc_v1.JobControllerClient(**kwargs)

    def _batch_client(self, region: str):
        from google.cloud import dataproc_v1

        kwargs: Dict[str, Any] = {"client_options": {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}}
        creds = self._credentials()
        if creds is not None:
            kwargs["credentials"] = creds
        return dataproc_v1.BatchControllerClient(**kwargs)

    def get_cluster(self, project_id: str, region: str, cluster_name: str):
        """Returns the Cluster proto, or None if it doesn't exist."""
        from google.api_core.exceptions import NotFound

        client = self._cluster_client(region)
        try:
            return client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
        except NotFound:
            return None

    def ensure_cluster(
        self,
        project_id: str,
        region: str,
        cluster_name: str,
        cluster_config: Optional[Dict[str, Any]] = None,
        timeout_seconds: int = 1200,
        log=None,
    ) -> Dict[str, Any]:
        """Idempotent: creates the cluster if missing, no-ops if already present.

        `cluster_config` accepts the same keys as `_build_cluster_config`'s
        kwargs (master_machine_type, worker_machine_type, num_workers,
        master_boot_disk_size_gb, worker_boot_disk_size_gb, zone,
        service_account, network_tags, image_version, software_properties,
        idle_delete_ttl_seconds).

        Returns {"action": "created"|"none", "cluster_status": <state name or None>}.
        """
        client = self._cluster_client(region)
        existing = self.get_cluster(project_id, region, cluster_name)
        if existing is not None:
            if log:
                log.info(f"dataproc_resource: cluster {cluster_name!r} already present.")
            return {"action": "none", "cluster_status": existing.status.state.name}

        cluster = {
            "project_id": project_id,
            "cluster_name": cluster_name,
            "config": _build_cluster_config(**(cluster_config or {})),
        }
        if log:
            log.info(f"dataproc_resource: creating cluster {cluster_name!r}, waiting up to {timeout_seconds}s.")
        op = client.create_cluster(project_id=project_id, region=region, cluster=cluster)
        result = op.result(timeout=timeout_seconds)
        return {"action": "created", "cluster_status": result.status.state.name}

    def delete_cluster(self, project_id: str, region: str, cluster_name: str, timeout_seconds: int = 1200, log=None) -> Dict[str, Any]:
        """Idempotent: deletes the cluster if present, no-ops if already absent."""
        client = self._cluster_client(region)
        existing = self.get_cluster(project_id, region, cluster_name)
        if existing is None:
            if log:
                log.info(f"dataproc_resource: cluster {cluster_name!r} already absent.")
            return {"action": "none"}
        if log:
            log.info(f"dataproc_resource: deleting cluster {cluster_name!r}, waiting up to {timeout_seconds}s.")
        op = client.delete_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
        op.result(timeout=timeout_seconds)
        return {"action": "deleted"}

    def submit_job_and_wait(
        self,
        project_id: str,
        region: str,
        cluster_name: str,
        job_type: str,
        job_config: Dict[str, Any],
        poll_interval_seconds: float = 10.0,
        timeout_seconds: int = 3600,
        log=None,
    ) -> Dict[str, Any]:
        """Submits a pyspark/spark/hive/pig job to `cluster_name` and polls to a terminal state.

        `job_config` accepts the same keys `_build_job_body` understands per
        job_type (main_python_file_uri/args/... for pyspark, main_jar_file_uri/
        main_class/... for spark, query_file_uri/query_list/... for hive/pig,
        plus shared jar_file_uris/file_uris/archive_uris/properties/labels).

        Returns {"job_id", "job_status", "driver_output_uri"} on success;
        raises RuntimeError on ERROR/CANCELLED, TimeoutError if it never
        reaches a terminal state within timeout_seconds.
        """
        import time

        job_body = _build_job_body(cluster_name, job_type, job_config)
        client = self._job_client(region)

        submitted = client.submit_job(project_id=project_id, region=region, job=job_body)
        job_id = submitted.reference.job_id
        if log:
            log.info(f"dataproc_resource: submitted {job_type} job {job_id!r} to cluster {cluster_name!r}.")

        deadline = time.monotonic() + timeout_seconds
        job = submitted
        while job.status.state.name not in (_TERMINAL_SUCCESS | _TERMINAL_FAILURE):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Dataproc job {job_id!r} did not reach a terminal state within {timeout_seconds}s "
                    f"(last status={job.status.state.name!r})."
                )
            time.sleep(poll_interval_seconds)
            job = client.get_job(project_id=project_id, region=region, job_id=job_id)
            if log:
                log.info(f"dataproc_resource: job {job_id!r} status={job.status.state.name!r}")

        state = job.status.state.name
        if state in _TERMINAL_FAILURE:
            details = job.status.details or "(no details)"
            raise RuntimeError(f"Dataproc job {job_id!r} finished with status={state!r}: {details}")

        result: Dict[str, Any] = {"job_id": job_id, "job_status": state}
        if job.driver_output_resource_uri:
            result["driver_output_uri"] = job.driver_output_resource_uri
        return result

    def create_batch_and_wait(
        self,
        project_id: str,
        region: str,
        batch_id: str,
        batch_config: Dict[str, Any],
        timeout_seconds: int = 3600,
        log=None,
    ) -> Dict[str, Any]:
        """Creates a Dataproc Serverless batch workload (`BatchControllerClient.create_batch`)
        and waits for it to finish -- the `DataprocCreateBatchOperator` equivalent.

        Dataproc Serverless batches are a distinct resource type from the
        cluster-based jobs `submit_job_and_wait` targets above -- a batch runs
        on ephemeral, Google-managed compute with no cluster to create or
        manage first.

        `batch_config` is the real `google.cloud.dataproc_v1.Batch` message
        shape, passed straight through -- e.g.
        `{"pyspark_batch": {"main_python_file_uri": "gs://bucket/job.py",
        "args": [...]}, "runtime_config": {"version": "2.1"}}`. Exactly one of
        `pyspark_batch`/`spark_batch`/`spark_r_batch`/`spark_sql_batch` must be
        set, matching the API's own oneof requirement.

        Returns {"batch_id", "state"} once terminal (SUCCEEDED/CANCELLED/
        FAILED/STATE_UNSPECIFIED). Raises RuntimeError on FAILED/CANCELLED.
        """
        from google.cloud import dataproc_v1

        client = self._batch_client(region)
        batch = dataproc_v1.Batch(**batch_config)
        if log:
            log.info(f"dataproc_resource: creating batch {batch_id!r}, waiting up to {timeout_seconds}s.")
        op = client.create_batch(
            parent=f"projects/{project_id}/regions/{region}",
            batch=batch,
            batch_id=batch_id,
        )
        result_batch = op.result(timeout=timeout_seconds)
        state = result_batch.state.name

        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Dataproc batch {batch_id!r} finished with state={state!r}: {result_batch.state_message!r}")
        return {"batch_id": batch_id, "state": state}


class DataprocResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DataprocResource (cluster lifecycle + job submission) for use by other components.

    A Dataproc cluster/job isn't data with lineage -- it's a reusable,
    parameterized action (the Airflow Dataproc *Operator* equivalent), so
    this is a resource injectable into any user-written op/asset, not a
    dedicated asset component.
    """

    resource_key: str = Field(default="dataproc_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Env var containing a GCP service account JSON string. Omit to use Application Default Credentials.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dataproc Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DataprocResource(gcp_credentials_env_var=self.gcp_credentials_env_var)
        return dg.Definitions(resources={self.resource_key: resource})
