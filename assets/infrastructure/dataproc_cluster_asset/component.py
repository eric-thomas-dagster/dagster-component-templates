"""DataprocClusterAssetComponent.

Declaratively ensures a Google Cloud Dataproc cluster is present or absent,
as a Dagster asset -- the `DataprocCreateClusterOperator`/
`DataprocDeleteClusterOperator` equivalent from Airflow's Dataproc provider.

Request/response shapes verified directly against the installed
`google-cloud-dataproc` v5 client (`google.cloud.dataproc_v1`), not from
memory: `ClusterControllerClient.create_cluster`/`delete_cluster` take
`project_id`/`region`/`cluster` (a `Cluster(project_id, cluster_name,
config=ClusterConfig(...))`) and return a long-running `Operation`;
`get_cluster` raises `google.api_core.exceptions.NotFound` when the cluster
doesn't exist. `ClusterConfig`'s real field names are `gce_cluster_config`
(zone_uri/service_account/tags), `master_config`/`worker_config`
(`InstanceGroupConfig`: num_instances/machine_type_uri/disk_config),
`software_config` (image_version/properties), `lifecycle_config`
(idle_delete_ttl, a `Duration`). The regional client requires
`client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}`.

A Dataproc cluster is long-lived infrastructure, not a batch job --
materializing this asset means "ensure the cluster is present/absent",
mirroring this repo's `managed_flink_application_asset` convention for
long-running infra rather than a completing job.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _make_cluster_client(region: str, gcp_credentials_env_var: Optional[str]):
    from google.cloud import dataproc_v1

    client_kwargs: Dict[str, Any] = {"client_options": {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}}
    if gcp_credentials_env_var:
        import json
        import os

        from google.oauth2 import service_account

        raw = os.environ.get(gcp_credentials_env_var)
        if not raw:
            raise ValueError(f"Env var {gcp_credentials_env_var!r} (gcp_credentials_env_var) is not set.")
        client_kwargs["credentials"] = service_account.Credentials.from_service_account_info(json.loads(raw))
    return dataproc_v1.ClusterControllerClient(**client_kwargs)


def _get_cluster(client, project_id: str, region: str, cluster_name: str):
    from google.api_core.exceptions import NotFound

    try:
        return client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
    except NotFound:
        return None


def _build_cluster_config(self) -> Dict[str, Any]:
    from google.protobuf import duration_pb2

    gce_cluster_config: Dict[str, Any] = {}
    if self.zone:
        gce_cluster_config["zone_uri"] = self.zone
    if self.service_account:
        gce_cluster_config["service_account"] = self.service_account
    if self.network_tags:
        gce_cluster_config["tags"] = list(self.network_tags)

    master_disk: Dict[str, Any] = {}
    if self.master_boot_disk_size_gb:
        master_disk["boot_disk_size_gb"] = self.master_boot_disk_size_gb
    worker_disk: Dict[str, Any] = {}
    if self.worker_boot_disk_size_gb:
        worker_disk["boot_disk_size_gb"] = self.worker_boot_disk_size_gb

    config: Dict[str, Any] = {
        "gce_cluster_config": gce_cluster_config or None,
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": self.master_machine_type,
            "disk_config": master_disk or None,
        },
        "worker_config": {
            "num_instances": self.num_workers,
            "machine_type_uri": self.worker_machine_type,
            "disk_config": worker_disk or None,
        },
    }
    if self.image_version or self.software_properties:
        config["software_config"] = {
            "image_version": self.image_version,
            "properties": self.software_properties or {},
        }
    if self.idle_delete_ttl_seconds:
        config["lifecycle_config"] = {"idle_delete_ttl": duration_pb2.Duration(seconds=self.idle_delete_ttl_seconds)}
    return config


class DataprocClusterAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ensure a Google Cloud Dataproc cluster is present or absent.

    A Dataproc cluster is long-lived infrastructure, not a batch job --
    materializing this asset means "ensure the cluster is in `desired_state`",
    mirroring `managed_flink_application_asset`'s long-running-infra model.
    """

    asset_name: str = Field(description="Dagster asset key.")
    project_id: str = Field(description="GCP project ID.")
    region: str = Field(description="Dataproc region, e.g. 'us-central1'.")
    cluster_name: str = Field(description="The Dataproc cluster name.")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Env var containing a GCP service account JSON string. Omit to use Application Default Credentials.")

    desired_state: str = Field(default="present", description="'present' or 'absent'. Idempotent: a no-op if the cluster is already in this state.")

    master_machine_type: str = Field(default="n2-standard-4", description="Machine type for the single master node.")
    worker_machine_type: str = Field(default="n2-standard-4", description="Machine type for worker nodes.")
    num_workers: int = Field(default=2, ge=2, description="Number of primary worker nodes (Dataproc requires at least 2 for a standard cluster).")
    master_boot_disk_size_gb: Optional[int] = Field(default=None, description="Master node boot disk size in GB. Omit to use the Dataproc default.")
    worker_boot_disk_size_gb: Optional[int] = Field(default=None, description="Worker node boot disk size in GB. Omit to use the Dataproc default.")
    zone: Optional[str] = Field(default=None, description="GCE zone URI or short name, e.g. 'us-central1-a'. Omit to let Dataproc auto-select a zone within the region.")
    service_account: Optional[str] = Field(default=None, description="GCE service account email for cluster VMs. Omit to use the project's default Compute Engine service account.")
    network_tags: Optional[Union[str, List[str]]] = Field(default=None, description="GCE network tags applied to cluster VMs (for firewall rules).")
    image_version: Optional[str] = Field(default=None, description="Dataproc image version, e.g. '2.2-debian12'. Omit to use Dataproc's current default image.")
    software_properties: Optional[Dict[str, str]] = Field(default=None, description="Cluster-wide software config properties, e.g. {'spark:spark.executor.memory': '4g'}.")
    idle_delete_ttl_seconds: Optional[int] = Field(default=None, description="Auto-delete the cluster after this many seconds idle. Omit to disable.")

    poll_interval_seconds: float = Field(default=10.0, gt=0.0, description="Seconds between status polls while the create/delete operation is in flight.")
    timeout_seconds: int = Field(default=1200, ge=1, description="Max time to wait for the cluster to reach the target state (cluster creation commonly takes 5-15 minutes).")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['gcp', 'dataproc'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps", "network_tags"), csv=("partition_values",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dataproc Cluster Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.desired_state not in ("present", "absent"):
            raise ValueError(f"desired_state must be 'present' or 'absent', got {self.desired_state!r}")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["gcp", "dataproc"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Ensure Dataproc cluster {self.cluster_name!r} is {self.desired_state}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _dataproc_cluster_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            client = _make_cluster_client(self.region, self.gcp_credentials_env_var)
            existing = _get_cluster(client, self.project_id, self.region, self.cluster_name)

            action = "none"
            status_state = None

            if self.desired_state == "present":
                if existing is not None:
                    context.log.info(f"dataproc_cluster_asset: cluster {self.cluster_name!r} already present.")
                    status_state = existing.status.state.name
                else:
                    cluster = {
                        "project_id": self.project_id,
                        "cluster_name": self.cluster_name,
                        "config": _build_cluster_config(self),
                    }
                    op = client.create_cluster(project_id=self.project_id, region=self.region, cluster=cluster)
                    context.log.info(f"dataproc_cluster_asset: creating cluster {self.cluster_name!r}, waiting up to {self.timeout_seconds}s.")
                    result = op.result(timeout=self.timeout_seconds)
                    status_state = result.status.state.name
                    action = "created"
            else:  # absent
                if existing is None:
                    context.log.info(f"dataproc_cluster_asset: cluster {self.cluster_name!r} already absent.")
                else:
                    op = client.delete_cluster(project_id=self.project_id, region=self.region, cluster_name=self.cluster_name)
                    context.log.info(f"dataproc_cluster_asset: deleting cluster {self.cluster_name!r}, waiting up to {self.timeout_seconds}s.")
                    op.result(timeout=self.timeout_seconds)
                    action = "deleted"

            metadata: Dict[str, Any] = {
                "project_id": dg.MetadataValue.text(self.project_id),
                "region": dg.MetadataValue.text(self.region),
                "cluster_name": dg.MetadataValue.text(self.cluster_name),
                "action": dg.MetadataValue.text(action),
            }
            if status_state:
                metadata["cluster_status"] = dg.MetadataValue.text(status_state)
            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_dataproc_cluster_asset])
