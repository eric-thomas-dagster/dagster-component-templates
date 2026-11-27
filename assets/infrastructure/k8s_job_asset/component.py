"""K8sJobAssetComponent.

Wraps `dagster-k8s`'s `execute_k8s_job` to launch a Kubernetes Job on materialization, wait for
completion, and surface stdout/stderr in the Dagster run log. Useful for compute that needs to
run in-cluster (training jobs, batch jobs, anything containerized) -- the Airflow
`KubernetesPodOperator` equivalent.
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field


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
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
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


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


class K8sJobAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Kubernetes Job as a Dagster asset via dagster-k8s's `execute_k8s_job` -- the Airflow
    `KubernetesPodOperator` equivalent."""

    asset_name: str = Field(description="Dagster asset name.")
    image: str = Field(description="Container image (e.g. 'my-registry/my-job:v1').")
    command: Optional[List[str]] = Field(default=None, description="Override container command.")
    namespace: str = Field(default="default", description="Kubernetes namespace.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Env vars to inject into the container.")
    cpu_limit: Optional[str] = Field(default=None, description="CPU limit (e.g. '1000m').")
    memory_limit: Optional[str] = Field(default=None, description="Memory limit (e.g. '2Gi').")
    cpu_request: Optional[str] = Field(default=None, description="CPU request (e.g. '500m') -- the guaranteed minimum, as distinct from cpu_limit's ceiling.")
    memory_request: Optional[str] = Field(default=None, description="Memory request (e.g. '1Gi') -- the guaranteed minimum, as distinct from memory_limit's ceiling.")
    image_pull_policy: Optional[str] = Field(
        default=None,
        description="Override the image pull policy (e.g. 'Always', 'IfNotPresent', 'Never') -- useful for local testing with kind. Dagster-k8s default: 'Always'.",
    )
    image_pull_secrets: Optional[List[str]] = Field(
        default=None,
        description="Names of Kubernetes imagePullSecrets to use for this Job's Pod (e.g. ['my-registry-secret']).",
    )
    service_account_name: Optional[str] = Field(
        default=None,
        description="Kubernetes service account under which to run the Job. Defaults to the cluster's 'default' service account.",
    )
    volumes: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Raw Kubernetes Volume specs for the Job's Pod (https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volume-v1-core). Pass straight through -- not simplified, since volume types vary too much (hostPath, configMap, secret, PVC, ...).",
    )
    volume_mounts: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Raw Kubernetes VolumeMount specs for the Job's container (https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volumemount-v1-core). Pass straight through.",
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description="Raise if the Job takes longer than this many seconds -- enforced natively by dagster-k8s's execute_k8s_job (timeout=) or PipesK8sClient (pod_wait_timeout=, use_pipes=True), not a client-side watchdog.",
    )
    args: Optional[List[str]] = Field(
        default=None,
        description="Container args, appended after the resolved command (not a replacement for it) -- Kubernetes' args field, analogous to Airflow's KubernetesPodOperator arguments.",
    )
    labels: Optional[Dict[str, str]] = Field(
        default=None,
        description="Kubernetes labels applied to the Job's Pod metadata.",
    )
    node_selector: Optional[Dict[str, str]] = Field(
        default=None,
        description="Kubernetes nodeSelector for the Pod (e.g. {'disktype': 'ssd'}) -- constrains scheduling to nodes with matching labels.",
    )
    tolerations: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Raw Kubernetes Toleration specs for the Pod (https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#toleration-v1-core), e.g. [{'key': 'dedicated', 'operator': 'Equal', 'value': 'ml', 'effect': 'NoSchedule'}]. Pass straight through.",
    )
    affinity: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Raw Kubernetes Affinity spec for the Pod (https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#affinity-v1-core), e.g. node/pod affinity or anti-affinity rules. Pass straight through -- not simplified, since affinity shapes vary too much.",
    )
    env_from_config_maps: Optional[List[Union[str, Dict[str, Any]]]] = Field(
        default=None,
        description="Kubernetes ConfigMaps whose contents are injected as env vars into the container. Each entry is either a plain ConfigMap name (string), or a dict {name, prefix, optional} for a prefixed (every key gets 'prefix' prepended) and/or optional (pod doesn't fail to schedule if the ConfigMap is missing) reference. Plain-name-only lists still go through dagster-k8s execute_k8s_job's simplified 'env_config_maps'; any entry using prefix/optional is built as a raw envFrom.configMapRef instead.",
    )
    env_from_secrets: Optional[List[Union[str, Dict[str, Any]]]] = Field(
        default=None,
        description="Kubernetes Secrets whose contents are injected as env vars into the container. Same shape as env_from_config_maps (plain name, or {name, prefix, optional}).",
    )
    kubeconfig_file: Optional[str] = Field(
        default=None,
        description="Path to a kubeconfig file for out-of-cluster execution (e.g. a local dev machine or a CI runner outside the target cluster). When set, in-cluster config loading is disabled in favor of this file.",
    )
    in_cluster: Optional[bool] = Field(
        default=None,
        description="Force in-cluster service-account config (True) or the default local kubeconfig discovery -- ~/.kube/config or $KUBECONFIG (False) -- when kubeconfig_file is unset. Ignored when kubeconfig_file is set (that file is always used). Leave unset to keep this component's historical default (assumes in-cluster on the execute_k8s_job path; auto-detects on the use_pipes=True path) -- set False explicitly to run against a remote cluster from a machine that is not itself in that cluster, without pointing at an explicit kubeconfig file.",
    )
    kube_context: Optional[str] = Field(
        default=None,
        description="Select a specific context within kubeconfig_file (for a kubeconfig with multiple cluster contexts). Ignored if kubeconfig_file is unset (in-cluster config has no notion of contexts). Defaults to the kubeconfig's own current-context.",
    )
    k8s_job_name: Optional[str] = Field(
        default=None,
        description="Override the Kubernetes Job name. Leave unset (recommended): dagster-k8s then derives a name that is already unique per run AND per op/asset. Setting this explicitly is only for when you need a fixed, predictable name -- be aware two steps sharing the same explicit name in the same run will collide.",
    )
    delete_failed_k8s_jobs: Optional[bool] = Field(
        default=None,
        description="Whether to immediately delete a failed Kubernetes Job object (dagster-k8s's own default: True). Set False to leave failed Jobs/Pods in the cluster for inspection -- pair with ttl_seconds_after_finished for automatic cleanup instead of leaving them forever. Only applies to the execute_k8s_job path (use_pipes=False); PipesK8sClient manages bare Pods, not Jobs, and always deletes its Pod on completion regardless of this field.",
    )
    ttl_seconds_after_finished: Optional[int] = Field(
        default=None,
        description="Sets the Job's native Kubernetes .spec.ttlSecondsAfterFinished -- the cluster's own TTL controller deletes the finished Job (success OR failure) after this many seconds, regardless of delete_failed_k8s_jobs. Only applies to the execute_k8s_job path (use_pipes=False).",
    )
    use_pipes: bool = Field(
        default=False,
        description=(
            "Use dagster-k8s's PipesK8sClient instead of execute_k8s_job -- the Airflow "
            "KubernetesPodOperator equivalent WITH the Dagster Pipes protocol: the pod streams "
            "logs AND structured messages (custom metadata, asset checks, a real materialized "
            "value) back live, instead of execute_k8s_job's plain log tailing + a fixed "
            "metadata dict after the fact. Requires the container's entrypoint to use the "
            "`dagster-pipes` package itself (`with dagster_pipes.open_dagster_pipes() as "
            "context: ...`) -- a plain, Pipes-unaware image works with execute_k8s_job "
            "(use_pipes=False, the default) but not here."
        ),
    )
    pipes_extras: Optional[Dict[str, Any]] = Field(
        default=None,
        description="use_pipes=True only: extra key-value data passed to the pod's PipesContext (read inside the container via `context.get_extra(key)`).",
    )
    group_name: str = Field(default="k8s", description="Asset group.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is time-based.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None, description="Backfill execution strategy for a partitioned asset.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-inferred from component name when unset.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(
            data,
            lists=('deps', 'owners', 'kinds', 'image_pull_secrets', 'args', 'env_from_config_maps', 'env_from_secrets'),
            csv=('partition_values',),
            tags=('asset_tags', 'labels', 'node_selector'),
            strs=('env_vars',),
        )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='K8s Job Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        cmd = self.command
        env_vars = self.env_vars or {}
        cpu = self.cpu_limit
        mem = self.memory_limit
        cpu_request = self.cpu_request
        mem_request = self.memory_request
        ns = self.namespace
        image = self.image
        image_pull_policy = self.image_pull_policy
        image_pull_secrets = [{"name": s} for s in (self.image_pull_secrets or [])] or None
        service_account_name = self.service_account_name
        volumes = self.volumes
        volume_mounts = self.volume_mounts
        timeout_seconds = self.timeout_seconds
        use_pipes = self.use_pipes
        pipes_extras = self.pipes_extras
        args = self.args
        labels = self.labels
        node_selector = self.node_selector
        tolerations = self.tolerations
        affinity = self.affinity
        env_from_config_maps = self.env_from_config_maps
        env_from_secrets = self.env_from_secrets
        kubeconfig_file = self.kubeconfig_file
        kube_context = self.kube_context
        in_cluster = self.in_cluster
        k8s_job_name = self.k8s_job_name
        delete_failed_k8s_jobs = self.delete_failed_k8s_jobs
        ttl_seconds_after_finished = self.ttl_seconds_after_finished

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        def _normalize_env_from_entry(e):
            return {"name": e} if isinstance(e, str) else dict(e)

        def _build_env_from_list(entries, ref_key):
            out = []
            for e in entries:
                ref = {"name": e["name"]}
                if e.get("optional") is not None:
                    ref["optional"] = e["optional"]
                item = {ref_key: ref}
                if e.get("prefix"):
                    item["prefix"] = e["prefix"]
                out.append(item)
            return out

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=f"Kubernetes Job: {image}",
            deps=deps_keys,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy_obj,
            owners=self.owners or [],
            tags=self.asset_tags or None,
            kinds=set(self.kinds) if self.kinds else None,
        )
        def _k8s_asset(context: dg.AssetExecutionContext):
            resources = None
            _limits = {k: v for k, v in {"cpu": cpu, "memory": mem}.items() if v}
            _requests = {k: v for k, v in {"cpu": cpu_request, "memory": mem_request}.items() if v}
            if _limits or _requests:
                resources = {}
                if _limits:
                    resources["limits"] = _limits
                if _requests:
                    resources["requests"] = _requests

            _cm_entries = [_normalize_env_from_entry(e) for e in (env_from_config_maps or [])]
            _secret_entries = [_normalize_env_from_entry(e) for e in (env_from_secrets or [])]
            _needs_raw_env_from = any(e.get("prefix") or e.get("optional") is not None for e in _cm_entries + _secret_entries)

            if use_pipes:
                from dagster_k8s import PipesK8sClient

                # PipesK8sClient has no dedicated resources/volumes/service_account/
                # image_pull_policy kwargs (unlike execute_k8s_job) -- those go through the raw
                # base_pod_spec dict instead (container[0] overrides + pod-level fields).
                container_overrides: Dict[str, Any] = {}
                if resources:
                    container_overrides["resources"] = resources
                if volume_mounts:
                    container_overrides["volume_mounts"] = volume_mounts
                if image_pull_policy:
                    container_overrides["image_pull_policy"] = image_pull_policy
                if args:
                    container_overrides["args"] = args
                if _cm_entries or _secret_entries:
                    container_overrides["env_from"] = (
                        _build_env_from_list(_cm_entries, "config_map_ref")
                        + _build_env_from_list(_secret_entries, "secret_ref")
                    )

                base_pod_spec: Dict[str, Any] = {}
                if container_overrides:
                    base_pod_spec["containers"] = [container_overrides]
                if volumes:
                    base_pod_spec["volumes"] = volumes
                if service_account_name:
                    base_pod_spec["service_account_name"] = service_account_name
                if image_pull_secrets:
                    base_pod_spec["image_pull_secrets"] = image_pull_secrets
                if node_selector:
                    base_pod_spec["node_selector"] = node_selector
                if tolerations:
                    base_pod_spec["tolerations"] = tolerations
                if affinity:
                    base_pod_spec["affinity"] = affinity

                base_pod_meta = {"labels": labels} if labels else None

                client = PipesK8sClient(
                    load_incluster_config=(
                        False if kubeconfig_file
                        else (in_cluster if in_cluster is not None else None)
                    ),
                    kubeconfig_file=kubeconfig_file,
                    kube_context=kube_context,
                )
                return client.run(
                    context=context,
                    image=image,
                    command=cmd,
                    namespace=ns,
                    env=env_vars or None,
                    extras=pipes_extras,
                    base_pod_meta=base_pod_meta,
                    base_pod_spec=base_pod_spec or None,
                    pod_wait_timeout=float(timeout_seconds) if timeout_seconds else 86400.0,
                ).get_materialize_result()

            from dagster_k8s import execute_k8s_job

            # execute_k8s_job's `env_vars` is a List[str] of "KEY=value" (parsed via
            # dagster._core.utils.parse_env_var) -- NOT a dict. Passing a dict here (as this
            # component did before) iterates its KEYS only, each treated as a bare var name to
            # read from the LAUNCHER's own environment, not the literal value the user configured.
            env_var_list = [f"{k}={v}" for k, v in env_vars.items()] or None

            _pod_spec_config: Dict[str, Any] = {}
            if node_selector:
                _pod_spec_config["node_selector"] = node_selector
            if tolerations:
                _pod_spec_config["tolerations"] = tolerations
            if affinity:
                _pod_spec_config["affinity"] = affinity

            _job_spec_config: Dict[str, Any] = {}
            if ttl_seconds_after_finished is not None:
                _job_spec_config["ttl_seconds_after_finished"] = ttl_seconds_after_finished

            _execute_kwargs: Dict[str, Any] = {}
            if delete_failed_k8s_jobs is not None:
                _execute_kwargs["delete_failed_k8s_jobs"] = delete_failed_k8s_jobs

            # dagster-k8s's own env_config_maps/env_secrets params only take plain names (no
            # prefix/optional) -- when any entry needs that richness, build the real envFrom
            # list by hand and merge it in via container_config instead, rather than double-
            # injecting through both paths.
            _env_config_maps_arg = None
            _env_secrets_arg = None
            _container_config = None
            if _needs_raw_env_from:
                _container_config = {
                    "env_from": (
                        _build_env_from_list(_cm_entries, "config_map_ref")
                        + _build_env_from_list(_secret_entries, "secret_ref")
                    )
                }
            else:
                _env_config_maps_arg = [e["name"] for e in _cm_entries] or None
                _env_secrets_arg = [e["name"] for e in _secret_entries] or None

            # kubeconfig_file always wins when set. Otherwise: in_cluster explicitly set wins;
            # unset preserves this component's historical default (assume in-cluster), so
            # existing deployments keep working unchanged.
            _load_incluster_execute = (
                False if kubeconfig_file
                else (in_cluster if in_cluster is not None else True)
            )

            execute_k8s_job(
                context=context,
                image=image,
                command=cmd,
                args=args,
                # No k8s_job_name override by default: dagster-k8s derives a name that's already
                # unique per run AND per op/asset. The previous `run_id[:32]` override discarded
                # that uniqueness -- run_id is shared by every step in a run, so two k8s_job_asset
                # steps (or two ops) in the same run collided on the identical Job name.
                k8s_job_name=k8s_job_name,
                namespace=ns,
                resources=resources,
                env_vars=env_var_list,
                env_config_maps=_env_config_maps_arg,
                env_secrets=_env_secrets_arg,
                container_config=_container_config,
                image_pull_policy=image_pull_policy,
                image_pull_secrets=image_pull_secrets,
                service_account_name=service_account_name,
                volumes=volumes,
                volume_mounts=volume_mounts,
                labels=labels,
                timeout=timeout_seconds,
                pod_spec_config=_pod_spec_config or None,
                job_spec_config=_job_spec_config or None,
                kubeconfig_file=kubeconfig_file,
                load_incluster_config=_load_incluster_execute,
                _kubeconfig_file_context=kube_context,
                **_execute_kwargs,
            )
            return dg.MaterializeResult(metadata={"image": dg.MetadataValue.text(image), "namespace": dg.MetadataValue.text(ns)})
        return dg.Definitions(assets=[_k8s_asset])
