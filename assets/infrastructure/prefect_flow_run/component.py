"""Prefect Flow Run — Dagster asset that triggers a Prefect deployment.

Materializing this asset creates a Prefect flow run (via
`prefect.deployments.run_deployment`) and — by default — waits for the
flow to complete before returning. Flow run id, state, state message,
and per-parameter values land in the materialization metadata so you
can inspect them from `dg dev`.

The asset works against:
  - Local Prefect server (`prefect server start` — default api_url
    'http://127.0.0.1:4200/api').
  - Prefect Cloud (set api_url + api_key_env_var to your cloud workspace).

For a per-file document processing flow (Dagster owns the partition,
Prefect owns the per-file work), pair with a partitioned upstream that
supplies the parameters — e.g. a partition_static_column pointing at a
`file_path` field in an upstream DataFrame.

Docs: https://docs.prefect.io/latest/develop/deployments/
"""
import os
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Canonical partition builder shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set partition_type OR partition_dimensions, not both.")

    def _axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            n = spec.get("dynamic_partition_name") or spec.get("name")
            return DynamicPartitionsDefinition(name=n)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    _vals = (
        [v.strip() for v in partition_values.split(",") if v.strip()]
        if isinstance(partition_values, str) else list(partition_values or [])
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _vals: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class PrefectFlowRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Prefect deployment and materialize the flow run as a Dagster asset.

    Example — local Prefect server, unpartitioned:

        ```yaml
        type: dagster_community_components.PrefectFlowRunAssetComponent
        attributes:
          asset_name: nightly_report
          deployment_name: "reporting/nightly"      # flow_name/deployment_name
          parameters:
            region: us-east-1
            date: "2026-07-31"
          wait_for_result: true
          api_url: http://127.0.0.1:4200/api
        ```

    Example — Prefect Cloud, dynamic-partitioned per file:

        ```yaml
        type: dagster_community_components.PrefectFlowRunAssetComponent
        attributes:
          asset_name: parsed_document
          deployment_name: "document-parser/main"
          # {partition_key} substitutes into any string parameter value.
          parameters:
            file_path: "{partition_key}"
          partition_type: dynamic
          dynamic_partition_name: incoming_files
          api_url: https://api.prefect.cloud/api/accounts/<acct>/workspaces/<ws>
          api_key_env_var: PREFECT_API_KEY
        ```
    """

    asset_name: str = Field(description="Dagster asset name (or '/' path).")
    deployment_name: str = Field(
        description="Prefect deployment name in 'flow_name/deployment_name' format.",
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Parameters passed to the flow run. String values are templated: "
            "`{partition_key}` substitutes the current partition key; "
            "`{run_id}` substitutes the Dagster run_id. Non-string values pass through."
        ),
    )
    wait_for_result: bool = Field(
        default=True,
        description=(
            "Wait for the flow run to reach a terminal state before the "
            "Dagster asset finishes materializing. When False, the asset "
            "returns immediately after submitting the flow run — use this "
            "for fire-and-forget triggers and pair with `prefect_flow_run_sensor` "
            "downstream."
        ),
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Only used when wait_for_result=True. Max seconds to wait for the "
            "flow run. None = wait indefinitely. If exceeded, the asset raises."
        ),
    )
    poll_interval_seconds: float = Field(
        default=5.0,
        description="Only used when wait_for_result=True. Seconds between polls.",
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Tags applied to the created flow run.",
    )
    flow_run_name: Optional[str] = Field(
        default=None,
        description=(
            "Optional flow run name. Templated with `{partition_key}` and "
            "`{run_id}` like parameter values."
        ),
    )

    # Connection — inline (skip the resource for one-off usage)
    api_url: str = Field(
        default="http://127.0.0.1:4200/api",
        description="Prefect API URL. Default is local server at :4200.",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a Prefect Cloud API key. Leave unset for local server.",
    )

    # Failure semantics
    fail_on_flow_run_failure: bool = Field(
        default=True,
        description=(
            "When True and wait_for_result=True, the Dagster asset fails if "
            "the Prefect flow run ends in a non-COMPLETED state (FAILED, "
            "CRASHED, CANCELLED). When False, the asset always materializes "
            "successfully — inspect the state in the metadata."
        ),
    )

    # Standard catalog metadata
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['prefect'].",
    )
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    # Partitions
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi'|None",
    )
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    # Deps (lineage-only)
    deps: Optional[List[str]] = Field(default=None)

    # Retry
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        kinds = self.kinds or ["prefect"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        freshness = None
        if self.freshness_max_lag_minutes is not None:
            freshness = dg.FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        def _apply_env():
            os.environ["PREFECT_API_URL"] = _self.api_url
            if _self.api_key_env_var:
                key = os.environ.get(_self.api_key_env_var)
                if key:
                    os.environ["PREFECT_API_KEY"] = key

        def _template(value: Any, partition_key: str, run_id: str) -> Any:
            if not isinstance(value, str):
                return value
            return value.replace("{partition_key}", partition_key).replace("{run_id}", run_id)

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or f"Trigger Prefect deployment {self.deployment_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            freshness_policy=freshness,
            retry_policy=retry_policy,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _flow_run_asset(context: dg.AssetExecutionContext) -> dict:
            _apply_env()
            from prefect.deployments import run_deployment

            partition_key = str(context.partition_key) if context.has_partition_key else ""
            run_id = str(getattr(context, "run_id", "") or "")

            params = {}
            for k, v in (_self.parameters or {}).items():
                params[k] = _template(v, partition_key, run_id)

            flow_run_name = _template(_self.flow_run_name, partition_key, run_id) if _self.flow_run_name else None

            context.log.info(
                f"Triggering Prefect deployment {_self.deployment_name!r} "
                f"with params={params} (wait_for_result={_self.wait_for_result})"
            )

            flow_run = run_deployment(
                name=_self.deployment_name,
                parameters=params or None,
                flow_run_name=flow_run_name,
                tags=list(_self.tags or []),
                timeout=(_self.timeout_seconds if _self.wait_for_result else 0),
                poll_interval=_self.poll_interval_seconds,
            )

            state = flow_run.state
            state_name = getattr(state, "name", "unknown") if state else "unknown"
            state_type = getattr(getattr(state, "type", None), "value", "unknown")
            state_message = getattr(state, "message", "") if state else ""

            result_meta: Dict[str, Any] = {
                "flow_run_id": dg.MetadataValue.text(str(flow_run.id)),
                "flow_name": dg.MetadataValue.text(str(getattr(flow_run, "name", ""))),
                "deployment_name": dg.MetadataValue.text(_self.deployment_name),
                "state_name": dg.MetadataValue.text(state_name),
                "state_type": dg.MetadataValue.text(state_type),
                "state_message": dg.MetadataValue.text(state_message[:500] if state_message else ""),
                "prefect_url": dg.MetadataValue.text(_self.api_url),
                "parameters": dg.MetadataValue.json(params),
                "wait_for_result": dg.MetadataValue.bool(_self.wait_for_result),
            }
            context.add_output_metadata(result_meta)

            terminal_success = (state_type == "COMPLETED")
            terminal_failure = (state_type in {"FAILED", "CRASHED", "CANCELLED"})

            context.log.info(
                f"Prefect flow run {flow_run.id} state={state_name} ({state_type})"
            )

            # Note: we deliberately do NOT try to fetch state.result(). Fetching
            # the flow's return value requires Prefect result persistence to be
            # configured, which is Prefect-specific plumbing users shouldn't have
            # to worry about here. Downstream Dagster assets read the artifacts
            # the flow WROTE (to S3/GCS/local disk) — the standard Prefect-owns-
            # execution pattern. This asset's return dict is intentionally small
            # (flow_run_id + state + parameters), enough for lineage + to derive
            # where the flow's artifacts landed if the output path was passed as
            # a parameter.

            if _self.wait_for_result and _self.fail_on_flow_run_failure and terminal_failure:
                raise dg.Failure(
                    description=f"Prefect flow run ended in {state_name} state: {state_message}",
                    metadata=result_meta,
                )

            return {
                "flow_run_id": str(flow_run.id),
                "state_name": state_name,
                "state_type": state_type,
                "parameters": params,
                "terminal_success": terminal_success,
            }

        return dg.Definitions(assets=[_flow_run_asset])
