"""Temporal Workflow Trigger — materializable asset that starts a Temporal Workflow.

Uses the ``temporalio`` Python SDK (gRPC) to start a Workflow on any
Temporal cluster: local dev-server, self-hosted, or Temporal Cloud. The
asset submits the workflow, optionally waits for the result, and stores
the workflow_id + run_id in materialization metadata so a paired
``temporal_workflow_sensor`` (in observation mode) can pick up the same
run.

Use Case B (Dagster owns the schedule) — for Case A (a Temporal Schedule
owns it), use ``external_temporal_workflow`` + ``temporal_workflow_sensor``
only, no trigger.

Docs: https://docs.temporal.io/develop/python
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class TemporalWorkflowTriggerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Start a Temporal Workflow via the temporalio SDK.

    Example — start a workflow and wait for its result:

        ```yaml
        type: dagster_community_components.TemporalWorkflowTriggerComponent
        attributes:
          asset_key: temporal/etl/nightly_aggregation
          workflow_type: NightlyAggregationWorkflow
          task_queue: etl-tq
          workflow_id: "nightly-agg-{run_id}"
          workflow_args:
            date: "2026-05-27"
            region: us-east-1
          target_host: localhost:7233
          namespace: default
          wait_for_result: true
        ```

    For Temporal Cloud, set ``target_host: <namespace>.tmprl.cloud:7233``
    and provide TLS credentials via ``tls_cert_env_var`` / ``tls_key_env_var``
    (both mTLS) or ``api_key_env_var`` (Temporal Cloud API keys).

    **Do NOT pair on the same asset_key as ``external_temporal_workflow``** —
    those represent the same workflow with different schedule ownership.
    """

    asset_key: str = Field(
        description="Dagster asset key. Use '/' separators (e.g. 'temporal/etl/nightly').",
    )
    workflow_type: str = Field(
        description="Registered Workflow class name on the Temporal worker (e.g. 'NightlyAggregationWorkflow').",
    )
    task_queue: str = Field(
        description="Task queue the target workflow's worker polls (e.g. 'etl-tq').",
    )
    workflow_id: str = Field(
        description=(
            "Workflow ID. Supports {run_id} / {partition_key} / {partition_keys.<dim>} substitution. "
            "Use a stable prefix + a variable suffix to make dedupe easy."
        ),
    )
    workflow_args: Optional[List[Any]] = Field(
        default=None,
        description="Positional args passed to the workflow. If both `workflow_args` and `workflow_arg` are set, `workflow_arg` is appended.",
    )
    workflow_arg: Optional[Any] = Field(
        default=None,
        description="Convenience: single arg (dict/str/int) when the workflow takes exactly one input. Cleaner for the common single-arg case.",
    )
    task_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Per-workflow task timeout (WorkflowTaskTimeout). Leaves worker-side default when unset.",
    )
    execution_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Total execution timeout (WorkflowExecutionTimeout). Leaves worker-side default when unset.",
    )
    run_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Timeout for a single run of the workflow (WorkflowRunTimeout).",
    )

    target_host: str = Field(
        default="localhost:7233",
        description="Temporal frontend host:port (default local dev-server). For Cloud: '<namespace>.tmprl.cloud:7233'.",
    )
    namespace: str = Field(default="default", description="Temporal namespace.")
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a Temporal Cloud API key. Sets Authorization header via metadata.",
    )
    tls_cert_env_var: Optional[str] = Field(
        default=None,
        description="Env var containing a PEM-encoded client cert (mTLS auth). Pair with tls_key_env_var.",
    )
    tls_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var containing a PEM-encoded client private key (mTLS auth). Pair with tls_cert_env_var.",
    )
    tls_server_root_ca_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a PEM-encoded custom root CA (rarely needed on Temporal Cloud).",
    )

    wait_for_result: bool = Field(
        default=True,
        description=(
            "When true (default), block until the workflow completes and capture its result. "
            "When false, start-fire-and-forget: the run_id lands in metadata; use "
            "temporal_workflow_sensor to observe completion."
        ),
    )
    result_wait_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Cap on how long to wait for the workflow result (when wait_for_result=true).",
    )

    group_name: Optional[str] = Field(default="temporal", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'temporal', 'workflow').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"temporal", "workflow"})

        @dg.asset(
            key=dg.AssetKey(_self.asset_key.split("/")),
            group_name=_self.group_name,
            kinds=_kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Start Temporal workflow {_self.workflow_type} on task queue "
                f"{_self.task_queue!r}. Pairs with temporal_workflow_sensor "
                f"(asset_event_type=observation) if wait_for_result=false."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import asyncio
            import os
            try:
                from temporalio.client import Client
                from temporalio.service import TLSConfig
            except ImportError:
                raise ImportError(
                    "temporal_workflow_trigger requires temporalio: "
                    "pip install 'temporalio>=1.7.0'"
                )

            # Resolve workflow_id via substitution.
            substitutions = {"run_id": context.run_id}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = dict(pk.keys_by_dimension)
                else:
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = {}
            workflow_id = _substitute(_self.workflow_id, substitutions)

            # Assemble args list.
            args: List[Any] = list(_self.workflow_args or [])
            if _self.workflow_arg is not None:
                args.append(_self.workflow_arg)

            async def _run() -> Dict[str, Any]:
                tls_cfg: Any = None
                if _self.tls_cert_env_var and _self.tls_key_env_var:
                    cert = os.environ.get(_self.tls_cert_env_var, "").encode()
                    key = os.environ.get(_self.tls_key_env_var, "").encode()
                    root = None
                    if _self.tls_server_root_ca_env_var:
                        root = os.environ.get(_self.tls_server_root_ca_env_var, "").encode() or None
                    tls_cfg = TLSConfig(
                        client_cert=cert or None,
                        client_private_key=key or None,
                        server_root_ca_cert=root,
                    )
                elif _self.api_key_env_var:
                    # Temporal Cloud API-key auth: TLS on with system roots.
                    tls_cfg = True

                client_kwargs: Dict[str, Any] = {
                    "target_host": _self.target_host,
                    "namespace": _self.namespace,
                }
                if tls_cfg is not None:
                    client_kwargs["tls"] = tls_cfg
                if _self.api_key_env_var:
                    api_key = os.environ.get(_self.api_key_env_var)
                    if not api_key:
                        raise RuntimeError(f"api_key_env_var {_self.api_key_env_var!r} is not set.")
                    client_kwargs["api_key"] = api_key

                client = await Client.connect(**client_kwargs)

                context.log.info(
                    f"[temporal] starting workflow_type={_self.workflow_type!r} "
                    f"workflow_id={workflow_id!r} task_queue={_self.task_queue!r} "
                    f"target_host={_self.target_host} namespace={_self.namespace}"
                )

                start_kwargs: Dict[str, Any] = {
                    "id": workflow_id,
                    "task_queue": _self.task_queue,
                }
                if _self.task_timeout_seconds:
                    from datetime import timedelta
                    start_kwargs["task_timeout"] = timedelta(seconds=_self.task_timeout_seconds)
                if _self.execution_timeout_seconds:
                    from datetime import timedelta
                    start_kwargs["execution_timeout"] = timedelta(seconds=_self.execution_timeout_seconds)
                if _self.run_timeout_seconds:
                    from datetime import timedelta
                    start_kwargs["run_timeout"] = timedelta(seconds=_self.run_timeout_seconds)

                # start_workflow accepts positional args as a single list.
                # The SDK exposes signatures like start_workflow(wf, arg, id=..., task_queue=...)
                # or start_workflow(wf, args=[a, b], ...). Use args=[...] for both cases.
                handle = await client.start_workflow(
                    _self.workflow_type,
                    args=args,
                    **start_kwargs,
                )
                run_id = handle.result_run_id or handle.first_execution_run_id or ""
                context.log.info(f"[temporal] started run_id={run_id!r}")

                result_payload: Dict[str, Any] = {
                    "workflow_id": workflow_id,
                    "run_id": run_id,
                    "workflow_type": _self.workflow_type,
                    "task_queue": _self.task_queue,
                }
                if _self.wait_for_result:
                    from datetime import timedelta
                    if _self.result_wait_timeout_seconds:
                        result = await asyncio.wait_for(
                            handle.result(),
                            timeout=_self.result_wait_timeout_seconds,
                        )
                    else:
                        result = await handle.result()
                    result_payload["result"] = _stringify(result)
                    context.log.info(f"[temporal] workflow completed successfully")
                return result_payload

            out = asyncio.run(_run())

            md: Dict[str, Any] = {
                "temporal_workflow_id": out["workflow_id"],
                "temporal_run_id": out["run_id"],
                "temporal_workflow_type": out["workflow_type"],
                "temporal_task_queue": out["task_queue"],
                "temporal_namespace": _self.namespace,
                "temporal_target_host": _self.target_host,
            }
            if "result" in out:
                # Try to render as JSON metadata if it looks structured; otherwise text.
                result_val = out["result"]
                if isinstance(result_val, (dict, list)):
                    md["temporal_result"] = dg.MetadataValue.json(result_val)
                else:
                    md["temporal_result"] = dg.MetadataValue.text(str(result_val)[:2000])
            return dg.MaterializeResult(metadata=md)

        return dg.Definitions(assets=[_asset])


def _substitute(s: str, substitutions: Dict[str, Any]) -> str:
    if "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
    return out


def _stringify(result: Any) -> Any:
    """Coerce common Temporal result shapes into JSON-safe values."""
    try:
        import json
        json.dumps(result)
        return result
    except (TypeError, ValueError):
        return str(result)
