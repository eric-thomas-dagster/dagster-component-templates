"""Temporal Query Asset — materialize the live state of a running Temporal workflow.

Sends a Query (Temporal's synchronous read API) to a running Workflow and
stores the returned value as the asset's materialization. The workflow
processes the Query in its own event loop and responds; no state is mutated.

Common shape:
  - A long-lived Temporal workflow holds live state (pending items, current
    step, computed aggregates).
  - Dagster fires an asset periodically to read that state.
  - Downstream Dagster assets consume the materialized value.

Because Queries are read-only, the materialization is safe to run at any
cadence — schedule-driven, sensor-driven, or on demand.

Docs: https://docs.temporal.io/develop/python/message-passing#queries
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class TemporalQueryAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Query a running Temporal workflow and materialize its response.

    Example — Dagster materializes the current "pending approvals" from a
    long-running Temporal approval router:

        ```yaml
        type: dagster_community_components.TemporalQueryAssetComponent
        attributes:
          asset_key: temporal/approvals/pending
          workflow_id: approval-router-v1
          query_name: get_pending
          query_arg:
            since: "{run_id}"
          target_host: localhost:7233
          namespace: default
        ```
    """

    asset_key: str = Field(description="Dagster asset key, '/'-separated.")
    workflow_id: str = Field(
        description="Target workflow ID. Queries are addressed by workflow_id (current run receives it).",
    )
    query_name: str = Field(description="Query handler name defined on the workflow.")
    query_args: Optional[List[Any]] = Field(
        default=None,
        description="Positional args for the query. If both `query_args` and `query_arg` are set, `query_arg` is appended.",
    )
    query_arg: Optional[Any] = Field(
        default=None,
        description="Convenience: single arg (dict/str/int) when the query handler takes exactly one input.",
    )
    query_timeout_seconds: Optional[int] = Field(
        default=30,
        description="Cap on how long to wait for the query to complete.",
    )

    target_host: str = Field(default="localhost:7233", description="Temporal frontend host:port.")
    namespace: str = Field(default="default", description="Temporal namespace.")
    api_key_env_var: Optional[str] = Field(default=None, description="Temporal Cloud API key env var.")
    tls_cert_env_var: Optional[str] = Field(default=None, description="mTLS client cert env var.")
    tls_key_env_var: Optional[str] = Field(default=None, description="mTLS private key env var.")
    tls_server_root_ca_env_var: Optional[str] = Field(default=None, description="Custom root CA env var.")

    group_name: Optional[str] = Field(default="temporal", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'temporal', 'query').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"temporal", "query"})

        @dg.asset(
            key=dg.AssetKey(_self.asset_key.split("/")),
            group_name=_self.group_name,
            kinds=_kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Query Temporal workflow {_self.workflow_id!r} for {_self.query_name!r} on {_self.target_host}."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> Any:
            import asyncio
            import os

            try:
                from temporalio.client import Client
                from temporalio.service import TLSConfig
            except ImportError:
                raise ImportError("pip install 'temporalio>=1.7.0'")

            _validate_temporal_config(
                target_host=_self.target_host,
                namespace=_self.namespace,
                api_key_env_var=_self.api_key_env_var,
                log=context.log,
            )

            subs = {"run_id": context.run_id}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    subs["partition_key"] = str(pk)
                    subs["partition_keys"] = dict(pk.keys_by_dimension)
                else:
                    subs["partition_key"] = str(pk)
                    subs["partition_keys"] = {}

            args = _substitute_args(list(_self.query_args or []), subs)
            if _self.query_arg is not None:
                args.append(_substitute_value(_self.query_arg, subs))

            async def _query() -> Any:
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
                    tls_cfg = True

                client_kwargs: Dict[str, Any] = {
                    "target_host": _self.target_host,
                    "namespace": _self.namespace,
                }
                if tls_cfg is not None:
                    client_kwargs["tls"] = tls_cfg
                if _self.api_key_env_var:
                    ak = os.environ.get(_self.api_key_env_var)
                    if not ak:
                        raise RuntimeError(f"api_key_env_var {_self.api_key_env_var!r} not set.")
                    client_kwargs["api_key"] = ak

                client = await Client.connect(**client_kwargs)
                handle = client.get_workflow_handle(_self.workflow_id)
                context.log.info(
                    f"[temporal] querying workflow_id={_self.workflow_id!r} "
                    f"query={_self.query_name!r} args_count={len(args)}"
                )
                coro = handle.query(_self.query_name, *args) if args else handle.query(_self.query_name)
                if _self.query_timeout_seconds:
                    return await asyncio.wait_for(coro, timeout=_self.query_timeout_seconds)
                return await coro

            result = asyncio.run(_query())
            context.log.info(f"[temporal] query returned type={type(result).__name__}")

            md: Dict[str, Any] = {
                "temporal_workflow_id": _self.workflow_id,
                "temporal_query_name": _self.query_name,
                "temporal_namespace": _self.namespace,
                "temporal_target_host": _self.target_host,
            }
            if isinstance(result, (dict, list)):
                md["query_result"] = dg.MetadataValue.json(result)
                if isinstance(result, list):
                    md["query_result_count"] = len(result)
            else:
                md["query_result"] = dg.MetadataValue.text(str(result)[:2000])

            context.add_output_metadata(md)
            return result

        return dg.Definitions(assets=[_asset])


def _validate_temporal_config(
    target_host: str,
    namespace: str,
    api_key_env_var: Optional[str],
    log,
    minimum_interval_seconds: Optional[int] = None,
) -> Optional[int]:
    """Preflight validation for Temporal Cloud / API-key auth. See trigger docs."""
    is_cloud = ".tmprl.cloud" in (target_host or "").lower()
    if is_cloud and "." not in (namespace or ""):
        log.warning(
            f"[temporal] namespace={namespace!r} looks incomplete for Temporal Cloud — "
            f"Cloud namespaces are '<name>.<account_id>' (e.g. 'myns.abcde')."
        )
    if api_key_env_var:
        try:
            import temporalio
            ver = getattr(temporalio, "__version__", "0")
            major, minor, *_ = (int(x) for x in ver.split(".")[:2])
            if (major, minor) < (1, 8):
                raise RuntimeError(
                    f"api_key_env_var is set but temporalio=={ver} doesn't support API-key auth. "
                    f"Upgrade: pip install 'temporalio>=1.8.0'."
                )
        except (AttributeError, ValueError):
            pass
    if minimum_interval_seconds is not None and is_cloud and minimum_interval_seconds < 60:
        log.warning(
            f"[temporal] minimum_interval_seconds={minimum_interval_seconds} is aggressive for "
            f"Temporal Cloud — clamping to 60."
        )
        return 60
    return minimum_interval_seconds


def _substitute_value(v: Any, subs: Dict[str, Any]) -> Any:
    if isinstance(v, str):
        if "{" not in v:
            return v
        out = v
        out = out.replace("{run_id}", str(subs.get("run_id", "")))
        out = out.replace("{partition_key}", str(subs.get("partition_key", "")))
        for dim, val in (subs.get("partition_keys") or {}).items():
            out = out.replace("{partition_keys." + dim + "}", str(val))
        return out
    if isinstance(v, dict):
        return {k: _substitute_value(val, subs) for k, val in v.items()}
    if isinstance(v, list):
        return [_substitute_value(x, subs) for x in v]
    return v


def _substitute_args(args: List[Any], subs: Dict[str, Any]) -> List[Any]:
    return [_substitute_value(a, subs) for a in args]
