"""Temporal Signal Asset — send a Signal to a running Temporal workflow.

Materializes an asset by sending a Signal (Temporal's inbound event API) to a
long-lived running Workflow. Signals are the canonical way to push state INTO
a running workflow — think "flush the current batch", "add this order", "kick
off next iteration", "graceful shutdown".

Common shape:
  - A Temporal workflow runs indefinitely (batch aggregator, approval router,
    long-lived agent, streaming pipeline).
  - Dagster fires an asset on some cadence (schedule, sensor, upstream ready).
  - That materialization sends a Signal into the workflow, causing it to act.

Signals are fire-and-forget from the SDK's perspective. This component
captures the signal timestamp + name + arg preview in materialization
metadata for lineage.

Docs: https://docs.temporal.io/develop/python/message-passing#signals
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class TemporalSignalAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send a Temporal Signal to a running workflow as a Dagster asset.

    Example — nightly Dagster schedule fires "flush batch" signal into a
    long-lived Temporal batch aggregator:

        ```yaml
        type: dagster_community_components.TemporalSignalAssetComponent
        attributes:
          asset_key: temporal/batch/flush_signal
          workflow_id: order-aggregator-v2
          signal_name: flush_batch
          signal_arg:
            reason: nightly
            requested_at: "{run_id}"
          target_host: localhost:7233
          namespace: default
        ```

    On Temporal Cloud, add ``api_key_env_var`` or ``tls_cert_env_var`` +
    ``tls_key_env_var`` like the trigger / sensor components.
    """

    asset_key: str = Field(description="Dagster asset key, '/'-separated.")
    workflow_id: str = Field(
        description=(
            "Target workflow ID. Signals are addressed by workflow_id, not by run — "
            "if there's a currently running run of this id, it receives the signal."
        ),
    )
    signal_name: str = Field(description="Signal handler name defined on the workflow.")
    signal_args: Optional[List[Any]] = Field(
        default=None,
        description="Positional args for the signal. If both `signal_args` and `signal_arg` are set, `signal_arg` is appended.",
    )
    signal_arg: Optional[Any] = Field(
        default=None,
        description="Convenience: single arg (dict/str/int) when the signal handler takes exactly one input.",
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
        description="Asset kinds (auto-includes 'temporal', 'signal').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"temporal", "signal"})

        @dg.asset(
            key=dg.AssetKey(_self.asset_key.split("/")),
            group_name=_self.group_name,
            kinds=_kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Send Temporal signal {_self.signal_name!r} to workflow "
                f"{_self.workflow_id!r} on {_self.target_host}."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import asyncio
            import os
            from datetime import datetime, timezone

            try:
                from temporalio.client import Client
                from temporalio.service import TLSConfig
            except ImportError:
                raise ImportError("pip install 'temporalio>=1.7.0'")

            # {run_id} etc. substitution — applied to string-shaped args.
            subs = {"run_id": context.run_id}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    subs["partition_key"] = str(pk)
                    subs["partition_keys"] = dict(pk.keys_by_dimension)
                else:
                    subs["partition_key"] = str(pk)
                    subs["partition_keys"] = {}

            args = _substitute_args(list(_self.signal_args or []), subs)
            if _self.signal_arg is not None:
                args.append(_substitute_value(_self.signal_arg, subs))

            async def _send() -> str:
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
                    f"[temporal] sending signal={_self.signal_name!r} "
                    f"to workflow_id={_self.workflow_id!r} args_count={len(args)}"
                )
                await handle.signal(_self.signal_name, *args) if args else await handle.signal(_self.signal_name)
                return datetime.now(timezone.utc).isoformat()

            sent_at = asyncio.run(_send())
            context.log.info(f"[temporal] signal delivered at {sent_at}")

            md: Dict[str, Any] = {
                "temporal_workflow_id": _self.workflow_id,
                "temporal_signal_name": _self.signal_name,
                "temporal_namespace": _self.namespace,
                "temporal_target_host": _self.target_host,
                "signal_sent_at": sent_at,
                "signal_args_count": len(args),
            }
            if args:
                preview = args[0] if len(args) == 1 else args
                if isinstance(preview, (dict, list)):
                    md["signal_args_preview"] = dg.MetadataValue.json(preview)
                else:
                    md["signal_args_preview"] = dg.MetadataValue.text(str(preview)[:1000])
            return dg.MaterializeResult(metadata=md)

        return dg.Definitions(assets=[_asset])


def _substitute_value(v: Any, subs: Dict[str, Any]) -> Any:
    """Substitute template tokens inside strings; recurse into dicts/lists."""
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
