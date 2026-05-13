"""SAP Event Mesh Sensor Component.

Poll an SAP Event Mesh queue and trigger a Dagster job (or yield RunRequests
with dynamic partition keys) for each new event. Real-time-ish: with a short
polling interval, latency is bounded by the interval.

SAP Event Mesh runs on BTP — AMQP 1.0 broker that fronts S/4HANA business
events, SuccessFactors events, custom app events, etc. This sensor talks to
its REST endpoint (simpler than AMQP-WebSocket, no persistent connection).

Auth: OAuth 2.0 client_credentials via XSUAA. Pair with `oauth_token_resource`
for headless token lifecycle.

API used:
  GET <host>/messagingrest/v1/queues/<queue>/messages?count=<n>
    → returns up to <n> messages, REMOVING them from the queue (pop semantics)

Set `peek: true` to use the peek endpoint instead (doesn't pop — useful for
read-only observation):
  GET <host>/messagingrest/v1/queues/<queue>/messages/peek?count=<n>
"""

import json
from typing import Optional

import dagster as dg
from dagster import (
    AddDynamicPartitionsRequest,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    SkipReason,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class SapEventMeshSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Poll an SAP Event Mesh queue and fire RunRequests / register partitions.

    Example — trigger a Dagster job per event:

        ```yaml
        type: dagster_component_templates.SapEventMeshSensorComponent
        attributes:
          sensor_name: sap_em_orders_sensor
          messaging_host: https://my-tenant.messaging.eu10.hana.ondemand.com
          queue_name: orders.received
          oauth_token_resource_key: em_token
          job_name: process_order_event_job
          minimum_interval_seconds: 30
        ```

    Example — register a dynamic partition per message:

        ```yaml
        attributes:
          sensor_name: sap_em_invoices_partitioner
          messaging_host: https://my-tenant.messaging.eu10.hana.ondemand.com
          queue_name: invoices.created
          oauth_token_resource_key: em_token
          dynamic_partition_name: invoice_events
          partition_key_template: "{message_id}"
          asset_selection: ["process_invoice"]
        ```
    """

    sensor_name: str = Field(description="Unique Dagster sensor name")

    messaging_host: str = Field(
        description="Event Mesh REST endpoint (e.g. https://my-tenant.messaging.eu10.hana.ondemand.com)"
    )
    queue_name: str = Field(description="Event Mesh queue to consume from")

    oauth_token_resource_key: str = Field(
        description="Key of a registered oauth_token_resource (client_credentials via XSUAA)"
    )

    peek: bool = Field(
        default=False,
        description="If true: peek (don't pop) — for read-only observation. If false: consume (pop).",
    )

    batch_size: int = Field(default=10, ge=1, le=100, description="Messages per poll")
    minimum_interval_seconds: int = Field(default=30)
    request_timeout_seconds: int = Field(default=60)
    verify_ssl: bool = Field(default=True)

    # --- Trigger mode -------------------------------------------------------

    job_name: Optional[str] = Field(
        default=None,
        description="Job to run per message. Mutually exclusive with asset_selection.",
    )
    asset_selection: Optional[list[str]] = Field(
        default=None,
        description="Assets to materialize per message. Use with dynamic_partition_name.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="If set: each message registers a new dynamic partition + fires a RunRequest with that partition key.",
    )
    partition_key_template: str = Field(
        default="{message_id}",
        description=(
            "Template for the dynamic partition key. Supports {message_id} + any "
            "top-level field of the message body. Default '{message_id}'."
        ),
    )

    tags_template: Optional[dict] = Field(
        default=None,
        description="Tags to attach to each RunRequest. Templates resolved like partition_key_template.",
    )

    default_status: str = Field(
        default="stopped", description="'running' or 'stopped' (default — explicit opt-in)"
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self
        default_status = (
            DefaultSensorStatus.RUNNING
            if cfg.default_status.lower() == "running"
            else DefaultSensorStatus.STOPPED
        )

        from dagster import AssetSelection

        selection = None
        if cfg.asset_selection:
            selection = AssetSelection.keys(*cfg.asset_selection)

        @sensor(
            name=cfg.sensor_name,
            minimum_interval_seconds=cfg.minimum_interval_seconds,
            default_status=default_status,
            job_name=cfg.job_name if not selection else None,
            asset_selection=selection,
            required_resource_keys={cfg.oauth_token_resource_key},
        )
        def sap_event_mesh_sensor(context: SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason=SkipReason("requests not installed"))

            token_res = getattr(context.resources, cfg.oauth_token_resource_key)
            headers = {
                "Authorization": token_res.get_authorization_header(),
                "Accept": "application/json",
            }

            path = "messages" if not cfg.peek else "messages/peek"
            url = f"{cfg.messaging_host.rstrip('/')}/messagingrest/v1/queues/{cfg.queue_name}/{path}"

            try:
                resp = requests.get(
                    url,
                    headers=headers,
                    params={"count": cfg.batch_size},
                    timeout=cfg.request_timeout_seconds,
                    verify=cfg.verify_ssl,
                )
            except Exception as e:
                context.log.error(f"Event Mesh GET failed: {e}")
                return SensorResult(skip_reason=SkipReason(f"poll error: {e}"))

            if resp.status_code == 204:
                return SensorResult(skip_reason=SkipReason("no messages"))
            if resp.status_code >= 400:
                context.log.error(f"Event Mesh HTTP {resp.status_code}: {resp.text[:300]}")
                return SensorResult(skip_reason=SkipReason(f"HTTP {resp.status_code}"))

            # Event Mesh returns: either an array of {message_id, payload, properties} (multi)
            # or a single message envelope. Normalize to list.
            body = resp.json() if resp.content else []
            if isinstance(body, dict):
                body = [body]
            if not isinstance(body, list) or not body:
                return SensorResult(skip_reason=SkipReason("empty response"))

            run_requests = []
            new_partitions = []
            for msg in body:
                message_id = (
                    msg.get("messageId")
                    or msg.get("message_id")
                    or msg.get("MessageID")
                    or msg.get("id")
                    or ""
                )
                payload = msg.get("payload") or msg.get("body") or {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {"_raw": payload}

                # Build a flat substitution context
                subst = {"message_id": str(message_id)}
                if isinstance(payload, dict):
                    for k, v in payload.items():
                        if isinstance(v, (str, int, float, bool)):
                            subst[k] = str(v)

                def _render(tpl: str) -> str:
                    out = tpl
                    for k, v in subst.items():
                        out = out.replace("{" + k + "}", v)
                    return out

                partition_key = (
                    _render(cfg.partition_key_template) if cfg.dynamic_partition_name else None
                )
                tags = {k: _render(v) for k, v in (cfg.tags_template or {}).items()}
                tags["sap_event_mesh.message_id"] = str(message_id)

                if partition_key and cfg.dynamic_partition_name:
                    new_partitions.append(partition_key)

                run_requests.append(
                    RunRequest(
                        run_key=str(message_id) or None,
                        tags=tags,
                        partition_key=partition_key,
                    )
                )

            dynamic_partitions_requests = None
            if cfg.dynamic_partition_name and new_partitions:
                dynamic_partitions_requests = [
                    AddDynamicPartitionsRequest(
                        partitions_def_name=cfg.dynamic_partition_name,
                        partition_keys=new_partitions,
                    )
                ]

            return SensorResult(
                run_requests=run_requests,
                dynamic_partitions_requests=dynamic_partitions_requests,
            )

        return dg.Definitions(sensors=[sap_event_mesh_sensor])
