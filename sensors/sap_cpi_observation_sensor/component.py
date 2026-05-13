"""SAP Cloud Integration (CPI / Integration Suite) Observation Sensor.

Poll the SAP Integration Suite Message Processing Logs (MPL) OData API and
emit AssetObservations for completed iFlow runs. Lets Dagster track when
SAP-side iFlows have run, with what status, against which message IDs —
without Dagster owning the iFlow itself.

SAP Integration Suite is the cloud middleware that replaced PI/PO (and
sits alongside SAP Datasphere / Event Mesh). Customers run iFlows for
S/4HANA ↔ SaaS sync, EDI, file-based integration, etc. This sensor gives
Dagster observability over those flows.

API: GET <tmn-host>/api/v1/MessageProcessingLogs?$filter=...&$orderby=LogEnd desc

Auth: OAuth client_credentials via SAP Integration Suite's OAuth client. Pair
with `oauth_token_resource`.
"""

from typing import Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetObservation,
    MetadataValue,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class SapCPIObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit AssetObservations for SAP Integration Suite iFlow runs.

    Example:

        ```yaml
        type: dagster_component_templates.SapCPIObservationSensorComponent
        attributes:
          sensor_name: cpi_orders_iflow_observer
          tmn_host: https://my-tenant.it-cpi017.cfapps.us10-002.hana.ondemand.com
          iflow_id: orders_to_s4hana
          asset_key: sap_cpi/orders_to_s4hana
          oauth_token_resource_key: cpi_token
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(description="Unique Dagster sensor name")

    tmn_host: str = Field(
        description="Integration Suite Tenant Management Node URL (e.g. https://<tenant>.it-cpi<n>.cfapps.<region>.hana.ondemand.com)"
    )
    iflow_id: Optional[str] = Field(
        default=None,
        description="iFlow IntegrationFlow ID to observe. Leave empty to observe ALL iFlows on the tenant.",
    )
    asset_key: str = Field(
        description="Dagster asset key to emit observations for (slash-delimited)"
    )

    oauth_token_resource_key: str = Field(
        description="Key of a registered oauth_token_resource"
    )

    only_statuses: Optional[list[str]] = Field(
        default=None,
        description="Filter to specific MPL statuses: COMPLETED, FAILED, RETRY, etc. Default: all.",
    )

    minimum_interval_seconds: int = Field(default=60)
    request_timeout_seconds: int = Field(default=60)
    batch_size: int = Field(default=50, description="Max MPLs per poll")
    verify_ssl: bool = Field(default=True)

    default_status: str = Field(default="stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self
        default_status = (
            DefaultSensorStatus.RUNNING
            if cfg.default_status.lower() == "running"
            else DefaultSensorStatus.STOPPED
        )
        asset_key_obj = AssetKey(cfg.asset_key.split("/"))

        @sensor(
            name=cfg.sensor_name,
            minimum_interval_seconds=cfg.minimum_interval_seconds,
            default_status=default_status,
            required_resource_keys={cfg.oauth_token_resource_key},
        )
        def sap_cpi_observation_sensor(context: SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                return SkipReason("requests not installed")

            token_res = getattr(context.resources, cfg.oauth_token_resource_key)
            headers = {
                "Authorization": token_res.get_authorization_header(),
                "Accept": "application/json",
            }

            # Build $filter clause
            last_log_id = context.cursor or ""
            filters = []
            if cfg.iflow_id:
                filters.append(f"IntegrationFlowName eq '{cfg.iflow_id}'")
            if cfg.only_statuses:
                statuses = " or ".join(f"Status eq '{s}'" for s in cfg.only_statuses)
                filters.append(f"({statuses})")
            if last_log_id:
                # MPL IDs are monotonically increasing (timestamp-ish) — use as a cursor
                filters.append(f"MessageGuid gt '{last_log_id}'")
            filter_expr = " and ".join(filters)

            params = {
                "$format": "json",
                "$top": str(cfg.batch_size),
                "$orderby": "LogEnd asc",
            }
            if filter_expr:
                params["$filter"] = filter_expr

            url = f"{cfg.tmn_host.rstrip('/')}/api/v1/MessageProcessingLogs"

            try:
                resp = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=cfg.request_timeout_seconds,
                    verify=cfg.verify_ssl,
                )
            except Exception as e:
                context.log.error(f"CPI MPL GET failed: {e}")
                return SkipReason(f"poll error: {e}")

            if resp.status_code >= 400:
                context.log.error(f"CPI MPL HTTP {resp.status_code}: {resp.text[:300]}")
                return SkipReason(f"HTTP {resp.status_code}")

            body = resp.json()
            results = (body.get("d") or {}).get("results") or []
            if not results:
                return SkipReason("no new MPLs")

            observations = []
            new_cursor = last_log_id
            for mpl in results:
                guid = mpl.get("MessageGuid") or mpl.get("Id") or ""
                status = mpl.get("Status") or ""
                iflow_name = mpl.get("IntegrationFlowName") or ""
                log_end = mpl.get("LogEnd") or ""
                log_start = mpl.get("LogStart") or ""
                correlation_id = mpl.get("CorrelationId") or ""
                application_id = mpl.get("ApplicationMessageId") or ""

                observations.append(
                    AssetObservation(
                        asset_key=asset_key_obj,
                        metadata={
                            "sap_cpi.message_guid": MetadataValue.text(str(guid)),
                            "sap_cpi.status": MetadataValue.text(str(status)),
                            "sap_cpi.iflow": MetadataValue.text(str(iflow_name)),
                            "sap_cpi.log_start": MetadataValue.text(str(log_start)),
                            "sap_cpi.log_end": MetadataValue.text(str(log_end)),
                            "sap_cpi.correlation_id": MetadataValue.text(str(correlation_id)),
                            "sap_cpi.application_message_id": MetadataValue.text(str(application_id)),
                        },
                    )
                )
                # Track max GUID for cursor advancement (lexicographic — fine for GUID format)
                if str(guid) > new_cursor:
                    new_cursor = str(guid)

            return SensorResult(asset_events=observations, cursor=new_cursor)

        return dg.Definitions(sensors=[sap_cpi_observation_sensor])
