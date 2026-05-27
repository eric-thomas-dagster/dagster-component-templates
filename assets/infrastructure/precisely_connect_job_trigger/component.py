"""Precisely Connect ETL — trigger a job-run from Dagster.

Materializes by POSTing a run request to Precisely Connect ETL's
``/runtask`` endpoint. Pairs with ``precisely_job_sensor`` (in
``asset_event_type='observation'`` mode) for the submit-then-watch
pattern.

## Two ways to model a Precisely job in Dagster — pick ONE

| Case | Who owns the schedule | Components | What lights up materialization |
|---|---|---|---|
| **A. Observe-only** | Precisely | ``external_precisely_job`` (declare-only) + ``precisely_job_sensor`` (asset_event_type=materialization) | Sensor emits AssetMaterialization on terminal SUCCESS |
| **B. Dagster-triggered** | Dagster | ``precisely_connect_job_trigger`` (this component) + optional ``precisely_job_sensor`` (asset_event_type=observation) | Trigger emits materialization on /runtask submit; sensor emits AssetObservation when Precisely confirms terminal SUCCESS |

**Do NOT mix ``external_precisely_job`` and ``precisely_connect_job_trigger``
on the same asset_key** — they're alternatives. The trigger IS the
Precisely-job asset; the external asset is the declare-only sibling for
the other case. Same ``asset_key`` value across the components in your
chosen Case is the glue.

## /runtask response shape

Unlike the Job Status endpoint (which is the single fully-documented
public REST surface), ``/runtask`` is documented in the Connect ETL
admin REST API but the response payload shape varies by Connect ETL
version (4.x returns plain text jobRunId; 5.x returns JSON with
``{ jobRunId: ... }``). This component handles both shapes.

Validation level: **code** — YAML loads, build_defs returns an
@asset, runtime path requires a real Connect ETL server. Promote to
``live`` with a customer pilot.

Docs: https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class PreciselyConnectJobTriggerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Precisely Connect ETL job-run via the ``/runtask`` endpoint.

    Example:
        ```yaml
        type: dagster_community_components.PreciselyConnectJobTriggerComponent
        attributes:
          asset_name: precisely_load_customers_run
          job_id: "abc-123-xyz"
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
          parameters:
            INPUT_FILE: /data/customers.csv
            BATCH_DATE: "2026-05-27"
        ```

    The returned jobRunId is captured as asset metadata so a paired
    ``precisely_job_sensor`` can read it from the latest materialization
    and watch for terminal SUCCESS — closing the submit + watch loop.
    """

    asset_key: str = Field(
        description=(
            "Dagster asset key for the Precisely job (e.g. "
            "'precisely/etl/load_customers'). Use '/' separators for nested "
            "keys. Same value an ``external_precisely_job`` would use — but "
            "do NOT use both components on the same key; pick the Case "
            "(A or B in the module docstring) that matches your schedule "
            "ownership."
        ),
    )
    job_id: str = Field(
        description="Precisely Connect ETL stable job id (the job to trigger).",
    )
    host_env_var: str = Field(
        default="PRECISELY_HOST",
        description="Env var with Precisely Connect ETL host URL.",
    )
    api_token_env_var: str = Field(
        default="PRECISELY_API_TOKEN",
        description="Env var with Precisely API bearer token.",
    )
    parameters: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional Connect ETL task parameters passed as JSON body to "
            "/runtask. Shape is job-specific (defined in DMExpress / Connect "
            "ETL UI); examples include INPUT_FILE, BATCH_DATE, etc."
        ),
    )
    timeout_seconds: int = Field(
        default=60,
        ge=1,
        description="HTTP timeout for the submit POST.",
    )
    group_name: Optional[str] = Field(default="precisely", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'precisely', 'etl').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("precisely")
        kinds.add("etl")

        @dg.asset(
            key=dg.AssetKey(_self.asset_key.split("/")),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.asset_tags,
            description=_self.description or (
                f"Trigger Precisely Connect ETL job {_self.job_id}. "
                f"Pairs with precisely_job_sensor (asset_event_type=observation) "
                f"to record terminal status on the same asset."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            try:
                import requests
            except ImportError:
                raise ImportError("precisely_connect_job_trigger requires `requests`: pip install requests")

            host = os.environ.get(_self.host_env_var, "").rstrip("/")
            token = os.environ.get(_self.api_token_env_var, "")
            if not host or not token:
                raise RuntimeError(
                    f"Precisely creds missing: set {_self.host_env_var} and {_self.api_token_env_var}."
                )

            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            url = f"{host}/projects/{_self.job_id}/runtask"
            body: Dict[str, Any] = {"taskName": _self.job_id}
            if _self.parameters:
                body["parameters"] = dict(_self.parameters)

            context.log.info(f"Triggering Precisely Connect ETL job: POST {url}")
            resp = requests.post(url, headers=headers, json=body, timeout=_self.timeout_seconds)
            resp.raise_for_status()

            # 5.x returns JSON {"jobRunId": "..."}; 4.x returns plain-text jobRunId.
            text = resp.text.strip()
            job_run_id: Optional[str] = None
            try:
                body_json = resp.json()
                job_run_id = body_json.get("jobRunId") or body_json.get("runId") or body_json.get("id")
            except Exception:
                job_run_id = text or None

            if not job_run_id:
                # Don't fail the materialization — the trigger went out, the response
                # parse fell through. Customers can correlate by timestamp.
                context.log.warning(
                    f"Precisely /runtask returned no parseable jobRunId. "
                    f"Raw body: {text[:200]!r}"
                )

            metadata: Dict[str, Any] = {
                "precisely_job_id": _self.job_id,
                "precisely_host": dg.MetadataValue.url(host),
            }
            if job_run_id:
                metadata["precisely_job_run_id"] = job_run_id
            if _self.parameters:
                metadata["precisely_parameters"] = dg.MetadataValue.json(dict(_self.parameters))

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
