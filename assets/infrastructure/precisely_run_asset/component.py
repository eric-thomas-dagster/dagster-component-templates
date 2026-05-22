"""Precisely Connect ETL run-observation asset.

Surfaces the status of a Precisely Connect ETL job run as an
``observable_source_asset`` so the run state shows up in the Dagster
catalog and downstream assets can depend on it.

**Observation-only — Precisely owns the run.** Precisely Connect ETL
publishes a Job Status REST endpoint
(``GET {host}/projects/{jobRunId}/status``) but does NOT publish a
submit / trigger endpoint. Earlier versions of this component tried to
``POST`` to a best-guess ``/projects/{job_id}/run`` path — that wasn't a
real Precisely API and would 404 in every customer install. The submit
half has been removed; this component now only **observes** runs that
Precisely's own scheduler kicks off.

If you also want to fire downstream Dagster work the moment a Precisely
run finishes, pair this asset with the ``precisely_job_sensor``
component (sensor watches the same status endpoint and emits
``RunRequest`` on terminal SUCCESS).

Precisely Connect ETL Job Status enum (returned as plain text):
    WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
    COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT

Docs: https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference/Job-Status
"""
from typing import Dict, List, Optional

import dagster as dg
from dagster import ConfigurableResource, ObserveResult, observable_source_asset
from pydantic import Field


PRECISELY_TERMINAL_SUCCESS = {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
PRECISELY_TERMINAL_FAIL = {"COMPLETED_WITH_ERRORS", "CANCELLED", "ERRORED", "LOST_CONTACT"}
PRECISELY_TERMINAL = PRECISELY_TERMINAL_SUCCESS | PRECISELY_TERMINAL_FAIL
PRECISELY_IN_PROGRESS = {"WAITING", "RUNNING"}


class PreciselyResource(ConfigurableResource):
    """Resource for reading status from the Precisely Connect ETL REST API.

    Only the verified ``GET /projects/{jobRunId}/status`` endpoint is
    exercised. There is no ``run_job`` method by design — Precisely does
    not publish a submit endpoint, so the component never pretends to
    trigger runs.

    Example:
        ```python
        PreciselyResource(
            host=EnvVar("PRECISELY_HOST"),
            api_token=EnvVar("PRECISELY_API_TOKEN"),
        )
        ```
    """

    host: str = Field(description="Precisely Connect ETL host URL (e.g. https://precisely.mycompany.com)")
    api_token: str = Field(description="Precisely API token (Bearer)")

    def _base(self) -> str:
        return self.host.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}", "Accept": "application/json"}

    def get_run_status(self, job_run_id: str) -> str:
        """Fetch the current status of a Connect ETL job run.

        Returns one of: WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
        COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT.

        Per Precisely Connect ETL docs, this endpoint returns a plain-text
        status string, not JSON.
        """
        import requests
        resp = requests.get(
            f"{self._base()}/projects/{job_run_id}/status",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text.strip().upper()


class PreciselyRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Observe a Precisely Connect ETL run as a Dagster source asset.

    The asset is registered as an ``observable_source_asset``: every time
    Dagster polls it (manually or via a schedule), it fetches the latest
    status from the documented Precisely Job Status endpoint and emits an
    ``ObserveResult`` with the status string. Downstream Dagster assets
    can depend on this asset to surface Precisely lineage in the catalog.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.PreciselyRunAssetComponent
        attributes:
          asset_key: precisely/etl/load_customers
          job_run_id: "abc-123-def-456"           # the Precisely run-id to watch
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.PreciselyRunAssetComponent
        attributes:
          asset_key: precisely/etl/load_customers
          job_run_id: "abc-123-def-456"
          resource_key: precisely
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'precisely/etl/load_customers')")
    job_run_id: str = Field(
        description=(
            "Precisely Connect ETL job-run ID to observe. Use the run-id "
            "from your Precisely-side scheduler / UI."
        ),
    )
    host_env_var: Optional[str] = Field(default=None, description="Env var with Precisely Connect host URL")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with Precisely API token")
    resource_key: Optional[str] = Field(default=None, description="Key of a PreciselyResource")
    group_name: Optional[str] = Field(default="precisely", description="Dagster asset group name")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on")

    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['precisely']). Auto-inferred when unset.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else {"precisely"}

        @observable_source_asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"Precisely Connect ETL run {self.job_run_id} (observed)",
            group_name=self.group_name,
            kinds=kinds,
            required_resource_keys={self.resource_key} if self.resource_key else set(),
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def precisely_run_asset(context) -> ObserveResult:
            import os
            import requests

            if _self.resource_key:
                resource: PreciselyResource = getattr(context.resources, _self.resource_key)
                status = resource.get_run_status(_self.job_run_id)
                base = resource._base()
            else:
                host = os.environ.get(_self.host_env_var or "", "").rstrip("/")
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = host
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                resp = requests.get(
                    f"{base}/projects/{_self.job_run_id}/status",
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                status = resp.text.strip().upper()

            context.log.info(f"Precisely run {_self.job_run_id} status: {status}")

            if status in PRECISELY_TERMINAL_FAIL:
                context.log.warning(
                    f"Precisely run {_self.job_run_id} ended in failure state '{status}'."
                )

            return ObserveResult(
                metadata={
                    "job_run_id": _self.job_run_id,
                    "status": status,
                    "is_terminal": status in PRECISELY_TERMINAL,
                    "is_success": status in PRECISELY_TERMINAL_SUCCESS,
                    "is_in_progress": status in PRECISELY_IN_PROGRESS,
                    "precisely_host": base,
                }
            )

        return dg.Definitions(assets=[precisely_run_asset])
