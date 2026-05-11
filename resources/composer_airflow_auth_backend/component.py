"""ComposerAirflowAuthBackendComponent — dagster-airlift auth backend for Cloud Composer.

Cloud Composer's Airflow REST API requires an OIDC ID token with the
environment's airflow URL as the `target_audience`. dagster-airlift ships
`AirflowBasicAuthBackend` (OSS Airflow) and `MwaaSessionAuthBackend` (AWS
MWAA) but no Composer backend. This component plugs that gap.

Usage:
  Drop the resource into your Definitions, hand it to AirflowInstance,
  then use dagster-airlift's standard APIs:

    from dagster_airlift.core import AirflowInstance, build_defs_from_airflow_instance

    af = AirflowInstance(
        name="composer-prod",
        auth_backend=composer_backend,   # provided by this component
    )

    defs = build_defs_from_airflow_instance(airflow_instance=af)

  Or call af.trigger_dag(dag_id="...") / af.wait_for_run_completion(...).

Reusing dagster-airlift means you get DAG observation, sensor-based run
tracking, and migration tooling for free — instead of duplicating it.
"""

import json
import os
from typing import Any, Dict, Optional

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from pydantic import Field


class ComposerAirflowAuthBackendComponent(Component, Model, Resolvable):
    """Provide a dagster-airlift AirflowAuthBackend backed by GCP OIDC for Cloud Composer."""

    resource_key: str = Field(
        default="composer_airflow_auth",
        description="Key under which the auth backend is exposed in Definitions.resources.",
    )

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    airflow_url: str = Field(
        description=(
            "Composer environment's Airflow URL — e.g. "
            "'https://abc123-dot-us-central1.composer.googleusercontent.com'. "
            "Look up via "
            "`gcloud composer environments describe <env> --location=<region> "
            "--format='value(config.airflowUri)'`."
        ),
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        airflow_url = self.airflow_url.rstrip("/")
        resource_key = self.resource_key

        try:
            from dagster_airlift.core import AirflowAuthBackend
        except ImportError:
            raise ImportError(
                "pip install dagster-airlift — this component plugs into dagster-airlift's "
                "AirflowAuthBackend system."
            )

        class _ComposerAirflowAuthBackend(AirflowAuthBackend):
            def __init__(self, creds_info: Dict[str, Any], webserver_url: str):
                self._creds_info = creds_info
                self._webserver_url = webserver_url

            def get_session(self):
                from google.auth.transport.requests import AuthorizedSession
                from google.oauth2 import service_account
                creds = service_account.IDTokenCredentials.from_service_account_info(
                    self._creds_info, target_audience=self._webserver_url,
                )
                return AuthorizedSession(creds)

            def get_webserver_url(self) -> str:
                return self._webserver_url

        backend = _ComposerAirflowAuthBackend(creds_dict, airflow_url)
        return Definitions(resources={resource_key: backend})
