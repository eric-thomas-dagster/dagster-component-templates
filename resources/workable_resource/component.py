"""Workable Resource component.

Bearer-token wrapper over the Workable SPI v3 REST API with convenience
methods for the candidate-sourcing/reverse-ETL flow (search by email,
create in a job pipeline or the account-wide talent pool, update, and
advance pipeline stage).

Base URL: https://{subdomain}.workable.com/spi/v3
Auth header: Authorization: Bearer <api_token>

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`.
"""
import os

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class WorkableResource(ConfigurableResource):
    """Dagster resource wrapping the Workable SPI v3 REST API."""

    api_token_env_var: str = Field(description="Env var holding the Workable API token.")
    subdomain: str = Field(description="Workable subdomain (e.g. 'your-company' from your-company.workable.com).")

    @property
    def base_url(self) -> str:
        return f"https://{self.subdomain}.workable.com/spi/v3"

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests

        token = os.environ.get(self.api_token_env_var)
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        return session

    def find_candidate_by_email(self, email: str) -> list:
        """GET /candidates?email={email} -- exact-match candidate filter."""
        resp = self.get_client().get(f"{self.base_url}/candidates", params={"email": email})
        resp.raise_for_status()
        return resp.json().get("candidates", [])

    def create_job_candidate(self, shortcode: str, body: dict) -> dict:
        """POST /jobs/{shortcode}/candidates -- create a candidate attached to a specific job."""
        resp = self.get_client().post(f"{self.base_url}/jobs/{shortcode}/candidates", json=body)
        resp.raise_for_status()
        return resp.json().get("candidate", {})

    def create_talent_pool_candidate(self, body: dict) -> dict:
        """POST /talent_pool/candidates -- create a candidate in the account-wide talent pool (no job)."""
        resp = self.get_client().post(f"{self.base_url}/talent_pool/candidates", json=body)
        resp.raise_for_status()
        return resp.json().get("candidate", {})

    def update_candidate(self, candidate_id: str, body: dict) -> dict:
        """PATCH /candidates/{id} -- partial update of an existing candidate's fields."""
        resp = self.get_client().patch(f"{self.base_url}/candidates/{candidate_id}", json=body)
        resp.raise_for_status()
        return resp.json()

    def move_candidate(self, candidate_id: str, member_id: str, target_stage: str) -> dict:
        """POST /candidates/{id}/move -- advance a candidate to another pipeline stage.

        `member_id` identifies the acting Workable account member and is
        REQUIRED by this endpoint -- not optional.
        """
        resp = self.get_client().post(
            f"{self.base_url}/candidates/{candidate_id}/move",
            json={"member_id": member_id, "target_stage": target_stage},
        )
        resp.raise_for_status()
        return resp.json()


class WorkableResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a WorkableResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.WorkableResourceComponent
        attributes:
          resource_key: workable_resource
          api_token_env_var: WORKABLE_API_TOKEN
          subdomain: my-company
        ```
    """

    resource_key: str = Field(
        default="workable_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_token_env_var: str = Field(
        description="Env var holding the Workable API token.",
    )
    subdomain: str = Field(
        description="Workable subdomain (e.g. 'your-company' from your-company.workable.com).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = WorkableResource(
            api_token_env_var=self.api_token_env_var,
            subdomain=self.subdomain,
        )
        return dg.Definitions(resources={self.resource_key: resource})
