"""New Relic Resource.

Wraps New Relic's REST + GraphQL APIs (NerdGraph) for use from custom
ops. New Relic uses regional endpoints (US, EU) and an Account ID.

Companion components:
- `dataframe_to_newrelic_logs` (sink) — push log events
- `newrelic_nrql_query` (source) — query NRQL → DataFrame
"""

import os
from typing import Optional
import dagster as dg
from pydantic import Field


class NewRelicResource(dg.ConfigurableResource):
    """New Relic API client wrapper."""

    api_key_env_var: str = Field(description="Env var holding the New Relic User API key")
    account_id: str = Field(description="New Relic Account ID")
    region: str = Field(
        default="US",
        description="'US' (default) or 'EU' — selects api.newrelic.com vs api.eu.newrelic.com",
    )

    @property
    def base_url(self) -> str:
        return "https://api.newrelic.com" if self.region.upper() == "US" else "https://api.eu.newrelic.com"

    @property
    def graphql_url(self) -> str:
        return "https://api.newrelic.com/graphql" if self.region.upper() == "US" else "https://api.eu.newrelic.com/graphql"

    @property
    def logs_url(self) -> str:
        return "https://log-api.newrelic.com/log/v1" if self.region.upper() == "US" else "https://log-api.eu.newrelic.com/log/v1"

    @property
    def metrics_url(self) -> str:
        return "https://metric-api.newrelic.com/metric/v1" if self.region.upper() == "US" else "https://metric-api.eu.newrelic.com/metric/v1"

    @property
    def events_url(self) -> str:
        return f"https://insights-collector.newrelic.com/v1/accounts/{self.account_id}/events" if self.region.upper() == "US" else f"https://insights-collector.eu01.nr-data.net/v1/accounts/{self.account_id}/events"

    def _api_key(self) -> str:
        key = os.environ.get(self.api_key_env_var)
        if not key:
            raise RuntimeError(f"Missing {self.api_key_env_var} environment variable")
        return key

    def nrql(self, query: str) -> dict:
        """Run an NRQL query via NerdGraph and return the JSON response."""
        import requests
        gql = """query($accountId: Int!, $query: Nrql!) {
            actor { account(id: $accountId) { nrql(query: $query) { results } } }
        }"""
        resp = requests.post(
            self.graphql_url,
            json={"query": gql, "variables": {"accountId": int(self.account_id), "query": query}},
            headers={"API-Key": self._api_key()},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class NewRelicResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a New Relic resource."""

    resource_key: str = Field(default="newrelic", description="Dagster resource key")
    api_key_env_var: str = Field(description="Env var holding the User API key")
    account_id: str = Field(description="New Relic Account ID")
    region: str = Field(default="US", description="'US' or 'EU'")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: NewRelicResource(
            api_key_env_var=self.api_key_env_var,
            account_id=self.account_id,
            region=self.region,
        )})
