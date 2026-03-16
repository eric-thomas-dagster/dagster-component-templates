"""Google Analytics Resource component."""
from dataclasses import dataclass
import json
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class GoogleAnalyticsResource(ConfigurableResource):
    """Dagster resource wrapping the Google Analytics Data API client."""

    property_id: str = Field(description="GA4 property ID e.g. '123456789'")
    gcp_credentials_env_var: str = Field(
        description="Env var holding GCP service account JSON string"
    )

    def get_client(self):
        import json
        import os
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.oauth2.service_account import Credentials

        credentials_info = json.loads(
            os.environ.get(self.gcp_credentials_env_var, "{}")
        )
        credentials = Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        return BetaAnalyticsDataClient(credentials=credentials)


@dataclass
class GoogleAnalyticsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GoogleAnalyticsResource for use by other components."""

    resource_key: str = Field(
        default="google_analytics_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    property_id: str = Field(
        description="GA4 property ID e.g. '123456789'",
    )
    gcp_credentials_env_var: str = Field(
        description="Env var holding GCP service account JSON string",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GoogleAnalyticsResource(
            property_id=self.property_id,
            gcp_credentials_env_var=self.gcp_credentials_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
