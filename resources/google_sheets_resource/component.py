"""Google Sheets Resource component."""
from dataclasses import dataclass
from typing import Optional
import json
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class GoogleSheetsResource(ConfigurableResource):
    """Dagster resource wrapping the gspread client."""

    gcp_credentials_env_var: str = Field(
        description="Env var holding GCP service account JSON string"
    )
    scopes: str = Field(
        default="https://www.googleapis.com/auth/spreadsheets",
        description="OAuth scopes for the Google Sheets API",
    )

    def get_client(self):
        import gspread
        credentials_json = json.loads(os.environ.get(self.gcp_credentials_env_var, "{}"))
        return gspread.service_account_from_dict(credentials_json)


@dataclass
class GoogleSheetsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GoogleSheetsResource for use by other components."""

    resource_key: str = Field(
        default="google_sheets_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    gcp_credentials_env_var: str = Field(
        description="Env var holding GCP service account JSON string",
    )
    scopes: Optional[str] = Field(
        default="https://www.googleapis.com/auth/spreadsheets",
        description="OAuth scopes for the Google Sheets API",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GoogleSheetsResource(
            gcp_credentials_env_var=self.gcp_credentials_env_var,
            scopes=self.scopes or "https://www.googleapis.com/auth/spreadsheets",
        )
        return dg.Definitions(resources={self.resource_key: resource})
