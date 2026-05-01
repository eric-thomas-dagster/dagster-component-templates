"""Google Cloud Firestore Resource component."""
import json
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from google.cloud import firestore
from google.oauth2.service_account import Credentials
from pydantic import Field


class FirestoreResource(ConfigurableResource):
    project: str
    gcp_credentials: str = ""
    database: str = "(default)"

    def get_client(self) -> firestore.Client:
        if self.gcp_credentials:
            creds = Credentials.from_service_account_info(
                json.loads(self.gcp_credentials),
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            return firestore.Client(
                project=self.project,
                credentials=creds,
                database=self.database,
            )
        return firestore.Client(project=self.project, database=self.database)


class FirestoreResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Google Cloud Firestore resource for use by other components."""

    resource_key: str = Field(default="firestore_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    project: str = Field(description="GCP project ID")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Environment variable holding service account JSON. Leave empty to use Application Default Credentials.")
    database: str = Field(default="(default)", description="Firestore database ID. '(default)' targets the default database.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        gcp_credentials = (
            dg.EnvVar(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else ""
        )
        resource = FirestoreResource(
            project=self.project,
            gcp_credentials=gcp_credentials,
            database=self.database,
        )
        return dg.Definitions(resources={self.resource_key: resource})
