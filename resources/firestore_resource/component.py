import json
import os
from dataclasses import dataclass

import dagster as dg
from dagster import ConfigurableResource
from google.cloud import firestore
from google.oauth2.service_account import Credentials


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


@dataclass
class FirestoreResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a Google Cloud Firestore resource."""

    resource_key: str = "firestore_resource"
    project: str = ""
    gcp_credentials_env_var: str = ""
    database: str = "(default)"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        gcp_credentials = (
            os.environ.get(self.gcp_credentials_env_var, "")
            if self.gcp_credentials_env_var
            else ""
        )
        resource = FirestoreResource(
            project=self.project,
            gcp_credentials=gcp_credentials,
            database=self.database,
        )
        return dg.Definitions(resources={self.resource_key: resource})
