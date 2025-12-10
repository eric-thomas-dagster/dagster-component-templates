"""Google Cloud Functions Component.

Import Google Cloud Functions as Dagster assets for invoking serverless functions.
"""

import re
from typing import Optional, List, Dict, Any

from google.cloud import functions_v2
from google.api_core import exceptions
from google.oauth2 import service_account

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class GoogleCloudFunctionsComponent(Component, Model, Resolvable):
    """Component for importing Google Cloud Functions as Dagster assets.

    Supports importing:
    - Functions (invoke serverless functions)

    Example:
        ```yaml
        type: dagster_component_templates.GoogleCloudFunctionsComponent
        attributes:
          project_id: my-gcp-project
          location: us-central1
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          import_functions: true
        ```
    """

    project_id: str = Field(description="GCP project ID")

    location: str = Field(
        default="us-central1",
        description="GCP location/region for Cloud Functions"
    )

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional)"
    )

    import_functions: bool = Field(
        default=True,
        description="Import Cloud Functions as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    group_name: str = Field(
        default="google_cloud_functions",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Google Cloud Functions component"
    )

    def _get_client(self) -> functions_v2.FunctionServiceClient:
        """Create Cloud Functions client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return functions_v2.FunctionServiceClient(credentials=credentials)
        return functions_v2.FunctionServiceClient()

    def _matches_filters(self, name: str) -> bool:
        """Check if entity matches name filters."""
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False
        return True

    def _list_functions(self, client: functions_v2.FunctionServiceClient) -> List[Dict[str, Any]]:
        """List all Cloud Functions."""
        functions = []
        parent = f"projects/{self.project_id}/locations/{self.location}"

        try:
            for function in client.list_functions(parent=parent):
                function_name = function.name.split("/")[-1]
                if self._matches_filters(function_name):
                    functions.append({
                        "name": function_name,
                        "full_name": function.name,
                        "url": function.service_config.uri if hasattr(function, 'service_config') else None,
                    })
        except exceptions.GoogleAPICallError:
            pass

        return functions

    def _get_function_assets(self, client: functions_v2.FunctionServiceClient) -> List:
        """Generate Cloud Function assets."""
        assets = []
        functions = self._list_functions(client)

        for func_info in functions:
            func_name = func_info["name"]
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', func_name)
            asset_key = f"cloud_function_{safe_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "function_name": func_name,
                    "project": self.project_id,
                    "location": self.location,
                    "url": func_info.get("url"),
                },
            )
            def function_asset(context: AssetExecutionContext, func_info=func_info):
                """Invoke Cloud Function."""
                # Note: Actual invocation would require HTTP client or Cloud Functions invoke API
                # This is a template showing the structure

                metadata = {
                    "function_name": func_info["name"],
                    "function_url": func_info.get("url", "N/A"),
                    "note": "Template function - implement function invocation with HTTP client or invoke API"
                }

                context.log.info(f"Cloud Function: {func_info['name']}")

                return metadata

            assets.append(function_asset)

        return assets

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        assets = []

        if self.import_functions:
            client = self._get_client()
            assets.extend(self._get_function_assets(client))

        return Definitions(assets=assets)
