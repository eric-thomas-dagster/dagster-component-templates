"""SharePoint Monitor Sensor Component.

Monitors a SharePoint document library for new or modified files
and triggers a job when changes are detected.
Requires an Azure AD app registration with Sites.Read.All permission.

resource_key, when set, genuinely drives the poll via a registered
SharePointResourceComponent's own list_library_items(...) method (the
same Graph API calls duplicated below) -- previously this field was
declared and wired into required_resource_keys but never actually read,
so setting it silently had no effect and the sensor always ran its own
inline OAuth + Graph calls from tenant_id_env_var/client_id_env_var/
client_secret_env_var regardless.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field, model_validator


class SharePointMonitorSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Monitor a SharePoint document library for new or modified files.

    Example:
        ```yaml
        type: dagster_community_components.SharePointMonitorSensorComponent
        attributes:
          sensor_name: sharepoint_reports_monitor
          site_url: https://myorg.sharepoint.com/sites/DataTeam
          library_name: Reports
          tenant_id_env_var: AZURE_TENANT_ID
          client_id_env_var: AZURE_CLIENT_ID
          client_secret_env_var: AZURE_CLIENT_SECRET
          job_name: process_sharepoint_file_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    site_url: str = Field(description="SharePoint site URL")
    library_name: str = Field(description="Document library name (e.g. 'Documents' or 'Reports')")
    tenant_id_env_var: Optional[str] = Field(default=None, description="Env var with Azure tenant ID. Required unless resource_key is set.")
    client_id_env_var: Optional[str] = Field(default=None, description="Env var with Azure app client ID. Required unless resource_key is set.")
    client_secret_env_var: Optional[str] = Field(default=None, description="Env var with Azure app client secret. Required unless resource_key is set.")
    folder_path: str = Field(default="", description="Optional subfolder path within the library")
    file_extension_filter: str = Field(default="", description="Filter by extension e.g. '.xlsx' (empty = all files)")
    job_name: str = Field(description="Job to trigger when new/modified files are detected")
    minimum_interval_seconds: int = Field(default=300, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional resource key registered by a SharePointResourceComponent. When set, polls "
            "run via that resource's own list_library_items(...) method instead of the inline "
            "tenant_id_env_var/client_id_env_var/client_secret_env_var below."
        ),
    )

    @model_validator(mode="after")
    def _check_credentials(self):
        if not self.resource_key and not (self.tenant_id_env_var and self.client_id_env_var and self.client_secret_env_var):
            raise ValueError(
                "SharePointMonitorSensorComponent: supply resource_key (a registered "
                "SharePointResourceComponent) OR all of tenant_id_env_var/client_id_env_var/client_secret_env_var."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Share Point Monitor Sensor', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def sharepoint_sensor(context: SensorEvaluationContext, **_resources):
            cursor = context.cursor or "1970-01-01T00:00:00Z"

            if resource_key:
                _res = getattr(context.resources, resource_key, None)
                if _res is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    items = _res.list_library_items(_self.site_url, _self.library_name, _self.folder_path, top=200)
                except Exception as e:
                    return SensorResult(skip_reason=f"SharePoint error (via resource '{resource_key}'): {e}")
            else:
                import os
                try:
                    import requests
                except ImportError:
                    return SensorResult(skip_reason="requests not installed")

                tenant_id = os.environ.get(_self.tenant_id_env_var, "")
                client_id = os.environ.get(_self.client_id_env_var, "")
                client_secret = os.environ.get(_self.client_secret_env_var, "")

                # Get OAuth token via client credentials
                try:
                    token_resp = requests.post(
                        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
                        data={"grant_type": "client_credentials", "client_id": client_id,
                              "client_secret": client_secret,
                              "scope": "https://graph.microsoft.com/.default"},
                        timeout=30,
                    )
                    token_resp.raise_for_status()
                    access_token = token_resp.json()["access_token"]
                except Exception as e:
                    return SensorResult(skip_reason=f"Auth failed: {e}")

                headers = {"Authorization": f"Bearer {access_token}"}

                # Resolve site ID from URL
                try:
                    hostname = _self.site_url.split("/")[2]
                    site_path = "/" + "/".join(_self.site_url.split("/")[3:])
                    site_resp = requests.get(
                        f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
                        headers=headers, timeout=30,
                    )
                    site_resp.raise_for_status()
                    site_id = site_resp.json()["id"]
                except Exception as e:
                    return SensorResult(skip_reason=f"Could not resolve site ID: {e}")

                # List items in the library
                try:
                    drive_resp = requests.get(
                        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
                        headers=headers, timeout=30,
                    )
                    drive_resp.raise_for_status()
                    drives = drive_resp.json().get("value", [])
                    drive = next((d for d in drives if d["name"] == _self.library_name), None)
                    if not drive:
                        return SensorResult(skip_reason=f"Library '{_self.library_name}' not found")
                    drive_id = drive["id"]

                    folder = _self.folder_path.strip("/")
                    path_segment = f":/{folder}:" if folder else ""
                    items_resp = requests.get(
                        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root{path_segment}/children",
                        headers=headers, params={"$top": 200}, timeout=30,
                    )
                    items_resp.raise_for_status()
                    items = items_resp.json().get("value", [])
                except Exception as e:
                    return SensorResult(skip_reason=f"List files failed: {e}")

            run_requests = []
            latest = cursor
            for item in items:
                if "folder" in item:
                    continue  # skip subfolders
                modified = item.get("lastModifiedDateTime", "")
                name = item.get("name", "")
                if _self.file_extension_filter and not name.endswith(_self.file_extension_filter):
                    continue
                if modified > cursor:
                    run_requests.append(RunRequest(
                        run_key=f"{item['id']}-{modified}",
                        run_config={"ops": {"config": {
                            "site_url": _self.site_url,
                            "library_name": _self.library_name,
                            "file_name": name,
                            "file_id": item["id"],
                            "web_url": item.get("webUrl", ""),
                            "last_modified": modified,
                            "size": item.get("size", 0),
                        }}},
                    ))
                    if modified > latest:
                        latest = modified

            if run_requests:
                return SensorResult(run_requests=run_requests, cursor=latest)
            return SensorResult(skip_reason="No new or modified files")

        return dg.Definitions(sensors=[sharepoint_sensor])
