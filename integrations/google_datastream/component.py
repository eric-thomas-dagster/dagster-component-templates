"""Google Cloud Datastream Component.

Import Google Cloud Datastream streams and connection profiles as Dagster assets
for monitoring CDC replication from databases to BigQuery and Cloud Storage.
"""

import re
from typing import Optional, List, Dict, Any

from google.cloud import datastream_v1
from google.api_core import exceptions
from google.oauth2 import service_account

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


class GoogleDatastreamComponent(Component, Model, Resolvable):
    """Component for importing Google Cloud Datastream entities as Dagster assets.

    Supports importing:
    - Streams (observe CDC replication status and lag)
    - Connection Profiles (observe source/destination configurations)

    Example:
        ```yaml
        type: dagster_component_templates.GoogleDatastreamComponent
        attributes:
          project_id: my-gcp-project
          location: us-central1
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          import_streams: true
          import_connection_profiles: true
        ```
    """

    project_id: str = Field(
        description="GCP project ID"
    )

    location: str = Field(
        default="us-central1",
        description="GCP location/region for Datastream resources"
    )

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional if using default credentials)"
    )

    import_streams: bool = Field(
        default=True,
        description="Import Datastream streams as observable assets"
    )

    import_connection_profiles: bool = Field(
        default=False,
        description="Import connection profiles as observable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for streams"
    )

    group_name: str = Field(
        default="google_datastream",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Google Datastream component"
    )

    def _get_client(self) -> datastream_v1.DatastreamClient:
        """Create Datastream client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return datastream_v1.DatastreamClient(credentials=credentials)
        else:
            return datastream_v1.DatastreamClient()

    def _matches_filters(self, name: str) -> bool:
        """Check if entity matches name filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        return True

    def _list_streams(self, client: datastream_v1.DatastreamClient) -> List[Dict[str, Any]]:
        """List all Datastream streams."""
        streams = []
        parent = f"projects/{self.project_id}/locations/{self.location}"

        try:
            request = datastream_v1.ListStreamsRequest(parent=parent)
            page_result = client.list_streams(request=request)

            for stream in page_result:
                stream_name = stream.name.split("/")[-1]

                if self._matches_filters(stream_name):
                    streams.append({
                        "name": stream_name,
                        "full_name": stream.name,
                        "state": stream.state.name,
                        "source": stream.source_config.source_connection_profile,
                        "destination": stream.destination_config.destination_connection_profile,
                        "display_name": stream.display_name,
                    })

        except exceptions.GoogleAPICallError as e:
            raise Exception(f"Failed to list Datastream streams: {e}")

        return streams

    def _list_connection_profiles(self, client: datastream_v1.DatastreamClient) -> List[Dict[str, Any]]:
        """List all connection profiles."""
        profiles = []
        parent = f"projects/{self.project_id}/locations/{self.location}"

        try:
            request = datastream_v1.ListConnectionProfilesRequest(parent=parent)
            page_result = client.list_connection_profiles(request=request)

            for profile in page_result:
                profile_name = profile.name.split("/")[-1]

                if self._matches_filters(profile_name):
                    # Determine profile type
                    profile_type = "UNKNOWN"
                    if profile.mysql_profile:
                        profile_type = "MySQL"
                    elif profile.postgresql_profile:
                        profile_type = "PostgreSQL"
                    elif profile.oracle_profile:
                        profile_type = "Oracle"
                    elif profile.gcs_profile:
                        profile_type = "Cloud Storage"
                    elif profile.bigquery_profile:
                        profile_type = "BigQuery"

                    profiles.append({
                        "name": profile_name,
                        "full_name": profile.name,
                        "type": profile_type,
                        "display_name": profile.display_name,
                    })

        except exceptions.GoogleAPICallError as e:
            raise Exception(f"Failed to list connection profiles: {e}")

        return profiles

    def _get_stream_assets(self, client: datastream_v1.DatastreamClient) -> List:
        """Generate stream observable assets."""
        assets = []
        streams = self._list_streams(client)

        for stream_info in streams:
            stream_name = stream_info["name"]
            asset_key = f"datastream_{stream_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "stream_name": stream_name,
                    "state": stream_info["state"],
                    "source": stream_info["source"],
                    "destination": stream_info["destination"],
                },
            )
            def stream_asset(context: AssetExecutionContext, stream_info=stream_info):
                """Observe Datastream CDC stream status."""
                client = self._get_client()

                try:
                    # Get stream details
                    stream = client.get_stream(name=stream_info["full_name"])

                    metadata = {
                        "stream_name": stream_info["name"],
                        "display_name": stream_info["display_name"],
                        "state": stream.state.name,
                        "source_type": stream_info["source"].split("/")[-1],
                        "destination_type": stream_info["destination"].split("/")[-1],
                        "backfill_all": stream.backfill_all.name if stream.backfill_all else "NONE",
                    }

                    # Get error information if stream has errors
                    if stream.errors:
                        error_messages = [error.message for error in stream.errors]
                        metadata["errors"] = ", ".join(error_messages[:3])  # First 3 errors
                        context.log.warning(f"Stream has errors: {error_messages}")
                    else:
                        context.log.info(f"Stream state: {stream.state.name}")

                    return metadata

                except exceptions.GoogleAPICallError as e:
                    context.log.error(f"Failed to get stream details: {e}")
                    raise

            assets.append(stream_asset)

        return assets

    def _get_connection_profile_assets(self, client: datastream_v1.DatastreamClient) -> List:
        """Generate connection profile observable assets."""
        assets = []
        profiles = self._list_connection_profiles(client)

        for profile_info in profiles:
            profile_name = profile_info["name"]
            asset_key = f"connection_profile_{profile_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "profile_name": profile_name,
                    "profile_type": profile_info["type"],
                },
            )
            def profile_asset(context: AssetExecutionContext, profile_info=profile_info):
                """Observe Datastream connection profile configuration."""
                client = self._get_client()

                try:
                    # Get profile details
                    profile = client.get_connection_profile(name=profile_info["full_name"])

                    metadata = {
                        "profile_name": profile_info["name"],
                        "display_name": profile_info["display_name"],
                        "profile_type": profile_info["type"],
                    }

                    # Add type-specific details
                    if profile.mysql_profile:
                        metadata["hostname"] = profile.mysql_profile.hostname
                        metadata["port"] = profile.mysql_profile.port
                    elif profile.postgresql_profile:
                        metadata["hostname"] = profile.postgresql_profile.hostname
                        metadata["port"] = profile.postgresql_profile.port
                    elif profile.oracle_profile:
                        metadata["hostname"] = profile.oracle_profile.hostname
                        metadata["port"] = profile.oracle_profile.port
                    elif profile.gcs_profile:
                        metadata["bucket"] = profile.gcs_profile.bucket
                    elif profile.bigquery_profile:
                        metadata["note"] = "BigQuery destination profile"

                    context.log.info(f"Connection profile type: {profile_info['type']}")

                    return metadata

                except exceptions.GoogleAPICallError as e:
                    context.log.error(f"Failed to get connection profile: {e}")
                    raise

            assets.append(profile_asset)

        return assets

    def _get_observation_sensor(self, client: datastream_v1.DatastreamClient):
        """Generate sensor to observe Datastream streams."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def datastream_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe Google Cloud Datastream streams."""

            if not self.import_streams:
                return

            # Get all streams
            streams = self._list_streams(client)

            for stream_info in streams:
                stream_name = stream_info["name"]

                try:
                    # Get stream details
                    stream = client.get_stream(name=stream_info["full_name"])

                    # Emit materialization for running streams
                    if stream.state.name in ["RUNNING", "PAUSED", "FAILED"]:
                        asset_key = f"datastream_{stream_name}"

                        metadata = {
                            "stream_name": MetadataValue.text(stream_name),
                            "state": MetadataValue.text(stream.state.name),
                            "display_name": MetadataValue.text(stream_info["display_name"]),
                        }

                        # Add error count if present
                        if stream.errors:
                            metadata["error_count"] = MetadataValue.int(len(stream.errors))

                        yield AssetMaterialization(
                            asset_key=asset_key,
                            metadata=metadata,
                        )

                except exceptions.GoogleAPICallError as e:
                    context.log.warning(f"Failed to describe stream {stream_name}: {e}")

        return datastream_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        client = self._get_client()

        assets = []
        sensors = []

        # Import streams
        if self.import_streams:
            assets.extend(self._get_stream_assets(client))

        # Import connection profiles
        if self.import_connection_profiles:
            assets.extend(self._get_connection_profile_assets(client))

        # Generate observation sensor
        if self.generate_sensor and self.import_streams:
            sensors.append(self._get_observation_sensor(client))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
