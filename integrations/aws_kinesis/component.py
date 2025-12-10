"""AWS Kinesis Component.

Import AWS Kinesis Firehose delivery streams and Data Analytics applications
as Dagster assets for orchestrating real-time data delivery and analytics.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

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


class AWSKinesisComponent(Component, Model, Resolvable):
    """Component for importing AWS Kinesis entities as Dagster assets.

    Supports importing:
    - Firehose Delivery Streams (trigger data delivery)
    - Data Analytics Applications (start analytics processing)

    Example:
        ```yaml
        type: dagster_component_templates.AWSKinesisComponent
        attributes:
          aws_region: us-east-1
          aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
          aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
          import_firehose_streams: true
          import_analytics_applications: true
        ```
    """

    aws_region: str = Field(
        description="AWS region"
    )

    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID (optional if using IAM role)"
    )

    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key (optional if using IAM role)"
    )

    aws_session_token: Optional[str] = Field(
        default=None,
        description="AWS session token for temporary credentials (optional)"
    )

    import_firehose_streams: bool = Field(
        default=True,
        description="Import Firehose delivery streams as materializable assets"
    )

    import_analytics_applications: bool = Field(
        default=True,
        description="Import Data Analytics applications as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    filter_by_tags: Optional[str] = Field(
        default=None,
        description="Comma-separated tag keys to filter entities (e.g., 'env,team')"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for delivery streams and applications"
    )

    group_name: str = Field(
        default="aws_kinesis",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the AWS Kinesis component"
    )

    def _get_boto3_session(self) -> boto3.Session:
        """Create boto3 session with credentials."""
        session_kwargs = {"region_name": self.aws_region}

        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        if self.aws_session_token:
            session_kwargs["aws_session_token"] = self.aws_session_token

        return boto3.Session(**session_kwargs)

    def _matches_filters(self, name: str, tags: Optional[Dict[str, str]] = None) -> bool:
        """Check if entity matches name and tag filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        # Tag filter
        if self.filter_by_tags and tags:
            required_keys = [k.strip() for k in self.filter_by_tags.split(",")]
            if not all(key in tags for key in required_keys):
                return False

        return True

    def _list_firehose_streams(self, session: boto3.Session) -> List[str]:
        """List all Firehose delivery streams."""
        firehose = session.client("firehose")
        streams = []

        try:
            paginator = firehose.get_paginator("list_delivery_streams")
            for page in paginator.paginate():
                for stream_name in page["DeliveryStreamNames"]:
                    if self._matches_filters(stream_name):
                        streams.append(stream_name)
        except ClientError as e:
            raise Exception(f"Failed to list Firehose delivery streams: {e}")

        return streams

    def _list_analytics_applications(self, session: boto3.Session) -> List[Dict[str, Any]]:
        """List all Kinesis Data Analytics applications."""
        analytics = session.client("kinesisanalyticsv2")
        applications = []

        try:
            paginator = analytics.get_paginator("list_applications")
            for page in paginator.paginate():
                for app in page["ApplicationSummaries"]:
                    app_name = app["ApplicationName"]
                    if self._matches_filters(app_name):
                        applications.append({
                            "name": app_name,
                            "arn": app["ApplicationARN"],
                            "runtime": app.get("RuntimeEnvironment", "UNKNOWN")
                        })
        except ClientError as e:
            raise Exception(f"Failed to list Kinesis Analytics applications: {e}")

        return applications

    def _get_firehose_assets(self, session: boto3.Session) -> List:
        """Generate Firehose delivery stream assets."""
        assets = []
        streams = self._list_firehose_streams(session)

        for stream_name in streams:
            asset_key = f"firehose_stream_{stream_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "stream_name": stream_name,
                    "aws_region": self.aws_region,
                },
            )
            def firehose_asset(context: AssetExecutionContext, stream_name=stream_name):
                """Start Firehose delivery stream data delivery."""
                session = self._get_boto3_session()
                firehose = session.client("firehose")

                # Get stream description
                try:
                    response = firehose.describe_delivery_stream(
                        DeliveryStreamName=stream_name
                    )
                    stream_desc = response["DeliveryStreamDescription"]

                    status = stream_desc["DeliveryStreamStatus"]
                    context.log.info(f"Delivery stream status: {status}")

                    # Get destinations
                    destinations = []
                    if "Destinations" in stream_desc:
                        for dest in stream_desc["Destinations"]:
                            dest_id = dest.get("DestinationId", "unknown")
                            # Determine destination type
                            if "S3DestinationDescription" in dest:
                                dest_type = "S3"
                                bucket = dest["S3DestinationDescription"]["BucketARN"]
                                destinations.append(f"{dest_type}:{bucket}")
                            elif "RedshiftDestinationDescription" in dest:
                                dest_type = "Redshift"
                                destinations.append(dest_type)
                            elif "ElasticsearchDestinationDescription" in dest:
                                dest_type = "Elasticsearch"
                                destinations.append(dest_type)
                            elif "SplunkDestinationDescription" in dest:
                                dest_type = "Splunk"
                                destinations.append(dest_type)
                            elif "HttpEndpointDestinationDescription" in dest:
                                dest_type = "HTTP Endpoint"
                                destinations.append(dest_type)

                    metadata = {
                        "stream_name": stream_name,
                        "status": status,
                        "destinations": ", ".join(destinations) if destinations else "None",
                        "create_timestamp": str(stream_desc.get("CreateTimestamp", "")),
                        "version_id": stream_desc.get("VersionId", "unknown"),
                    }

                    context.log.info(f"Firehose stream active with destinations: {destinations}")

                    return metadata

                except ClientError as e:
                    context.log.error(f"Failed to describe Firehose stream: {e}")
                    raise

            assets.append(firehose_asset)

        return assets

    def _get_analytics_assets(self, session: boto3.Session) -> List:
        """Generate Kinesis Data Analytics application assets."""
        assets = []
        applications = self._list_analytics_applications(session)

        for app in applications:
            app_name = app["name"]
            asset_key = f"analytics_app_{app_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "application_name": app_name,
                    "runtime": app["runtime"],
                    "aws_region": self.aws_region,
                },
            )
            def analytics_asset(context: AssetExecutionContext, app_name=app_name):
                """Start Kinesis Data Analytics application."""
                session = self._get_boto3_session()
                analytics = session.client("kinesisanalyticsv2")

                try:
                    # Get application details
                    response = analytics.describe_application(
                        ApplicationName=app_name
                    )
                    app_detail = response["ApplicationDetail"]

                    status = app_detail["ApplicationStatus"]
                    context.log.info(f"Application status: {status}")

                    # Start application if not running
                    if status in ["READY", "STOPPING", "STOPPED"]:
                        context.log.info(f"Starting application {app_name}...")

                        # For SQL applications, we need an input starting position
                        run_config = {}
                        if app_detail.get("RuntimeEnvironment") == "SQL-1_0":
                            run_config["SqlRunConfigurations"] = [{
                                "InputId": inp["InputId"],
                                "InputStartingPositionConfiguration": {
                                    "InputStartingPosition": "NOW"
                                }
                            } for inp in app_detail.get("InputDescriptions", [])]

                        analytics.start_application(
                            ApplicationName=app_name,
                            RunConfiguration=run_config
                        )

                        context.log.info(f"Application {app_name} start initiated")
                        status = "STARTING"
                    else:
                        context.log.info(f"Application {app_name} already in {status} state")

                    metadata = {
                        "application_name": app_name,
                        "status": status,
                        "runtime_environment": app_detail.get("RuntimeEnvironment", "UNKNOWN"),
                        "version_id": app_detail.get("ApplicationVersionId", 0),
                        "create_timestamp": str(app_detail.get("CreateTimestamp", "")),
                    }

                    return metadata

                except ClientError as e:
                    context.log.error(f"Failed to start analytics application: {e}")
                    raise

            assets.append(analytics_asset)

        return assets

    def _get_observation_sensor(self, session: boto3.Session):
        """Generate sensor to observe Kinesis resources."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def kinesis_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe AWS Kinesis delivery streams and analytics applications."""

            # Get cursor (last check time)
            cursor = context.cursor
            if cursor:
                last_check = datetime.fromisoformat(cursor)
            else:
                last_check = datetime.utcnow() - timedelta(hours=1)

            now = datetime.utcnow()

            # Observe Firehose streams
            if self.import_firehose_streams:
                firehose = session.client("firehose")
                streams = self._list_firehose_streams(session)

                for stream_name in streams:
                    try:
                        response = firehose.describe_delivery_stream(
                            DeliveryStreamName=stream_name
                        )
                        stream_desc = response["DeliveryStreamDescription"]

                        status = stream_desc["DeliveryStreamStatus"]

                        if status == "ACTIVE":
                            asset_key = f"firehose_stream_{stream_name}"

                            yield AssetMaterialization(
                                asset_key=asset_key,
                                metadata={
                                    "stream_name": MetadataValue.text(stream_name),
                                    "status": MetadataValue.text(status),
                                    "observed_at": MetadataValue.text(now.isoformat()),
                                },
                            )
                    except ClientError as e:
                        context.log.warning(f"Failed to describe stream {stream_name}: {e}")

            # Observe Analytics applications
            if self.import_analytics_applications:
                analytics = session.client("kinesisanalyticsv2")
                applications = self._list_analytics_applications(session)

                for app in applications:
                    app_name = app["name"]
                    try:
                        response = analytics.describe_application(
                            ApplicationName=app_name
                        )
                        app_detail = response["ApplicationDetail"]

                        status = app_detail["ApplicationStatus"]

                        if status == "RUNNING":
                            asset_key = f"analytics_app_{app_name}"

                            yield AssetMaterialization(
                                asset_key=asset_key,
                                metadata={
                                    "application_name": MetadataValue.text(app_name),
                                    "status": MetadataValue.text(status),
                                    "runtime": MetadataValue.text(app_detail.get("RuntimeEnvironment", "UNKNOWN")),
                                    "observed_at": MetadataValue.text(now.isoformat()),
                                },
                            )
                    except ClientError as e:
                        context.log.warning(f"Failed to describe application {app_name}: {e}")

            # Update cursor
            context.update_cursor(now.isoformat())

        return kinesis_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        session = self._get_boto3_session()

        assets = []
        sensors = []

        # Import Firehose delivery streams
        if self.import_firehose_streams:
            assets.extend(self._get_firehose_assets(session))

        # Import Data Analytics applications
        if self.import_analytics_applications:
            assets.extend(self._get_analytics_assets(session))

        # Generate observation sensor
        if self.generate_sensor and (self.import_firehose_streams or self.import_analytics_applications):
            sensors.append(self._get_observation_sensor(session))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
