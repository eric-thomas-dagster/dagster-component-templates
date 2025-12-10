"""Google Cloud Pub/Sub Component.

Import Google Cloud Pub/Sub topics and subscriptions as Dagster assets
for monitoring message queue status and throughput.
"""

import re
from typing import Optional, List, Dict, Any

from google.cloud import pubsub_v1
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


class GooglePubSubComponent(Component, Model, Resolvable):
    """Component for importing Google Cloud Pub/Sub entities as Dagster assets.

    Supports importing:
    - Topics (observe message publishing activity)
    - Subscriptions (observe message consumption)

    Example:
        ```yaml
        type: dagster_component_templates.GooglePubSubComponent
        attributes:
          project_id: my-gcp-project
          credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
          import_topics: true
          import_subscriptions: true
        ```
    """

    project_id: str = Field(description="GCP project ID")

    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to GCP service account credentials JSON file (optional)"
    )

    import_topics: bool = Field(
        default=True,
        description="Import Pub/Sub topics as observable assets"
    )

    import_subscriptions: bool = Field(
        default=True,
        description="Import Pub/Sub subscriptions as observable assets"
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
        description="Generate observation sensor"
    )

    group_name: str = Field(
        default="google_pubsub",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Google Pub/Sub component"
    )

    def _get_publisher_client(self) -> pubsub_v1.PublisherClient:
        """Create Pub/Sub publisher client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return pubsub_v1.PublisherClient(credentials=credentials)
        return pubsub_v1.PublisherClient()

    def _get_subscriber_client(self) -> pubsub_v1.SubscriberClient:
        """Create Pub/Sub subscriber client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            return pubsub_v1.SubscriberClient(credentials=credentials)
        return pubsub_v1.SubscriberClient()

    def _matches_filters(self, name: str) -> bool:
        """Check if entity matches name filters."""
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False
        return True

    def _list_topics(self, client: pubsub_v1.PublisherClient) -> List[str]:
        """List all Pub/Sub topics."""
        topics = []
        project_path = f"projects/{self.project_id}"

        try:
            for topic in client.list_topics(request={"project": project_path}):
                topic_name = topic.name.split("/")[-1]
                if self._matches_filters(topic_name):
                    topics.append(topic_name)
        except exceptions.GoogleAPICallError:
            pass

        return topics

    def _list_subscriptions(self, client: pubsub_v1.SubscriberClient) -> List[str]:
        """List all Pub/Sub subscriptions."""
        subscriptions = []
        project_path = f"projects/{self.project_id}"

        try:
            for subscription in client.list_subscriptions(request={"project": project_path}):
                sub_name = subscription.name.split("/")[-1]
                if self._matches_filters(sub_name):
                    subscriptions.append(sub_name)
        except exceptions.GoogleAPICallError:
            pass

        return subscriptions

    def _get_topic_assets(self, client: pubsub_v1.PublisherClient) -> List:
        """Generate topic observable assets."""
        assets = []
        topics = self._list_topics(client)

        for topic_name in topics:
            asset_key = f"topic_{topic_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={"topic_name": topic_name, "project": self.project_id},
            )
            def topic_asset(context: AssetExecutionContext, topic_name=topic_name):
                """Observe Pub/Sub topic."""
                metadata = {
                    "topic_name": topic_name,
                    "topic_path": f"projects/{self.project_id}/topics/{topic_name}",
                }
                context.log.info(f"Topic: {topic_name}")
                return metadata

            assets.append(topic_asset)

        return assets

    def _get_subscription_assets(self, client: pubsub_v1.SubscriberClient) -> List:
        """Generate subscription observable assets."""
        assets = []
        subscriptions = self._list_subscriptions(client)

        for sub_name in subscriptions:
            asset_key = f"subscription_{sub_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={"subscription_name": sub_name, "project": self.project_id},
            )
            def subscription_asset(context: AssetExecutionContext, sub_name=sub_name):
                """Observe Pub/Sub subscription."""
                metadata = {
                    "subscription_name": sub_name,
                    "subscription_path": f"projects/{self.project_id}/subscriptions/{sub_name}",
                }
                context.log.info(f"Subscription: {sub_name}")
                return metadata

            assets.append(subscription_asset)

        return assets

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        assets = []

        if self.import_topics:
            client = self._get_publisher_client()
            assets.extend(self._get_topic_assets(client))

        if self.import_subscriptions:
            client = self._get_subscriber_client()
            assets.extend(self._get_subscription_assets(client))

        return Definitions(assets=assets)
