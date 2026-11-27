"""Pub/Sub Resource component.

Registers a `PubsubResource` -- a reusable, callable Google Cloud Pub/Sub
topic/subscription lifecycle helper, for use from any op or asset. The
Airflow `PubSubCreateTopicOperator` / `PubSubDeleteTopicOperator` /
`PubSubCreateSubscriptionOperator` / `PubSubDeleteSubscriptionOperator`
equivalent is this resource's methods, not a dedicated asset -- an Airflow
Operator is reusable action code (parameterized, callable from any task),
which maps to a Dagster `ConfigurableResource`, not to an asset (which models
persistent, lineage-tracked data). Call these methods from your own op/asset
when you want the lifecycle action to participate in your pipeline.

Request/response shapes verified directly against the `google-cloud-pubsub`
client reference (not from memory): `PublisherClient.create_topic(request={
'name': topic_path, 'labels': {...}, 'message_retention_duration': timedelta})`
-> a `Topic`; `get_topic(request={'topic': topic_path})`; `delete_topic(
request={'topic': topic_path})`. `SubscriberClient.create_subscription(
request={'name': subscription_path, 'topic': topic_path,
'ack_deadline_seconds': int, 'push_config': {'push_endpoint': str}})` ->
a `Subscription`; `get_subscription(request={'subscription': subscription_path})`;
`delete_subscription(request={'subscription': subscription_path})`.
`AlreadyExists`/`NotFound` come from `google.api_core.exceptions` -- the real
client raises on both rather than silently no-op'ing, so every method here
catches them to make the action idempotent.

Distinct from `GooglePubSubComponent` (`integrations/google_pubsub`), which
only *observes* pre-existing topics/subscriptions as read-only assets -- this
resource manages lifecycle (create/delete), not observation.
"""
from datetime import timedelta
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field, PrivateAttr


class PubsubResource(dg.ConfigurableResource):
    """Reusable Google Cloud Pub/Sub topic/subscription lifecycle helper.

    Every method is idempotent against the real client's `AlreadyExists`/
    `NotFound` errors -- calling `ensure_topic`/`ensure_subscription` a second
    time, or `delete_topic`/`delete_subscription` against something already
    gone, is a no-op rather than an error.
    """

    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var pointing to a GCP service account JSON path. If unset, uses Application Default Credentials.",
    )

    _client_cache: Dict[str, Any] = PrivateAttr(default_factory=dict)

    def _require_env(self, name: str) -> str:
        value = dg.EnvVar(name).get_value()
        if value is None:
            raise ValueError(f"Environment variable {name!r} is not set.")
        return value

    def _ensure_creds_env(self) -> None:
        import os

        if self.credentials_env_var and "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._require_env(self.credentials_env_var)

    def _publisher_client(self):
        from google.cloud import pubsub_v1

        if "publisher" not in self._client_cache:
            self._ensure_creds_env()
            self._client_cache["publisher"] = pubsub_v1.PublisherClient()
        return self._client_cache["publisher"]

    def _subscriber_client(self):
        from google.cloud import pubsub_v1

        if "subscriber" not in self._client_cache:
            self._ensure_creds_env()
            self._client_cache["subscriber"] = pubsub_v1.SubscriberClient()
        return self._client_cache["subscriber"]

    def ensure_topic(
        self,
        project_id: str,
        topic_id: str,
        labels: Optional[Dict[str, str]] = None,
        message_retention_duration_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Create the topic if missing (idempotent). Returns {"topic_path", "action": "created"|"none", "labels"}."""
        from google.api_core import exceptions as gax_exceptions

        client = self._publisher_client()
        topic_path = client.topic_path(project_id, topic_id)
        request: Dict[str, Any] = {"name": topic_path}
        if labels:
            request["labels"] = labels
        if message_retention_duration_seconds is not None:
            request["message_retention_duration"] = timedelta(seconds=message_retention_duration_seconds)
        try:
            topic = client.create_topic(request=request)
            return {"topic_path": topic_path, "action": "created", "labels": dict(topic.labels)}
        except gax_exceptions.AlreadyExists:
            topic = client.get_topic(request={"topic": topic_path})
            return {"topic_path": topic_path, "action": "none", "labels": dict(topic.labels)}

    def delete_topic(self, project_id: str, topic_id: str) -> Dict[str, Any]:
        """Delete the topic if present (idempotent). Returns {"topic_path", "action": "deleted"|"none"}."""
        from google.api_core import exceptions as gax_exceptions

        client = self._publisher_client()
        topic_path = client.topic_path(project_id, topic_id)
        try:
            client.delete_topic(request={"topic": topic_path})
            return {"topic_path": topic_path, "action": "deleted"}
        except gax_exceptions.NotFound:
            return {"topic_path": topic_path, "action": "none"}

    def ensure_subscription(
        self,
        project_id: str,
        topic_id: str,
        subscription_id: str,
        ack_deadline_seconds: int = 10,
        push_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create the subscription on topic_id if missing (idempotent).

        Omitting push_endpoint creates a pull subscription (the default).
        Returns {"subscription_path", "action": "created"|"none", "type": "push"|"pull"}.
        """
        from google.api_core import exceptions as gax_exceptions

        client = self._subscriber_client()
        topic_path = client.topic_path(project_id, topic_id)
        subscription_path = client.subscription_path(project_id, subscription_id)
        request: Dict[str, Any] = {
            "name": subscription_path,
            "topic": topic_path,
            "ack_deadline_seconds": ack_deadline_seconds,
        }
        if push_endpoint:
            request["push_config"] = {"push_endpoint": push_endpoint}
        try:
            client.create_subscription(request=request)
            action = "created"
        except gax_exceptions.AlreadyExists:
            client.get_subscription(request={"subscription": subscription_path})
            action = "none"
        return {"subscription_path": subscription_path, "action": action, "type": "push" if push_endpoint else "pull"}

    def delete_subscription(self, project_id: str, subscription_id: str) -> Dict[str, Any]:
        """Delete the subscription if present (idempotent). Returns {"subscription_path", "action": "deleted"|"none"}."""
        from google.api_core import exceptions as gax_exceptions

        client = self._subscriber_client()
        subscription_path = client.subscription_path(project_id, subscription_id)
        try:
            client.delete_subscription(request={"subscription": subscription_path})
            return {"subscription_path": subscription_path, "action": "deleted"}
        except gax_exceptions.NotFound:
            return {"subscription_path": subscription_path, "action": "none"}


class PubsubResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PubsubResource (Pub/Sub topic/subscription lifecycle helper) for use by other components."""

    resource_key: str = Field(default="pubsub_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var pointing to a GCP service account JSON path. If unset, uses Application Default Credentials.",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Pub/Sub Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PubsubResource(credentials_env_var=self.credentials_env_var)
        return dg.Definitions(resources={self.resource_key: resource})
