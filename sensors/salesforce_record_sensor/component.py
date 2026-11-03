"""SalesforceRecordSensorComponent.

Polls a Salesforce object (SOQL) for records created/updated since the
cursor and triggers a RunRequest per match. Of the SaaS-record sensors in
this repo (stripe_event_sensor, zendesk_ticket_sensor, jira_issue_sensor,
linear_issue_sensor, notion_database_sensor, servicenow_sensor,
pagerduty_incident_sensor, github_event_sensor), Salesforce was the one
gap -- despite salesforce_ingestion and salesforce_record_upsert both
already existing.

Two ways to authenticate, mirroring salesforce_ingestion's own
resource_key convention so one Salesforce credential can serve the
ingestion asset, the reverse-ETL sink, and this sensor without
configuring it three times:

  - `resource_key`: reuse a resource registered by a
    SalesforceResourceComponent at run time -- this calls the resource's
    own `soql(...)` method directly (a real dependency on a Dagster
    resource, not a cross-component code import, so it stays within this
    repo's "no shared helper files" convention). Only meaningful when
    that resource is configured with auth_mode='password' (OAuth 2.0
    password grant) -- same restriction salesforce_ingestion documents,
    since the JWT Bearer flow isn't implemented standalone here either.
  - Inline `consumer_key`/`consumer_secret`/`username`/`password`/
    `security_token`/`domain`: a self-contained OAuth 2.0 password-grant
    login + SOQL query, duplicated here rather than imported, matching
    SalesforceResource._login()'s own request shape.
"""
from typing import List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
)
from pydantic import Field, model_validator


class SalesforceRecordSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a run when a Salesforce object has new or updated records."""

    sensor_name: str = Field(description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys to materialize when a record matches.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")

    sobject: str = Field(description="Salesforce object to poll (e.g. 'Opportunity', 'Lead', 'Case', 'Account').")
    fields: List[str] = Field(
        default=["Id", "Name", "LastModifiedDate"],
        description="SOQL fields to select. 'Id' and 'LastModifiedDate' are used internally -- include them even if you add more.",
    )
    soql_filter: Optional[str] = Field(
        default=None,
        description="Additional WHERE clause, ANDed with the LastModifiedDate cursor filter (e.g. \"StageName = 'Closed Won'\").",
    )

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional resource key registered by a SalesforceResourceComponent. When set, "
            "queries run via that resource's own soql(...) method at run time instead of the "
            "inline consumer_key/consumer_secret/username/password/security_token below -- lets "
            "one Salesforce credential serve the ingestion connector, the salesforce_record_upsert "
            "reverse-ETL sink, and this sensor without configuring it three times. Only meaningful "
            "when that resource is configured with auth_mode='password' (this sensor's standalone "
            "path only supports the password grant, not JWT Bearer, either)."
        ),
    )
    domain: str = Field(default="login", description="'login' (production) or 'test' (sandbox). Ignored when resource_key is set.")
    consumer_key: Optional[str] = Field(default=None, description="Connected App consumer key. Required unless resource_key is set.")
    consumer_secret: Optional[str] = Field(default=None, description="Connected App consumer secret. Required unless resource_key is set.")
    username: Optional[str] = Field(default=None, description="Salesforce username. Required unless resource_key is set.")
    password: Optional[str] = Field(default=None, description="Salesforce password. Required unless resource_key is set.")
    security_token: Optional[str] = Field(default=None, description="Salesforce security token (Settings > Personal Setup > Reset My Security Token). Required unless resource_key is set.")
    api_version: str = Field(default="58.0", description="Salesforce REST API version. Ignored when resource_key is set.")

    @model_validator(mode="after")
    def _check_credentials(self):
        if not self.resource_key and not (self.consumer_key and self.consumer_secret and self.username and self.password and self.security_token):
            raise ValueError(
                "SalesforceRecordSensorComponent: supply resource_key (a registered "
                "SalesforceResourceComponent) OR all of consumer_key/consumer_secret/"
                "username/password/security_token."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Salesforce Record Sensor", editable=True)

    @staticmethod
    def _soql_timestamp(iso_cursor: str) -> str:
        """Salesforce's SOQL datetime literal has no quotes, e.g. 2024-01-01T00:00:00.000Z."""
        return iso_cursor

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        def _login(self_) -> dict:
            """Self-contained OAuth 2.0 password grant -- used only when resource_key is unset."""
            import requests

            url = f"https://{self_.domain}.salesforce.com/services/oauth2/token"
            payload = {
                "grant_type": "password",
                "client_id": self_.consumer_key,
                "client_secret": self_.consumer_secret,
                "username": self_.username,
                "password": self_.password + (self_.security_token or ""),
            }
            resp = requests.post(url, data=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return {"access_token": data["access_token"], "instance_url": data["instance_url"]}

        def sensor_fn(context: SensorEvaluationContext, **resources):
            cursor = context.cursor or "1970-01-01T00:00:00.000Z"
            where = f"LastModifiedDate > {cursor}"
            if _self.soql_filter:
                where = f"({_self.soql_filter}) AND {where}"
            query = (
                f"SELECT {', '.join(_self.fields)} FROM {_self.sobject} "
                f"WHERE {where} ORDER BY LastModifiedDate ASC LIMIT 200"
            )

            records: List[dict] = []
            if resource_key:
                _res = resources.get(resource_key)
                if _res is None:
                    return SkipReason(f"resource_key={resource_key!r} not found in context.resources")
                try:
                    records = list(_res.soql(query, max_records=200))
                except Exception as e:
                    return SkipReason(f"Salesforce SOQL error: {e}")
            else:
                try:
                    import requests

                    auth = _login(_self)
                    resp = requests.get(
                        f"{auth['instance_url']}/services/data/v{_self.api_version}/query",
                        params={"q": query},
                        headers={"Authorization": f"Bearer {auth['access_token']}"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    records = resp.json().get("records", [])
                except ImportError:
                    return SkipReason("requests not installed")
                except Exception as e:
                    return SkipReason(f"Salesforce API error: {e}")

            if not records:
                return SkipReason(f"no new/updated {_self.sobject} records")

            latest = max(r.get("LastModifiedDate", cursor) for r in records)
            context.update_cursor(latest)
            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=str(r.get("Id")),
                        asset_selection=targets,
                        tags={"sobject": _self.sobject, "salesforce_id": str(r.get("Id", ""))},
                    )
                    for r in records
                ]
            )

        sensor_def = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else DefaultSensorStatus.STOPPED,
            asset_selection=AssetSelection.assets(*targets),
            required_resource_keys=required_resource_keys,
        )
        return dg.Definitions(sensors=[sensor_def])
