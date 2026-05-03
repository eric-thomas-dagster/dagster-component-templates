"""AuditLogsToElasticSecurityComponent.

Ship audit-log DataFrame to Elastic Security via the bulk indexing API.

Authentication: reads credentials from environment variables. See README.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class AuditLogsToElasticSecurityComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ship audit-log DataFrame to Elastic Security via the bulk indexing API."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key — events to ship")

    es_url: str = Field(description="Elasticsearch URL (e.g. 'https://es.acme.com:9200')")
    api_key_env: Optional[str] = Field(default=None, description="Env var with API key (preferred)")
    username_env: Optional[str] = Field(default=None, description="Basic-auth username env var")
    password_env: Optional[str] = Field(default=None, description="Basic-auth password env var")
    index: str = Field(default="logs-audit-default", description="Target index or data stream")
    verify_certs: bool = Field(default=True)

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="security_sink", description="Dagster asset group")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        retry = None
        if self.retry_policy_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else dg.Backoff.LINEAR,
            )

        @dg.asset(
            name=self.asset_name,
            description=self.description or "Ship audit-log DataFrame to Elastic Security via the bulk indexing API.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['elastic', 'elasticsearch', 'siem']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> dg.MaterializeResult:
            from elasticsearch import Elasticsearch, helpers
            kw = {"verify_certs": _self.verify_certs}
            if _self.api_key_env:
                kw["api_key"] = os.environ[_self.api_key_env]
            elif _self.username_env and _self.password_env:
                kw["basic_auth"] = (os.environ[_self.username_env], os.environ[_self.password_env])
            es = Elasticsearch(_self.es_url, **kw)
            actions = ({"_index": _self.index, "_source": json.loads(row.to_json())} for _, row in df.iterrows())
            success, errors = helpers.bulk(es, actions, raise_on_error=False)
            return dg.MaterializeResult(metadata={
                "events_sent": dg.MetadataValue.int(int(success)),
                "errors": dg.MetadataValue.int(len(errors) if isinstance(errors, list) else 0),
                "index": dg.MetadataValue.text(_self.index),
            })

        return dg.Definitions(assets=[_asset])
