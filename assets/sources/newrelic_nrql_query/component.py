"""New Relic NRQL → DataFrame.

Run an NRQL query via NerdGraph and materialize the result as a
DataFrame asset.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class NewRelicNrqlQueryComponent(Component, Model, Resolvable):
    """Run an NRQL query via NerdGraph → DataFrame asset."""

    asset_name: str = Field(description="Output Dagster asset name")
    api_key_env_var: str = Field(description="Env var holding the User API Key")
    account_id: str = Field(description="New Relic Account ID")
    region: str = Field(default="US", description="'US' or 'EU'")
    query: str = Field(description="NRQL query, e.g. 'SELECT count(*) FROM Transaction SINCE 1 hour ago FACET appName'")

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["newrelic", "nrql", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"NRQL: {cfg.query[:80]}",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def nrql_query(context: AssetExecutionContext) -> MaterializeResult:
            import requests
            api_key = os.environ.get(cfg.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"Missing {cfg.api_key_env_var}")
            url = (
                "https://api.newrelic.com/graphql"
                if cfg.region.upper() == "US"
                else "https://api.eu.newrelic.com/graphql"
            )
            gql = """query($accountId: Int!, $query: Nrql!) {
                actor { account(id: $accountId) { nrql(query: $query) { results } } }
            }"""
            resp = requests.post(
                url,
                json={"query": gql, "variables": {"accountId": int(cfg.account_id), "query": cfg.query}},
                headers={"API-Key": api_key},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            errors = data.get("errors")
            if errors:
                raise Exception(f"NerdGraph errors: {errors}")
            results = data["data"]["actor"]["account"]["nrql"]["results"] or []
            df = pd.DataFrame(results)
            context.log.info(f"NRQL returned {len(df)} rows × {len(df.columns)} cols")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "region": MetadataValue.text(cfg.region),
                    "query": MetadataValue.text(cfg.query[:500]),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[nrql_query])
