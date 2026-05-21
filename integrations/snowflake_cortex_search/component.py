"""SnowflakeCortexSearchComponent — query a Snowflake Cortex Search Service.

Cortex Search is Snowflake's built-in RAG / vector-search service. You
pre-create a `CORTEX SEARCH SERVICE` over a table of documents (Snowflake
handles chunking, embedding, indexing). This component issues a query
against that service and materializes the search results as a Dagster
asset — useful for evaluating retrieval quality, snapshotting search
results for downstream consumers, or building a "search-as-an-asset"
pattern where each materialization runs the same query and the asset
history captures how results change as the corpus evolves.

Returns: a list of result rows (a pandas DataFrame in the IO manager,
plus a preview in materialization metadata).

Reference:
  https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext, Component, ComponentLoadContext, Definitions,
    MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field


class SnowflakeCortexSearchComponent(Component, Model, Resolvable):
    """Query a Snowflake Cortex Search Service and materialize the results as a Dagster asset."""

    asset_name: str = Field(description="Output Dagster asset name")

    # ── Connection ──
    account: str = Field(description="Snowflake account, e.g. xy12345-abc.")
    user: str = Field(description="Snowflake user (NAME for keypair, LOGIN_NAME for password/SSO/PAT).")
    warehouse: Optional[str] = Field(default=None)
    database: str = Field(description="Database containing the search service.")
    schema_name: str = Field(description="Schema containing the search service.")
    role: Optional[str] = Field(default=None)

    # ── Auth (pick ONE) ──
    password: Optional[str] = Field(default=None, description="Snowflake password.")
    authenticator: Optional[str] = Field(default=None,
        description="Snowflake authenticator: 'SNOWFLAKE_JWT' (keypair), 'externalbrowser' (SSO), 'PROGRAMMATIC_ACCESS_TOKEN' (PAT), 'oauth'.")
    private_key_file: Optional[str] = Field(default=None)
    private_key_file_pwd: Optional[str] = Field(default=None)
    token: Optional[str] = Field(default=None)

    # ── Cortex Search ──
    service_name: str = Field(
        description="Name of the Cortex Search Service to query. Unqualified — gets resolved to `database.schema_name.service_name`.",
    )
    query: str = Field(description="The search query (natural-language text).")
    result_limit: int = Field(default=10, description="Max number of results to return.")
    result_columns: Optional[List[str]] = Field(
        default=None,
        description="Subset of the search service's source columns to return per result (e.g. ['CONTENT', 'TITLE', 'URL']). If unset, all columns the service exposes are returned.",
    )
    result_filter: Optional[str] = Field(
        default=None,
        description="Optional JSON filter applied at query time (e.g. '{\"@eq\": {\"category\": \"docs\"}}'). See Cortex Search docs for filter syntax.",
    )

    # ── Asset metadata ──
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    # ── Retry policy ──
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on query failure. Defines a RetryPolicy.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def _connect(self):
        import snowflake.connector
        ck = dict(account=self.account, user=self.user,
                  database=self.database, schema=self.schema_name)
        if self.warehouse: ck["warehouse"] = self.warehouse
        if self.role:      ck["role"] = self.role
        if self.authenticator:
            ck["authenticator"] = self.authenticator
            if self.private_key_file:
                ck["private_key_file"] = self.private_key_file
                if self.private_key_file_pwd:
                    ck["private_key_file_pwd"] = self.private_key_file_pwd
            elif self.token:
                ck["token"] = self.token
        elif self.password:
            ck["password"] = self.password
        else:
            raise ValueError(f"{type(self).__name__}: must set password OR authenticator+key/token.")
        return snowflake.connector.connect(**ck)

    def _build_search_sql(self) -> str:
        """SNOWFLAKE.CORTEX.SEARCH_PREVIEW takes a JSON request with query,
        limit, columns, and filter. Returns a VARIANT with a 'results' array."""
        import json
        fq_service = f"{self.database}.{self.schema_name}.{self.service_name}"
        request: Dict[str, Any] = {"query": self.query, "limit": self.result_limit}
        if self.result_columns:
            request["columns"] = self.result_columns
        if self.result_filter:
            request["filter"] = json.loads(self.result_filter)
        # SQL-encode the JSON request, escaping single quotes for the literal.
        req_str = json.dumps(request).replace("'", "''")
        return (
            "SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW("
            f"'{fq_service}', '{req_str}'))['results'] AS results"
        )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        kinds = list(self.kinds or []) or ["snowflake", "cortex"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            retry_policy=_retry_policy,
            name=self.asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _search_asset(context: AssetExecutionContext):
            import json
            sql = self._build_search_sql()
            context.log.info(f"Executing Cortex Search:\n{sql}")
            conn = self._connect()
            results: List[Dict[str, Any]] = []
            try:
                cur = conn.cursor()
                cur.execute(sql)
                row = cur.fetchone()
                if row and row[0] is not None:
                    # The driver may return a VARIANT as str or dict.
                    raw = row[0]
                    if isinstance(raw, str):
                        results = json.loads(raw)
                    elif isinstance(raw, list):
                        results = raw
                cur.close()
            finally:
                conn.close()
            preview = "\n".join(
                f"- {json.dumps(r, default=str)[:200]}" for r in results[:5]
            ) or "(no results)"
            return dg.MaterializeResult(
                value=results,
                metadata={
                    "cortex/service": MetadataValue.text(
                        f"{self.database}.{self.schema_name}.{self.service_name}"
                    ),
                    "cortex/query": MetadataValue.text(self.query),
                    "cortex/result_count": MetadataValue.int(len(results)),
                    "cortex/top_5_preview": MetadataValue.md(preview),
                },
            )

        return Definitions(assets=[_search_asset])

    @classmethod
    def get_description(cls) -> str:
        return "Query a Snowflake Cortex Search Service and materialize the results as a Dagster asset."
