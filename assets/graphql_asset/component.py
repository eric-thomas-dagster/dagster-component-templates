"""GraphQL Asset Component.

Executes one or more GraphQL queries against an API, flattens the JSON responses
with pandas, and writes results to a destination database via SQLAlchemy.

Features
--------
- Multiple named queries in a single component, each writing to its own table.
- Optional cursor-based or page-based pagination (``paginate: true``).
- Bearer-token (or custom-header) authentication.
- Configurable ``data_path`` to navigate nested responses, e.g. ``data.users``.
"""
from __future__ import annotations

from typing import Any, Optional

import dagster as dg


def _get_nested(obj: Any, path: str) -> Any:
    """Navigate a dot-separated path through nested dicts/lists."""
    for key in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
        if obj is None:
            return None
    return obj


class GraphQLAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Execute GraphQL queries and write results to a database.

    Each entry in ``queries`` is executed independently and written to a table named
    ``{asset_name}_{query_name}``.  Set ``paginate: true`` to enable automatic
    cursor-based pagination using ``pageInfo.endCursor`` / ``pageInfo.hasNextPage``,
    or integer ``page`` variable cycling.

    Example:
        ```yaml
        type: dagster_component_templates.GraphQLAssetComponent
        attributes:
          asset_name: github_data
          endpoint_env_var: GITHUB_GRAPHQL_URL
          api_key_env_var: GITHUB_TOKEN
          database_url_env_var: DATABASE_URL
          queries:
            - name: issues
              query: |
                query($owner: String!, $repo: String!, $cursor: String) {
                  repository(owner: $owner, name: $repo) {
                    issues(first: 100, after: $cursor) {
                      nodes { number title state createdAt }
                      pageInfo { endCursor hasNextPage }
                    }
                  }
                }
              variables:
                owner: my-org
                repo: my-repo
              data_path: data.repository.issues.nodes
          paginate: true
          page_size: 100
          group_name: github
          deps:
            - raw/repositories
        ```
    """

    endpoint_env_var: str = dg.Field(
        description="Env var name holding the GraphQL endpoint URL."
    )
    api_key_env_var: Optional[str] = dg.Field(
        default=None,
        description="Env var name holding the API key / token. Omit to disable auth.",
    )
    auth_header: str = dg.Field(
        default="Authorization",
        description="HTTP header name used for the API key.",
    )
    auth_prefix: str = dg.Field(
        default="Bearer",
        description='Prefix added before the key value, e.g. "Bearer". Set to "" for raw key.',
    )
    queries: list = dg.Field(
        description=(
            "List of query configs. Each entry must have a ``name`` (used as table name "
            "suffix) and ``query`` (GraphQL query string). Optional: ``variables`` (dict), "
            "``data_path`` (dot-separated path to the result array in the response)."
        )
    )
    database_url_env_var: str = dg.Field(
        description="Env var name holding the SQLAlchemy database connection URL."
    )
    target_schema: Optional[str] = dg.Field(
        default=None,
        description="Database schema to write tables into. Uses default schema if None.",
    )
    if_exists: str = dg.Field(
        default="replace",
        description='Behaviour when a table already exists: "replace", "append", or "fail".',
    )
    paginate: bool = dg.Field(
        default=False,
        description=(
            "If True, paginate results. Cursor-based pagination is attempted first "
            "(pageInfo.endCursor / pageInfo.hasNextPage). Falls back to integer page variable."
        ),
    )
    page_size: int = dg.Field(
        default=100,
        description="Records per page when paginating.",
    )
    max_pages: int = dg.Field(
        default=100,
        description="Maximum pages to fetch per query as a safety cap.",
    )
    group_name: Optional[str] = dg.Field(
        default="graphql",
        description="Dagster asset group name shown in the UI.",
    )
    asset_name: str = dg.Field(description="Dagster asset key name.")
    deps: Optional[list] = dg.Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name or "graphql",
            kinds={"graphql", "api", "sql"},
            deps=dep_keys,
            description=f"GraphQL asset: {len(self.queries)} quer{'y' if len(self.queries) == 1 else 'ies'}",
        )
        def _graphql_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import os
            import requests
            import pandas as pd
            from sqlalchemy import create_engine

            endpoint = os.environ[self.endpoint_env_var]
            db_url = os.environ[self.database_url_env_var]

            headers: dict[str, str] = {"Content-Type": "application/json"}
            if self.api_key_env_var:
                key = os.environ[self.api_key_env_var]
                headers[self.auth_header] = (
                    f"{self.auth_prefix} {key}" if self.auth_prefix else key
                )

            engine = create_engine(db_url)
            total_rows = 0
            queries_executed = 0
            per_query_counts: dict[str, int] = {}

            for q_cfg in self.queries:
                q_name: str = q_cfg["name"]
                query_str: str = q_cfg["query"]
                base_variables: dict = dict(q_cfg.get("variables") or {})
                data_path: Optional[str] = q_cfg.get("data_path")
                table_name = f"{self.asset_name}_{q_name}"

                records: list[dict] = []

                if self.paginate:
                    # Try cursor-based pagination first; fall back to integer page.
                    cursor: Optional[str] = None
                    use_cursor = True
                    page = 1

                    for _ in range(self.max_pages):
                        variables = dict(base_variables)
                        if use_cursor:
                            if cursor is not None:
                                variables["cursor"] = cursor
                        else:
                            variables["page"] = page
                            variables["pageSize"] = self.page_size

                        resp = requests.post(
                            endpoint,
                            json={"query": query_str, "variables": variables},
                            headers=headers,
                            timeout=60,
                        )
                        resp.raise_for_status()
                        data = resp.json()

                        # Extract array
                        if data_path:
                            items = _get_nested(data, data_path)
                        else:
                            items = data.get("data") or data

                        if not items:
                            break

                        if isinstance(items, list):
                            records.extend(items)
                        else:
                            records.append(items)

                        # Detect pageInfo for cursor pagination
                        raw_data = data.get("data", {}) or {}
                        has_next = False
                        for _v in _iter_leaves(raw_data):
                            if isinstance(_v, dict) and "hasNextPage" in _v:
                                has_next = bool(_v.get("hasNextPage"))
                                if _v.get("endCursor"):
                                    cursor = _v["endCursor"]
                                else:
                                    use_cursor = False
                                break

                        if use_cursor:
                            if not has_next:
                                break
                        else:
                            # Integer pagination: stop when fewer records than page_size
                            if len(items) < self.page_size:
                                break
                            page += 1

                else:
                    resp = requests.post(
                        endpoint,
                        json={"query": query_str, "variables": base_variables},
                        headers=headers,
                        timeout=60,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    if data_path:
                        items = _get_nested(data, data_path)
                    else:
                        items = data.get("data") or data

                    if items:
                        if isinstance(items, list):
                            records.extend(items)
                        else:
                            records.append(items)

                row_count = len(records)
                context.log.info(f"Query '{q_name}': {row_count} records -> {table_name}")

                if records:
                    df = pd.json_normalize(records)
                    df.to_sql(
                        table_name,
                        con=engine,
                        schema=self.target_schema,
                        if_exists=self.if_exists,
                        index=False,
                        method="multi",
                        chunksize=1000,
                    )

                per_query_counts[q_name] = row_count
                total_rows += row_count
                queries_executed += 1

            return dg.MaterializeResult(
                metadata={
                    "total_rows": dg.MetadataValue.int(total_rows),
                    "queries_executed": dg.MetadataValue.int(queries_executed),
                    "per_query_rows": dg.MetadataValue.json(per_query_counts),
                }
            )

        return dg.Definitions(assets=[_graphql_asset])


def _iter_leaves(obj: Any):
    """Yield all leaf values (including nested dict nodes) from a nested structure."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_leaves(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_leaves(item)
