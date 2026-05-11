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
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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

    endpoint_env_var: str = Field(
        description="Env var name holding the GraphQL endpoint URL."
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var name holding the API key / token. Omit to disable auth.",
    )
    auth_header: str = Field(
        default="Authorization",
        description="HTTP header name used for the API key.",
    )
    auth_prefix: str = Field(
        default="Bearer",
        description='Prefix added before the key value, e.g. "Bearer". Set to "" for raw key.',
    )
    queries: list = Field(
        description=(
            "List of query configs. Each entry must have a ``name`` (used as table name "
            "suffix) and ``query`` (GraphQL query string). Optional: ``variables`` (dict), "
            "``data_path`` (dot-separated path to the result array in the response)."
        )
    )
    database_url_env_var: str = Field(
        description="Env var name holding the SQLAlchemy database connection URL."
    )
    target_schema: Optional[str] = Field(
        default=None,
        description="Database schema to write tables into. Uses default schema if None.",
    )
    if_exists: str = Field(
        default="replace",
        description='Behaviour when a table already exists: "replace", "append", or "fail".',
    )
    paginate: bool = Field(
        default=False,
        description=(
            "If True, paginate results. Cursor-based pagination is attempted first "
            "(pageInfo.endCursor / pageInfo.hasNextPage). Falls back to integer page variable."
        ),
    )
    page_size: int = Field(
        default=100,
        description="Records per page when paginating.",
    )
    max_pages: int = Field(
        default=100,
        description="Maximum pages to fetch per query as a safety cap.",
    )
    group_name: Optional[str] = Field(
        default="graphql",
        description="Dagster asset group name shown in the UI.",
    )
    asset_name: str = Field(description="Dagster asset key name.")
    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the data being written / produced in metadata, "
            "so builder UIs can show output shape without warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows in the preview when include_preview_metadata=True. Random "
            "sample if len > 10x preview_rows; else head."
        ),
    )

    deps: Optional[list] = Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


    retry_policy_max_retries: Optional[int] = Field(


        default=None,


        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",


    )


    retry_policy_delay_seconds: Optional[int] = Field(


        default=None,


        description="Seconds between retries (default 1).",


    )


    retry_policy_backoff: str = Field(


        default="exponential",


        description="Backoff strategy: 'linear' or 'exponential'.",


    )




    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Dagster asset group name.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        # Build partition definition (auto-generated; supports daily, weekly,

        # monthly, hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=self.asset_name,
            group_name=self.group_name or "graphql",
            kinds={"graphql", "api", "sql"},
            deps=dep_keys,
            description=f"GraphQL asset: {len(self.queries)} quer{'y' if len(self.queries) == 1 else 'ies'}",
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
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
                **({"preview": MetadataValue.md((df.sample(preview_rows) if len(df) > preview_rows * 10 else df.head(preview_rows)).to_markdown(index=False))} if include_preview and len(df) > 0 else {}),
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
