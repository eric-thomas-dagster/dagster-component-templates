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
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



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


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Filter a fully-fetched DataFrame down to the current partition's rows.

    This is the ONE piece of the partitioning puzzle this component was
    missing: partition_type/partition_date_column/partition_static_column
    were declared and documented ("Column used to filter the upstream
    DataFrame to the current date/static partition value") but nothing
    ever actually filtered anything -- every partition silently wrote the
    identical full result set. Time-based partitions use
    context.partition_time_window, which dagster itself already keeps
    correctly range-aware for a single tick AND a single_run/multi_run
    backfill range alike -- no per-key looping needed. Static/dynamic/multi
    partitions have no natural time window, so rows are matched against the
    set of context.partition_keys instead (1 key normally, the whole range
    for a backfill).
    """
    if not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        import pandas as pd
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition -- no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        start = pd.Timestamp(window.start)
        end = pd.Timestamp(window.end)
        return df[(dates >= start) & (dates < end)]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df


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
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class GraphQLIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Execute GraphQL queries and write results to a database.

    Each entry in ``queries`` is executed independently and written to a table named
    ``{asset_name}_{query_name}``.  Set ``paginate: true`` to enable automatic
    cursor-based pagination using ``pageInfo.endCursor`` / ``pageInfo.hasNextPage``,
    or integer ``page`` variable cycling.

    Example:
        ```yaml
        type: dagster_component_templates.GraphQLIngestionComponent
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
    database_url_env_var: Optional[str] = Field(
        default=None,
        description="Env var name holding the SQLAlchemy database connection URL. Set this OR resource_key.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource (e.g. postgres_resource) to use for the destination connection instead of database_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
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

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
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

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run: each query's "
            "full result set is fetched once (as always) and then filtered "
            "down to the whole range via partition_date_column/"
            "partition_static_column before being written (same mechanism as "
            "a normal tick, just spanning more rows). 'multi_run' launches "
            "one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. Unlike dlt-based ingestion "
            "connectors, this asset writes directly to SQL and returns no "
            "IO-managed value, so 'single_run' works with any IO manager."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Standard catalog fields — phase 2 wiring
        _column_lineage_md = _column_lineage_metadata(self)
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

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



        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name or "graphql",
            kinds={"graphql", "api", "sql"},
            deps=dep_keys,
            description=self.description or (f"GraphQL asset: {len(self.queries)} quer{'y' if len(self.queries) == 1 else 'ies'}"),
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
            required_resource_keys={self.resource_key} if self.resource_key else set(),
        )
        def _graphql_ingestion(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            import os
            import requests
            import pandas as pd
            from sqlalchemy import create_engine

            endpoint = _require_env(self.endpoint_env_var)
            if self.resource_key:
                resource = getattr(context.resources, self.resource_key, None)
                if resource is None:
                    raise ValueError(f"Resource {self.resource_key!r} is not registered in this project's Definitions.")
                db_url = getattr(resource, "connection_string", None)
                if not db_url:
                    raise ValueError(
                        f"Resource {self.resource_key!r} ({type(resource).__name__}) does not expose a "
                        "'connection_string' property, so it can't be used to determine the database "
                        "URL automatically. Use database_url_env_var instead."
                    )
            elif self.database_url_env_var:
                db_url = _require_env(self.database_url_env_var)
            else:
                raise ValueError("Set 'resource_key' or 'database_url_env_var'.")

            headers: dict[str, str] = {"Content-Type": "application/json"}
            if self.api_key_env_var:
                key = _require_env(self.api_key_env_var)
                headers[self.auth_header] = (
                    f"{self.auth_prefix} {key}" if self.auth_prefix else key
                )

            engine = create_engine(db_url)
            total_rows = 0
            queries_executed = 0
            per_query_counts: dict[str, int] = {}
            last_df = None  # for the optional preview below -- may stay None if no query returned any records

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

                context.log.info(f"Query '{q_name}': {len(records)} records fetched")

                row_count = 0
                if records:
                    df = pd.json_normalize(records)
                    df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)
                    row_count = len(df)
                    last_df = df

                if row_count > 0:
                    context.log.info(f"Query '{q_name}': writing {row_count} rows -> {table_name}")
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
                    **(
                        {
                            "preview": dg.MetadataValue.md(
                                (
                                    last_df.sample(preview_rows)
                                    if len(last_df) > preview_rows * 10
                                    else last_df.head(preview_rows)
                                ).to_markdown(index=False)
                            )
                        }
                        if include_preview and last_df is not None and len(last_df) > 0
                        else {}
                    ),
                }
            )

        return dg.Definitions(assets=[_graphql_ingestion])


def _iter_leaves(obj: Any):
    """Yield all leaf values (including nested dict nodes) from a nested structure."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_leaves(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_leaves(item)


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
