"""ServiceNow Ingestion Component.

Read records from any ServiceNow table (incident, change_request, sc_request,
sys_user, cmdb_ci, custom tables, etc.) as a DataFrame asset. Goes through
the standard ServiceNow Table API + Dagster context, with pagination, query
filtering, field selection, and basic-auth or bearer-token (OAuth) auth.

Common use cases:
  - Pull last 90 days of incidents → warehouse for service-quality analytics
  - Snapshot the CMDB into a data product
  - Sync change-management activity for compliance / audit
  - Materialize a custom ServiceNow table that downstream Dagster assets
    transform and route to the data warehouse

Auth: either env vars directly (instance_env_var + username_env_var +
password_env_var, OR instance_env_var + bearer_token_env_var) — or via
a shared `servicenow_resource` if you have multiple ServiceNow assets in
the same project.
"""

import os
from typing import Any, Dict, List, Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


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


class ServiceNowIngestionComponent(Component, Model, Resolvable):
    """Component for reading records from a ServiceNow table.

    Calls the ServiceNow Table API (`/api/now/table/<table>`), paginates
    via `sysparm_limit` + `sysparm_offset`, applies an optional encoded
    `sysparm_query` filter, and returns results as a DataFrame asset.

    Example (basic auth — dev instance):
        ```yaml
        type: dagster_component_templates.ServiceNowIngestionComponent
        attributes:
          asset_name: incidents_last_30d
          instance_env_var: SNOW_INSTANCE
          username_env_var: SNOW_USERNAME
          password_env_var: SNOW_PASSWORD
          table: incident
          sysparm_query: "sys_created_on>=javascript:gs.daysAgoStart(30)"
          sysparm_fields: number,short_description,priority,state,assigned_to,sys_created_on
          page_size: 1000
          max_records: 50000
          group_name: servicenow
        ```

    Example (OAuth bearer token — production):
        ```yaml
        type: dagster_component_templates.ServiceNowIngestionComponent
        attributes:
          asset_name: change_requests_open
          instance_env_var: SNOW_INSTANCE
          bearer_token_env_var: SNOW_ACCESS_TOKEN
          table: change_request
          sysparm_query: "state!=closed"
          group_name: servicenow
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    instance_env_var: str = Field(
        description="Env var with the ServiceNow instance subdomain (e.g. 'mycompany' or 'dev123456')",
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow username (basic auth). Required unless bearer_token_env_var is set.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow password (basic auth). Required unless bearer_token_env_var is set.",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow OAuth bearer token. When set, basic-auth env vars are ignored.",
    )
    table: str = Field(
        description="ServiceNow table to query (e.g. 'incident', 'change_request', 'sc_request', 'sys_user', 'cmdb_ci', or a custom table name).",
    )
    sysparm_query: Optional[str] = Field(
        default=None,
        description="ServiceNow encoded query string filter (e.g. 'state=approved^priority=1'). See ServiceNow docs on encoded queries.",
    )
    sysparm_fields: Optional[str] = Field(
        default=None,
        description="Comma-separated list of fields to return. Defaults to all fields if unset.",
    )
    sysparm_display_value: str = Field(
        default="false",
        description="Reference field display: 'false' = sys_id only, 'true' = display value only, 'all' = both. Default 'false' (raw).",
    )
    page_size: int = Field(
        default=1000,
        description="Records fetched per API call (sysparm_limit). Default 1000; ServiceNow's max recommended is 10000.",
    )
    max_records: Optional[int] = Field(
        default=None,
        description="Cap on total records returned across all pages. None (default) = no cap.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS verification. Set false only for self-signed dev instances.",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage",
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
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
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        instance_env_var = self.instance_env_var
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
        bearer_token_env_var = self.bearer_token_env_var
        table = self.table
        sysparm_query = self.sysparm_query
        sysparm_fields = self.sysparm_fields
        sysparm_display_value = self.sysparm_display_value
        page_size = self.page_size
        max_records = self.max_records
        verify_ssl = self.verify_ssl
        deps = self.deps
        group_name = self.group_name

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "mongodb_reader"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy,
            name=asset_name,
            description=f"ServiceNow table reader: {table}",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def servicenow_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            """Read records from the ServiceNow table and return as a DataFrame."""
            try:
                import requests
            except ImportError:
                raise ImportError("requests required: pip install requests")

            instance = os.environ[instance_env_var]
            if not instance:
                raise ValueError(f"{instance_env_var} is empty")

            # Auth: bearer token wins if present, otherwise basic auth.
            headers = {"Accept": "application/json"}
            auth = None
            if bearer_token_env_var:
                token = os.environ.get(bearer_token_env_var, "")
                if not token:
                    raise ValueError(f"{bearer_token_env_var} is empty")
                headers["Authorization"] = f"Bearer {token}"
            else:
                if not (username_env_var and password_env_var):
                    raise ValueError(
                        "Provide bearer_token_env_var OR (username_env_var + password_env_var)."
                    )
                auth = (os.environ[username_env_var], os.environ[password_env_var])

            url = f"https://{instance}.service-now.com/api/now/table/{table}"
            base_params: Dict[str, Any] = {
                "sysparm_limit": page_size,
                "sysparm_display_value": sysparm_display_value,
            }
            if sysparm_query:
                base_params["sysparm_query"] = sysparm_query
            if sysparm_fields:
                base_params["sysparm_fields"] = sysparm_fields

            all_records: List[Dict[str, Any]] = []
            offset = 0
            while True:
                params = {**base_params, "sysparm_offset": offset}
                context.log.info(
                    f"GET {url}?sysparm_offset={offset}&sysparm_limit={page_size}"
                    + (f"&sysparm_query={sysparm_query}" if sysparm_query else "")
                )
                resp = requests.get(
                    url, headers=headers, auth=auth, params=params,
                    verify=verify_ssl, timeout=60,
                )
                resp.raise_for_status()
                batch = resp.json().get("result", [])
                if not batch:
                    break
                all_records.extend(batch)
                if max_records is not None and len(all_records) >= max_records:
                    all_records = all_records[:max_records]
                    break
                if len(batch) < page_size:
                    break
                offset += page_size

            df = pd.DataFrame(all_records)
            context.log.info(
                f"Retrieved {len(df)} records from ServiceNow table '{table}' "
                f"({len(df.columns)} columns)"
            )

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # ServiceNow is a source — no upstream DataFrame to infer column lineage from.
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[servicenow_ingestion_asset])


        return Definitions(assets=[servicenow_ingestion_asset], asset_checks=list(_schema_checks))
