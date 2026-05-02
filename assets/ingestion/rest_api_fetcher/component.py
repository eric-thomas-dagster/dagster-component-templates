"""REST API Fetcher Asset Component.

Fetch data from REST APIs and materialize as Dagster assets.
Supports authentication, pagination, caching, and various output formats.
"""

import json
from typing import Any, Dict, List, Optional, Union
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetKey,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class RestApiFetcherComponent(Component, Model, Resolvable):
    """Component for fetching data from REST APIs.

    This asset fetches data from REST API endpoints and materializes the results.
    Supports various authentication methods, response formats, and output options.

    Example:
        ```yaml
        type: dagster_component_templates.RestApiFetcherComponent
        attributes:
          asset_name: api_data
          api_url: https://api.example.com/data
          method: GET
          output_format: dataframe
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    api_url: str = Field(
        description="URL of the API endpoint to fetch data from"
    )

    method: str = Field(
        default="GET",
        description="HTTP method (GET, POST, PUT, DELETE, etc.)"
    )

    headers: Optional[str] = Field(
        default=None,
        description="JSON string of HTTP headers (e.g., '{\"Authorization\": \"Bearer token\"}')"
    )

    params: Optional[str] = Field(
        default=None,
        description="JSON string of query parameters (e.g., '{\"page\": 1, \"limit\": 100}')"
    )

    body: Optional[str] = Field(
        default=None,
        description="JSON string of request body for POST/PUT requests"
    )

    auth_type: Optional[str] = Field(
        default=None,
        description="Authentication type: 'basic', 'bearer', or None"
    )

    auth_username: Optional[str] = Field(
        default=None,
        description="Username for basic authentication (use ${USERNAME} for env var)"
    )

    auth_password: Optional[str] = Field(
        default=None,
        description="Password for basic authentication (use ${PASSWORD} for env var)"
    )

    auth_token: Optional[str] = Field(
        default=None,
        description="Bearer token for authentication (use ${API_TOKEN} for env var)"
    )

    timeout: int = Field(
        default=30,
        description="Request timeout in seconds"
    )

    output_format: str = Field(
        default="json",
        description=(
            "Output format: 'json', 'dataframe', 'csv', 'parquet', or "
            "'text' / 'html' (raw response body wrapped in a 1-row DataFrame "
            "with a single 'content' column — useful for HTML scraping)"
        ),
    )

    json_path: Optional[str] = Field(
        default=None,
        description="JSON path to extract data from response (e.g., 'data.results')"
    )

    cache_results: bool = Field(
        default=False,
        description="Whether to cache results locally"
    )

    cache_path: Optional[str] = Field(
        default=None,
        description="Path to cache file (required if cache_results is True)"
    )

    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None. The partition key is exposed to api_url and params via {partition_key} (always), plus {partition_date} / {partition_date_next} for time-based partitions.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Values for static or multi partitioning. Accepts a YAML list (`[1, 2, 3]` or `[us, eu, asia]`) or a single comma-separated string (`'1,2,3'`).",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include sample data preview in metadata when output_format is 'dataframe' (first 5 rows as markdown table and interactive preview)"
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


    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_url = self.api_url
        method = self.method.upper()
        headers_str = self.headers
        params_str = self.params
        body_str = self.body
        auth_type = self.auth_type
        auth_username = self.auth_username
        auth_password = self.auth_password
        auth_token = self.auth_token
        timeout = self.timeout
        output_format = self.output_format
        json_path = self.json_path
        cache_results = self.cache_results
        cache_path = self.cache_path
        verify_ssl = self.verify_ssl
        description = self.description or f"Fetch data from {api_url}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Infer kinds from component name if not explicitly set
        _comp_name = "rest_api_fetcher"  # component directory name
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

        # Build a partitions definition if partition_type is set.
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition,
                HourlyPartitionsDefinition,
                MonthlyPartitionsDefinition,
                MultiPartitionsDefinition,
                StaticPartitionsDefinition,
                WeeklyPartitionsDefinition,
            )
            _start = self.partition_start or "2024-01-01"
            # Accept either a list (preferred for dg YAML) or a comma string
            # (legacy / programmatic usage). Both normalize to a list of stripped strings.
            _raw = self.partition_values
            if _raw is None:
                _values = []
            elif isinstance(_raw, str):
                _values = [v.strip() for v in _raw.split(",") if v.strip()]
            else:
                _values = [str(v).strip() for v in _raw if str(v).strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })


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
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def rest_api_asset(context: AssetExecutionContext):
            """Asset that fetches data from REST API."""

            # Check if running in partitioned mode
            partition_date = None
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    context.log.info(f"Fetching API data for partition {context.partition_key}")
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date, "
                        "proceeding without partition date"
                    )
            else:
                context.log.info("Fetching API data (non-partitioned)")

            # Parse headers
            headers = {}
            if headers_str:
                try:
                    headers = json.loads(headers_str)
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid headers JSON: {e}")
                    raise

            # Parse params
            params = {}
            if params_str:
                try:
                    params = json.loads(params_str)
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid params JSON: {e}")
                    raise

            # Add partition date to params if available.
            # This allows API queries to filter by date using the partition.
            # Skip the auto-inject if the user has already opted into the more
            # explicit {partition_date} / {partition_date_next} templating, since
            # APIs that need explicit param names (USGS, etc.) reject the extras
            # with 400 Unknown Parameter.
            _user_uses_templating = (
                "{partition_date}" in (params_str or "")
                or "{partition_date_next}" in (params_str or "")
                or "{partition_key}" in (params_str or "")
                or "{partition_date}" in (api_url or "")
                or "{partition_key}" in (api_url or "")
            )
            if partition_date and not _user_uses_templating:
                params["date"] = partition_date.strftime("%Y-%m-%d")
                params["partition_date"] = partition_date.strftime("%Y-%m-%d")

            # Format-string templating in api_url and params values.
            # Users can use placeholders like {partition_date}, {partition_date_next},
            # {partition_key} to template the URL or any param value at run-time.
            # Example: api_url with {partition_date} substitutes the partition's date.
            from datetime import timedelta
            _template_vars = {
                "partition_key": context.partition_key if context.has_partition_key else "",
                "partition_date": (
                    partition_date.strftime("%Y-%m-%d") if partition_date else ""
                ),
                "partition_date_next": (
                    (partition_date + timedelta(days=1)).strftime("%Y-%m-%d")
                    if partition_date
                    else ""
                ),
            }
            # Use a new local name to avoid Python's closure-rebind-as-local rule
            # (assigning to api_url here would shadow the outer closure variable
            # and trip UnboundLocalError on the first access).
            try:
                _resolved_url = api_url.format(**_template_vars)
            except (KeyError, IndexError):
                _resolved_url = api_url
            for k, v in list(params.items()):
                if isinstance(v, str):
                    try:
                        params[k] = v.format(**_template_vars)
                    except (KeyError, IndexError):
                        pass

            # Parse body
            body = None
            if body_str:
                try:
                    body = json.loads(body_str)
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid body JSON: {e}")
                    raise

            # Setup authentication
            auth = None
            if auth_type == "basic" and auth_username and auth_password:
                auth = (auth_username, auth_password)
            elif auth_type == "bearer" and auth_token:
                headers["Authorization"] = f"Bearer {auth_token}"

            # Make API request
            context.log.info(f"Fetching data from {_resolved_url}")
            context.log.info(f"Method: {method}")

            try:
                response = requests.request(
                    method=method,
                    url=_resolved_url,
                    headers=headers if headers else None,
                    params=params if params else None,
                    json=body if body else None,
                    auth=auth,
                    timeout=timeout,
                    verify=verify_ssl,
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                context.log.error(f"API request failed: {e}")
                raise

            # Parse response. For text/html output we always want raw text;
            # otherwise try JSON first and fall back to text.
            if output_format in ("text", "html"):
                data = response.text
            else:
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    context.log.warning("Response is not JSON, returning raw text")
                    data = response.text

            context.log.info(f"Successfully fetched data (status: {response.status_code})")

            # Extract data using JSON path if provided
            if json_path and isinstance(data, dict):
                parts = json_path.split('.')
                for part in parts:
                    if isinstance(data, dict) and part in data:
                        data = data[part]
                    else:
                        context.log.warning(f"JSON path '{json_path}' not found in response")
                        break

            # Format output
            # Two valid dict shapes coming from APIs:
            #   1) columnar: {col: [v1, v2, ...]} — parallel lists of equal length
            #   2) row:      {field: scalar}     — a single record
            # Detect columnar so we don't produce a 1-row df with list-valued cells.
            def _to_df(payload, dest_label: str) -> pd.DataFrame:
                if isinstance(payload, list):
                    return pd.DataFrame(payload)
                if isinstance(payload, dict):
                    _vals = list(payload.values())
                    _is_columnar = (
                        len(_vals) > 0
                        and all(isinstance(v, list) for v in _vals)
                        and len(set(len(v) for v in _vals)) == 1
                    )
                    return pd.DataFrame(payload) if _is_columnar else pd.DataFrame([payload])
                context.log.error(f"Cannot convert {type(payload)} to {dest_label}")
                raise ValueError(f"Cannot convert {type(payload)} to {dest_label}")

            if output_format == "json":
                result = data

            elif output_format == "dataframe":
                result = _to_df(data, "DataFrame")

            elif output_format == "csv":
                result = _to_df(data, "CSV").to_csv(index=False)

            elif output_format == "parquet":
                buffer = BytesIO()
                _to_df(data, "Parquet").to_parquet(buffer, index=False)
                result = buffer.getvalue()

            elif output_format in ("text", "html"):
                # Wrap raw response body in a 1-row DataFrame so downstream
                # transforms like html_parser, regex_parser, markdown_stripper
                # have a column to operate on.
                result = pd.DataFrame({"content": [str(data)]})

            else:
                context.log.error(f"Unknown output format: {output_format}")
                raise ValueError(f"Unknown output format: {output_format}")

            # Cache results if requested
            if cache_results and cache_path:
                context.log.info(f"Caching results to {cache_path}")
                if output_format == "json":
                    with open(cache_path, 'w') as f:
                        json.dump(result, f, indent=2)
                elif output_format == "dataframe":
                    result.to_parquet(cache_path, index=False)
                elif output_format == "csv":
                    with open(cache_path, 'w') as f:
                        f.write(result)
                elif output_format == "parquet":
                    with open(cache_path, 'wb') as f:
                        f.write(result)

            # Add metadata
            metadata = {
                "api_url": api_url,
                "method": method,
                "status_code": response.status_code,
            }

            if output_format in ["dataframe", "csv", "parquet"]:
                if output_format == "dataframe":
                    df = result
                elif output_format == "csv":
                    df = pd.read_csv(BytesIO(result.encode()))
                else:  # parquet
                    df = pd.read_parquet(BytesIO(result))

                metadata.update({
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                    "columns": list(df.columns),
                })

            context.add_output_metadata(metadata)

            # Return with sample metadata if requested and output is a DataFrame
            if include_preview and output_format == "dataframe" and isinstance(result, pd.DataFrame) and len(result) > 0:
                return Output(
                    value=result,
                    metadata={
                        "row_count": len(result),
                        "columns": result.columns.tolist(),
                        "preview": MetadataValue.md(result.head().to_markdown())
                    }
                )
            else:
                return result

        return Definitions(assets=[rest_api_asset])
