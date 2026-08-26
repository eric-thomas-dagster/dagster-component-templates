"""REST API Fetcher Asset Component.

Fetch data from REST APIs and materialize as Dagster assets.
Supports authentication, pagination, caching, and various output formats.
"""

import json
import os
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

    headers: Optional[Union[str, Dict[str, str]]] = Field(
        default=None,
        description="HTTP headers — either a YAML dict ({Authorization: 'Bearer t'}) or a JSON string ('{\"Authorization\": \"Bearer t\"}'). Dict form is recommended (avoids YAML-loader template-resolution gotchas with single-brace strings)."
    )

    params: Optional[Union[str, Dict[str, Any]]] = Field(
        default=None,
        description="Query parameters — YAML dict or JSON string (dict form recommended)."
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
        default="dataframe",
        description=(
            "Output format: 'dataframe' (default — parsed JSON wrapped in a "
            "pandas DataFrame ready for downstream transforms), 'json' (raw "
            "Python dict/list — use only if a custom asset consumes it "
            "directly), 'csv', 'parquet', or 'text' / 'html' (raw response "
            "body wrapped in a 1-row DataFrame with a single 'content' column "
            "— useful for HTML scraping)"
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



    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    # ─── Response reshaping (JMESPath) ─────────────────────────────────────
    response_path: Optional[str] = Field(
        default=None,
        description=(
            "JMESPath expression selecting the array of records to iterate. "
            "Used with `column_map` to normalize a nested / heterogeneous API "
            "response into a tabular batch. Example: `output.rateReplyDetails` "
            "or `data.results[?status=='active']`. When both `json_path` and "
            "`response_path` are set, `json_path` runs first (dotted-path pre-"
            "extraction), then `response_path` (JMESPath) selects the array. "
            "Requires `pip install jmespath`."
        ),
    )

    column_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Dict of `{output_column: jmespath_expression}` evaluated against "
            "each element of the array selected by `response_path`. Produces "
            "one row per array element with named columns extracted from "
            "arbitrary JSON shapes. Example: "
            "`{rate: 'ratedShipmentDetails[0].totalNetCharge', fuel: 'ratedShipmentDetails[0].surcharges[?type==`FUEL`].amount | [0]'}`. "
            "Requires `pip install jmespath`."
        ),
    )

    # ─── Multi-endpoint dispatch ───────────────────────────────────────────
    endpoints: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-partition-value endpoint dispatch — dict keyed by the value of "
            "`partition_static_dim` (multi-partition mode) or by the partition "
            "key itself (single-dim static mode). When set, the top-level "
            "`api_url` / `method` / `headers` / `params` / `auth_*` / "
            "`response_path` / `column_map` fields are OVERRIDDEN by the "
            "endpoint config for the current partition. Each endpoint config "
            "may specify any of: `api_url`, `method`, `headers` (dict), "
            "`params` (dict), `body` (JSON string), `auth_type`, "
            "`auth_username_env_var`, `auth_password_env_var`, "
            "`auth_token_env_var`, `timeout`, `response_path`, `column_map`. "
            "Enables 'N different vendor APIs land in one warehouse table' "
            "with no custom Python (freight carrier rate feeds, per-region "
            "SaaS APIs, per-tenant reporting, etc.)."
        ),
    )

    # ─── Sinks (write batch to a warehouse table) ──────────────────────────
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Optional list of sinks that write the fetched DataFrame to a "
            "warehouse table (in addition to returning it as the asset value "
            "for the IO manager). Each sink is a dict — supported keys: "
            "`kind: table`, `resource_key: <name>`, `table: <name>`, "
            "`schema: <optional>`, `if_exists: append|replace` (default "
            "`append`), `mode: upsert_on_match` (partition-rewrite idempotent "
            "DELETE-then-INSERT keyed by `match`), `match: [col, col]` "
            "(required for `mode: upsert_on_match`), `partition_dim_columns: "
            "{output_col: partition_dim_name}` (inject partition values as "
            "columns before writing — useful for multi-partition dispatch "
            "into a single shared table). Auto-detects DuckDB's `.register()` "
            "fast path when the resource yields a DuckDB connection; falls "
            "back to SQLAlchemy `to_sql` for postgres / snowflake / bigquery "
            "/ mysql / mssql / …. `resource_key` must be declared on the "
            "Definitions the component is loaded into."
        ),
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

        # Closure-bind the new fields so they're available in the asset function.
        response_path = self.response_path
        column_map = self.column_map
        endpoints = self.endpoints
        sinks = self.sinks or []
        partition_static_dim = self.partition_static_dim

        # Compute required_resource_keys from sinks — Dagster wires only what
        # we declare, so this must reflect every `resource_key` we call into.
        required_resource_keys: set = set()
        for _sink in sinks:
            _rk = _sink.get("resource_key")
            if _rk:
                required_resource_keys.add(_rk)

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
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            required_resource_keys=required_resource_keys or None,
        )
        def rest_api_asset(context: AssetExecutionContext):
            """Asset that fetches data from REST API."""

            # These start bound to the top-level fields but may be overridden
            # by per-partition endpoint dispatch below.
            _api_url = api_url
            _method = method
            _headers_str = headers_str
            _params_str = params_str
            _body_str = body_str
            _auth_type = auth_type
            _auth_username = auth_username
            _auth_password = auth_password
            _auth_token = auth_token
            _timeout = timeout
            _response_path = response_path
            _column_map = column_map

            # Check if running in partitioned mode
            partition_date = None
            partition_dim_values: Dict[str, str] = {}
            if context.has_partition_key:
                pk = context.partition_key
                # MultiPartitionKey has .keys_by_dimension; single-dim is a str.
                if hasattr(pk, "keys_by_dimension"):
                    partition_dim_values = dict(pk.keys_by_dimension)
                    _date_str = partition_dim_values.get("date")
                    if _date_str:
                        try:
                            partition_date = datetime.strptime(_date_str, "%Y-%m-%d")
                        except ValueError:
                            pass
                    context.log.info(
                        f"Fetching API data for multi-partition {dict(partition_dim_values)}"
                    )
                else:
                    # Single-dim: try to parse as date, else treat as opaque key.
                    try:
                        partition_date = datetime.strptime(pk, "%Y-%m-%d")
                        context.log.info(f"Fetching API data for partition {pk}")
                    except ValueError:
                        context.log.info(
                            f"Fetching API data for static partition {pk!r}"
                        )
            else:
                context.log.info("Fetching API data (non-partitioned)")

            # ── Per-partition endpoint dispatch ──────────────────────────
            # When `endpoints:` is set, look up the config keyed by the
            # partition dim value and OVERRIDE api_url + auth + params +
            # response_path + column_map for this run only.
            if endpoints:
                if partition_dim_values:
                    dim_name = partition_static_dim or next(
                        (d for d in partition_dim_values if d != "date"),
                        None,
                    )
                    dim_value = partition_dim_values.get(dim_name) if dim_name else None
                else:
                    dim_value = context.partition_key if context.has_partition_key else None
                if dim_value is None:
                    raise ValueError(
                        "endpoints: dispatch requires a partition_static_dim value "
                        "(multi-partition mode) or a single-dim static partition key"
                    )
                ep = endpoints.get(str(dim_value))
                if ep is None:
                    raise ValueError(
                        f"endpoints: no config for partition value {dim_value!r}. "
                        f"Configured keys: {sorted(endpoints.keys())}"
                    )
                # Override top-level fields with endpoint config.
                _api_url = ep.get("api_url", _api_url)
                _method = (ep.get("method") or _method).upper()
                _headers_str = ep.get("headers", _headers_str)
                _params_str = ep.get("params", _params_str)
                _body_str = ep.get("body", _body_str)
                _auth_type = ep.get("auth_type", _auth_type)
                _timeout = ep.get("timeout", _timeout)
                _response_path = ep.get("response_path", _response_path)
                _column_map = ep.get("column_map", _column_map)
                # Auth via env vars only in endpoint config (safer than
                # putting raw secrets in per-endpoint YAML).
                if ep.get("auth_username_env_var"):
                    _auth_username = os.environ.get(ep["auth_username_env_var"])
                if ep.get("auth_password_env_var"):
                    _auth_password = os.environ.get(ep["auth_password_env_var"])
                if ep.get("auth_token_env_var"):
                    _auth_token = os.environ.get(ep["auth_token_env_var"])
                context.log.info(
                    f"endpoints: dispatched to {dim_value!r} → {_api_url}"
                )

            # Parse headers (accept dict or JSON string)
            headers = {}
            if _headers_str:
                if isinstance(_headers_str, dict):
                    headers = dict(_headers_str)
                else:
                    try:
                        headers = json.loads(_headers_str)
                    except json.JSONDecodeError as e:
                        context.log.error(f"Invalid headers JSON: {e}")
                        raise

            # Parse params (accept dict or JSON string)
            params = {}
            if _params_str:
                if isinstance(_params_str, dict):
                    params = dict(_params_str)
                else:
                    try:
                        params = json.loads(_params_str)
                    except json.JSONDecodeError as e:
                        context.log.error(f"Invalid params JSON: {e}")
                        raise

            # Add partition date to params if available.
            # This allows API queries to filter by date using the partition.
            # Skip the auto-inject if the user has already opted into the more
            # explicit {partition_date} / {partition_date_next} templating, since
            # APIs that need explicit param names (USGS, etc.) reject the extras
            # with 400 Unknown Parameter.
            if isinstance(_params_str, dict):
                _params_text = " ".join(str(v) for v in _params_str.values())
            else:
                _params_text = _params_str or ""
            _user_uses_templating = (
                "{partition_date}" in _params_text
                or "{partition_date_next}" in _params_text
                or "{partition_key}" in _params_text
                or "{partition_date}" in (_api_url or "")
                or "{partition_key}" in (_api_url or "")
            )
            if partition_date and not _user_uses_templating:
                params["date"] = partition_date.strftime("%Y-%m-%d")
                params["partition_date"] = partition_date.strftime("%Y-%m-%d")

            # Format-string templating in api_url and params values.
            from datetime import timedelta
            _template_vars = {
                "partition_key": (
                    str(context.partition_key) if context.has_partition_key else ""
                ),
                "partition_date": (
                    partition_date.strftime("%Y-%m-%d") if partition_date else ""
                ),
                "partition_date_next": (
                    (partition_date + timedelta(days=1)).strftime("%Y-%m-%d")
                    if partition_date
                    else ""
                ),
                # Expose every dim of a multi-partition key too, so users can
                # write `?carrier={carrier}&region={region}` in a static-dim
                # partition setup.
                **{k: str(v) for k, v in partition_dim_values.items()},
            }
            try:
                _resolved_url = _api_url.format(**_template_vars)
            except (KeyError, IndexError):
                _resolved_url = _api_url
            for k, v in list(params.items()):
                if isinstance(v, str):
                    try:
                        params[k] = v.format(**_template_vars)
                    except (KeyError, IndexError):
                        pass

            # Parse body
            body = None
            if _body_str:
                try:
                    body = json.loads(_body_str)
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid body JSON: {e}")
                    raise

            # Setup authentication
            auth = None
            if _auth_type == "basic" and _auth_username and _auth_password:
                auth = (_auth_username, _auth_password)
            elif _auth_type == "bearer" and _auth_token:
                headers["Authorization"] = f"Bearer {_auth_token}"

            # Make API request
            context.log.info(f"Fetching data from {_resolved_url}")
            context.log.info(f"Method: {_method}")

            try:
                response = requests.request(
                    method=_method,
                    url=_resolved_url,
                    headers=headers if headers else None,
                    params=params if params else None,
                    json=body if body else None,
                    auth=auth,
                    timeout=_timeout,
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

            # JMESPath response reshaping — `response_path` selects the array,
            # `column_map` extracts named columns per element. Together they
            # normalize a nested / heterogeneous response into a tabular batch
            # with a known schema. Lazy-import jmespath (optional dep).
            if _response_path or _column_map:
                try:
                    import jmespath
                except ImportError as e:
                    raise ImportError(
                        "response_path / column_map require jmespath. "
                        "Install with: pip install jmespath"
                    ) from e
                _rp = (_response_path or "").lstrip("$.").lstrip(".")
                array = jmespath.search(_rp, data) if _rp else data
                if array is None:
                    context.log.warning(
                        f"response_path {_response_path!r} matched nothing; "
                        f"emitting empty batch"
                    )
                    array = []
                elif not isinstance(array, list):
                    # Wrap a single object as a 1-element list so column_map
                    # still applies uniformly.
                    array = [array]
                if _column_map:
                    rows = [
                        {col: jmespath.search(expr, item) for col, expr in _column_map.items()}
                        for item in array
                    ]
                    data = rows
                else:
                    data = array

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
                "api_url": _api_url,
                "method": _method,
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

            # ── Sinks: write the batch to configured warehouse tables ────
            # Auto-detects DuckDB `.register()` fast path; falls back to
            # SQLAlchemy `to_sql`. Supports `mode: upsert_on_match` for
            # partition-rewrite idempotency (DELETE + INSERT in a tx).
            if sinks and output_format in ("dataframe", "csv", "parquet"):
                from contextlib import nullcontext
                # We already have `df` above for these output formats.
                sink_df = df.copy()  # avoid mutating the asset's return value
                for sink in sinks:
                    kind = (sink.get("kind") or "table").lower()
                    if kind != "table":
                        raise ValueError(
                            f"rest_api_fetcher sinks: only kind=table is supported "
                            f"(got {kind!r})"
                        )
                    sink_resource_key = sink.get("resource_key")
                    if not sink_resource_key:
                        raise ValueError("sink kind=table requires 'resource_key'")
                    sink_table = sink.get("table")
                    if not sink_table:
                        raise ValueError("sink kind=table requires 'table'")
                    sink_schema = sink.get("schema")
                    sink_if_exists = sink.get("if_exists", "append")
                    sink_mode = (sink.get("mode") or "").lower() or None
                    sink_match: List[str] = list(sink.get("match") or [])
                    if sink_mode == "upsert_on_match" and not sink_match:
                        raise ValueError(
                            "sink mode=upsert_on_match requires 'match: [col, ...]'"
                        )
                    # Inject partition dim values as columns before write.
                    _pdc = sink.get("partition_dim_columns") or {}
                    for col_name, dim_name in _pdc.items():
                        if dim_name in partition_dim_values:
                            sink_df[col_name] = partition_dim_values[dim_name]
                        elif dim_name == "partition_key" and context.has_partition_key:
                            sink_df[col_name] = str(context.partition_key)
                        else:
                            context.log.warning(
                                f"partition_dim_columns: dim {dim_name!r} not found "
                                f"on partition_key; column {col_name!r} left unset"
                            )

                    resource = getattr(context.resources, sink_resource_key)

                    def _acquire():
                        if hasattr(resource, "get_connection"):
                            gc = resource.get_connection()
                            return gc if hasattr(gc, "__enter__") else nullcontext(gc)
                        if hasattr(resource, "get_engine"):
                            eng = resource.get_engine()
                            return eng if hasattr(eng, "__enter__") else nullcontext(eng)
                        raise ValueError(
                            f"sink resource {sink_resource_key!r} must expose "
                            f".get_connection() or .get_engine()"
                        )

                    qualified = f"{sink_schema}.{sink_table}" if sink_schema else sink_table
                    with _acquire() as conn:
                        # Fast path: DuckDB .register() / .execute() / .unregister().
                        if (
                            hasattr(conn, "register")
                            and hasattr(conn, "execute")
                            and hasattr(conn, "unregister")
                        ):
                            conn.register("_rest_sink_batch", sink_df)
                            try:
                                if sink_mode == "upsert_on_match":
                                    conn.execute(
                                        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                                        f"SELECT * FROM _rest_sink_batch WHERE 1=0"
                                    )
                                    match_tuple = ", ".join(sink_match)
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(
                                            f"DELETE FROM {qualified} WHERE ({match_tuple}) IN "
                                            f"(SELECT DISTINCT {match_tuple} FROM _rest_sink_batch)"
                                        )
                                        conn.execute(
                                            f"INSERT INTO {qualified} "
                                            f"SELECT * FROM _rest_sink_batch"
                                        )
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                elif sink_if_exists == "replace":
                                    conn.execute(
                                        f"CREATE OR REPLACE TABLE {qualified} AS "
                                        f"SELECT * FROM _rest_sink_batch"
                                    )
                                else:
                                    conn.execute(
                                        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                                        f"SELECT * FROM _rest_sink_batch WHERE 1=0"
                                    )
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(
                                            f"INSERT INTO {qualified} "
                                            f"SELECT * FROM _rest_sink_batch"
                                        )
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                metadata[f"sink/{qualified}/fast_path"] = "duckdb-register"
                            finally:
                                try:
                                    conn.unregister("_rest_sink_batch")
                                except Exception:  # noqa: BLE001
                                    pass
                        else:
                            # SQLAlchemy fallback path.
                            if sink_mode == "upsert_on_match":
                                from sqlalchemy import text as _sa_text
                                distinct = sink_df[sink_match].drop_duplicates()
                                match_tuple = ", ".join(sink_match)
                                if len(distinct) > 0:
                                    placeholders = ", ".join(
                                        "(" + ", ".join(f":v{i}_{j}" for j in range(len(sink_match))) + ")"
                                        for i in range(len(distinct))
                                    )
                                    params_sql = {}
                                    for i, row in enumerate(distinct.itertuples(index=False)):
                                        for j, v in enumerate(row):
                                            params_sql[f"v{i}_{j}"] = v
                                    tx = conn.begin() if hasattr(conn, "begin") else None
                                    if tx is not None:
                                        with tx as _c:
                                            _c.execute(
                                                _sa_text(
                                                    f"DELETE FROM {qualified} "
                                                    f"WHERE ({match_tuple}) IN ({placeholders})"
                                                ),
                                                params_sql,
                                            )
                                            sink_df.to_sql(
                                                sink_table, _c, schema=sink_schema,
                                                if_exists="append", index=False,
                                            )
                                    else:
                                        sink_df.to_sql(
                                            sink_table, conn, schema=sink_schema,
                                            if_exists="append", index=False,
                                        )
                                else:
                                    sink_df.to_sql(
                                        sink_table, conn, schema=sink_schema,
                                        if_exists="append", index=False,
                                    )
                            else:
                                sink_df.to_sql(
                                    sink_table, conn, schema=sink_schema,
                                    if_exists=sink_if_exists, index=False,
                                )
                            metadata[f"sink/{qualified}/fast_path"] = "sqlalchemy-to_sql"

                    metadata[f"sink/{qualified}/rows"] = len(sink_df)
                    metadata[f"sink/{qualified}/mode"] = (
                        f"upsert_on_match({','.join(sink_match)})"
                        if sink_mode == "upsert_on_match"
                        else sink_if_exists
                    )
                    context.log.info(
                        f"sink → {qualified} (via {sink_resource_key}, "
                        f"{metadata[f'sink/{qualified}/mode']}, "
                        f"{len(sink_df)} rows)"
                    )

            context.add_output_metadata(metadata)

            # Return with sample metadata if requested and output is a DataFrame
            if include_preview and output_format == "dataframe" and isinstance(result, pd.DataFrame) and len(result) > 0:
                context.add_output_metadata({
                        "row_count": len(result),
                        "columns": result.columns.tolist(),
                        "preview": MetadataValue.md(result.head().to_markdown())
                    })
                return result
            else:
                return result

        return Definitions(assets=[rest_api_asset])
