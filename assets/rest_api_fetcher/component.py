"""REST API Fetcher Asset Component.

Fetch data from REST APIs and materialize as Dagster assets.
Supports authentication, pagination, caching, and various output formats.
"""

import json
from typing import Optional, Dict, Any
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
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
        description="Output format: 'json', 'dataframe', 'csv', or 'parquet'"
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

    include_sample_metadata: bool = Field(
        default=False,
        description="Include sample data preview in metadata when output_format is 'dataframe' (first 5 rows as markdown table and interactive preview)"
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
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
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

            # Add partition date to params if available
            # This allows API queries to filter by date using the partition
            if partition_date:
                params["date"] = partition_date.strftime("%Y-%m-%d")
                params["partition_date"] = partition_date.strftime("%Y-%m-%d")

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
            context.log.info(f"Fetching data from {api_url}")
            context.log.info(f"Method: {method}")

            try:
                response = requests.request(
                    method=method,
                    url=api_url,
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

            # Parse response
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
            if output_format == "json":
                result = data

            elif output_format == "dataframe":
                if isinstance(data, list):
                    result = pd.DataFrame(data)
                elif isinstance(data, dict):
                    result = pd.DataFrame([data])
                else:
                    context.log.error(f"Cannot convert {type(data)} to DataFrame")
                    raise ValueError(f"Cannot convert {type(data)} to DataFrame")

            elif output_format == "csv":
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict):
                    df = pd.DataFrame([data])
                else:
                    raise ValueError(f"Cannot convert {type(data)} to CSV")

                result = df.to_csv(index=False)

            elif output_format == "parquet":
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict):
                    df = pd.DataFrame([data])
                else:
                    raise ValueError(f"Cannot convert {type(data)} to Parquet")

                # Convert to parquet bytes
                buffer = BytesIO()
                df.to_parquet(buffer, index=False)
                result = buffer.getvalue()

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
            if include_sample and output_format == "dataframe" and isinstance(result, pd.DataFrame) and len(result) > 0:
                return Output(
                    value=result,
                    metadata={
                        "row_count": len(result),
                        "columns": result.columns.tolist(),
                        "sample": MetadataValue.md(result.head().to_markdown()),
                        "preview": MetadataValue.dataframe(result.head())
                    }
                )
            else:
                return result

        return Definitions(assets=[rest_api_asset])
