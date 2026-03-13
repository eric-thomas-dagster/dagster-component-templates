"""Geocoder Component.

Geocode addresses in a DataFrame to latitude/longitude coordinates using
Nominatim (free), Google Maps, or HERE geocoding APIs.
"""

import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
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


class GeocoderComponent(Component, Model, Resolvable):
    """Component for geocoding addresses to latitude/longitude coordinates.

    Adds lat/lng columns to an upstream DataFrame by geocoding an address column
    using Nominatim (OpenStreetMap), Google Maps, or HERE geocoding APIs.

    Example:
        ```yaml
        type: dagster_component_templates.GeocoderComponent
        attributes:
          asset_name: geocoded_customers
          upstream_asset_key: customers
          address_column: full_address
          provider: nominatim
          lat_column: latitude
          lng_column: longitude
          group_name: analytics
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with address data"
    )

    address_column: str = Field(
        description="Column with full address strings to geocode"
    )

    provider: str = Field(
        default="nominatim",
        description="Geocoding provider: 'nominatim' (free), 'google' (API key required), 'here' (API key required)"
    )

    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable name for API key (for paid providers)"
    )

    user_agent: str = Field(
        default="dagster_geocoder",
        description="User agent string required for Nominatim"
    )

    lat_column: str = Field(
        default="latitude",
        description="Name of output latitude column"
    )

    lng_column: str = Field(
        default="longitude",
        description="Name of output longitude column"
    )

    country_column: Optional[str] = Field(
        default=None,
        description="Optional column name to add normalized country name"
    )

    timeout: int = Field(
        default=10,
        description="Request timeout in seconds"
    )

    batch_delay: float = Field(
        default=1.0,
        description="Seconds between requests to respect rate limits"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        address_column = self.address_column
        provider = self.provider
        api_key_env_var = self.api_key_env_var
        user_agent = self.user_agent
        lat_column = self.lat_column
        lng_column = self.lng_column
        country_column = self.country_column
        timeout = self.timeout
        batch_delay = self.batch_delay
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
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
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "geocoder"  # component directory name
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


        @asset(
            name=asset_name,
            description=f"Geocoded addresses using {provider}",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def geocoder_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Geocode address column in the upstream DataFrame."""
            try:
                from geopy.geocoders import Nominatim, GoogleV3, Here
                from geopy.exc import GeocoderTimedOut
            except ImportError:
                raise ImportError("geopy is required: pip install geopy")

            if provider == "nominatim":
                geolocator = Nominatim(user_agent=user_agent, timeout=timeout)
            elif provider == "google":
                api_key = os.environ.get(api_key_env_var or "GOOGLE_MAPS_API_KEY")
                geolocator = GoogleV3(api_key=api_key, timeout=timeout)
            elif provider == "here":
                api_key = os.environ.get(api_key_env_var or "HERE_API_KEY")
                geolocator = Here(apikey=api_key, timeout=timeout)
            else:
                raise ValueError(f"Unknown provider '{provider}'. Use: nominatim, google, here")

            df = upstream.copy()
            lats, lngs, countries = [], [], []

            context.log.info(f"Geocoding {len(df)} addresses using {provider}")

            success_count = 0
            for i, address in enumerate(df[address_column]):
                try:
                    location = geolocator.geocode(str(address))
                    if location:
                        lats.append(location.latitude)
                        lngs.append(location.longitude)
                        country = location.raw.get("display_name", "").split(",")[-1].strip() if country_column else None
                        countries.append(country)
                        success_count += 1
                    else:
                        lats.append(None)
                        lngs.append(None)
                        countries.append(None)
                except Exception as e:
                    context.log.warning(f"Failed to geocode '{address}': {e}")
                    lats.append(None)
                    lngs.append(None)
                    countries.append(None)

                time.sleep(batch_delay)

                if (i + 1) % 10 == 0:
                    context.log.info(f"Progress: {i + 1}/{len(df)} addresses processed")

            df[lat_column] = lats
            df[lng_column] = lngs
            if country_column:
                df[country_column] = countries

            success_rate = success_count / len(df) * 100 if len(df) > 0 else 0
            context.log.info(f"Geocoding complete: {success_count}/{len(df)} succeeded ({success_rate:.1f}%)")


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
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[geocoder_asset])


        return Definitions(assets=[geocoder_asset], asset_checks=list(_schema_checks))
