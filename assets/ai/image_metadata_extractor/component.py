"""Image Metadata Extractor Component.

Extract technical metadata from images: dimensions, format, color mode, EXIF data.
"""

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


@dataclass
class ImageMetadataExtractorComponent(Component, Model, Resolvable):
    """Component for extracting technical metadata from images.

    Extracts dimensions, format, color mode, EXIF data (GPS coordinates, camera model,
    capture date) from image files. Adds all metadata as new DataFrame columns.

    Features:
    - Image dimensions (width, height), format, color mode
    - File size in bytes
    - EXIF metadata: capture date, camera model
    - GPS parsing: converts DMS EXIF tags to decimal lat/lon
    - Optional color histogram column

    Use Cases:
    - Photo library organization and enrichment
    - GDPR/compliance GPS data auditing
    - Camera inventory and usage reporting
    - Dataset quality assessment
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column: str = Field(description="Column with image file paths")
    extract_exif: bool = Field(default=True, description="Extract EXIF metadata")
    extract_gps: bool = Field(
        default=True, description="Parse GPS EXIF tags into lat/lon columns"
    )
    output_prefix: str = Field(
        default="img_", description="Prefix for output columns"
    )
    include_histogram: bool = Field(
        default=False, description="Add color histogram as a column"
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        image_column = self.image_column
        extract_exif = self.extract_exif
        extract_gps = self.extract_gps
        output_prefix = self.output_prefix
        include_histogram = self.include_histogram

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
        _comp_name = "image_metadata_extractor"  # component directory name
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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=self.group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            import os

            try:
                from PIL import Image, ExifTags
            except ImportError:
                raise ImportError("Pillow required: pip install Pillow")

            def dms_to_decimal(dms_values, ref):
                """Convert EXIF DMS tuple to decimal degrees."""
                try:
                    d = float(dms_values[0])
                    m = float(dms_values[1])
                    s = float(dms_values[2])
                    decimal = d + m / 60.0 + s / 3600.0
                    if ref in ("S", "W"):
                        decimal = -decimal
                    return round(decimal, 6)
                except Exception:
                    return None

            # Build reverse EXIF tag lookup
            tag_lookup = {v: k for k, v in ExifTags.TAGS.items()}

            df = upstream.copy()
            records = []

            for path in df[image_column]:
                row_data = {}
                try:
                    img = Image.open(str(path))
                    row_data[f"{output_prefix}width"] = img.width
                    row_data[f"{output_prefix}height"] = img.height
                    row_data[f"{output_prefix}format"] = img.format
                    row_data[f"{output_prefix}mode"] = img.mode
                    try:
                        row_data[f"{output_prefix}file_size_bytes"] = os.path.getsize(str(path))
                    except Exception:
                        row_data[f"{output_prefix}file_size_bytes"] = None

                    if extract_exif or extract_gps:
                        exif_data = {}
                        try:
                            raw_exif = img._getexif()
                            if raw_exif:
                                exif_data = {
                                    ExifTags.TAGS.get(tag_id, str(tag_id)): val
                                    for tag_id, val in raw_exif.items()
                                }
                        except Exception:
                            pass

                        if extract_exif:
                            row_data[f"{output_prefix}capture_date"] = exif_data.get(
                                "DateTimeOriginal", exif_data.get("DateTime", None)
                            )
                            row_data[f"{output_prefix}camera_model"] = exif_data.get(
                                "Model", exif_data.get("Make", None)
                            )

                        if extract_gps:
                            gps_info = exif_data.get("GPSInfo", None)
                            lat = lon = None
                            if gps_info and isinstance(gps_info, dict):
                                try:
                                    # GPSInfo keys are tag IDs
                                    gps_tags = {
                                        ExifTags.GPSTAGS.get(k, k): v
                                        for k, v in gps_info.items()
                                    }
                                    if "GPSLatitude" in gps_tags and "GPSLatitudeRef" in gps_tags:
                                        lat = dms_to_decimal(
                                            gps_tags["GPSLatitude"],
                                            gps_tags["GPSLatitudeRef"],
                                        )
                                    if "GPSLongitude" in gps_tags and "GPSLongitudeRef" in gps_tags:
                                        lon = dms_to_decimal(
                                            gps_tags["GPSLongitude"],
                                            gps_tags["GPSLongitudeRef"],
                                        )
                                except Exception:
                                    pass
                            row_data[f"{output_prefix}gps_lat"] = lat
                            row_data[f"{output_prefix}gps_lon"] = lon

                    if include_histogram:
                        try:
                            hist = img.histogram()
                            row_data[f"{output_prefix}histogram"] = hist
                        except Exception:
                            row_data[f"{output_prefix}histogram"] = None

                except Exception as e:
                    context.log.warning(f"Metadata extraction failed for {path}: {e}")
                    row_data[f"{output_prefix}width"] = None
                    row_data[f"{output_prefix}height"] = None
                    row_data[f"{output_prefix}format"] = None
                    row_data[f"{output_prefix}mode"] = None
                    row_data[f"{output_prefix}file_size_bytes"] = None
                    if extract_exif:
                        row_data[f"{output_prefix}capture_date"] = None
                        row_data[f"{output_prefix}camera_model"] = None
                    if extract_gps:
                        row_data[f"{output_prefix}gps_lat"] = None
                        row_data[f"{output_prefix}gps_lon"] = None

                records.append(row_data)

            meta_df = pd.DataFrame(records)
            for col in meta_df.columns:
                df[col] = meta_df[col].values

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "columns_added": list(meta_df.columns),
                    "preview": MetadataValue.md(
                        df[[image_column] + list(meta_df.columns)[:4]].head(5).to_markdown()
                    ),
                }
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
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
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


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
