from dataclasses import dataclass
from typing import Optional
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
class PdfTextExtractorComponent(Component, Model, Resolvable):
    """Extract text from a column containing PDF file paths or PDF bytes using pdfplumber."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
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
    column: str = Field(description="Column containing PDF file paths or bytes")
    output_column: str = Field(
        default="pdf_text",
        description="Column to write extracted text to.",
    )
    pages: Optional[str] = Field(
        default=None,
        description="Comma-separated page numbers or ranges like '1,3,5-10'. Default is all pages.",
    )
    include_page_numbers: bool = Field(
        default=False,
        description="Prefix each page's text with '[Page N]'.",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        column = self.column
        output_column = self.output_column
        pages = self.pages
        include_page_numbers = self.include_page_numbers

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

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
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
            try:
                import pdfplumber
            except ImportError:
                raise ImportError("Install pdfplumber: pip install pdfplumber")

            import io

            if column not in upstream.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            def parse_page_spec(spec: Optional[str]) -> Optional[list]:
                if spec is None:
                    return None
                page_nums = []
                for part in spec.split(","):
                    part = part.strip()
                    if "-" in part:
                        start, end = part.split("-", 1)
                        page_nums.extend(range(int(start), int(end) + 1))
                    else:
                        page_nums.append(int(part))
                # pdfplumber uses 0-based index
                return [p - 1 for p in page_nums]

            target_pages = parse_page_spec(pages)

            def extract_text(value) -> Optional[str]:
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return None
                try:
                    if isinstance(value, bytes):
                        pdf_source = io.BytesIO(value)
                    else:
                        pdf_source = str(value)

                    with pdfplumber.open(pdf_source) as pdf:
                        all_pages = pdf.pages
                        selected = (
                            [all_pages[i] for i in target_pages if i < len(all_pages)]
                            if target_pages is not None
                            else all_pages
                        )
                        parts = []
                        for idx, page in enumerate(selected):
                            text = page.extract_text() or ""
                            if include_page_numbers:
                                page_num = (target_pages[idx] + 1) if target_pages else (idx + 1)
                                parts.append(f"[Page {page_num}]\n{text}")
                            else:
                                parts.append(text)
                        return "\n".join(parts)
                except Exception as e:
                    context.log.warning(f"Failed to extract PDF text: {e}")
                    return None

            df = upstream.copy()
            df[output_column] = df[column].apply(extract_text)

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "columns": MetadataValue.int(len(df.columns)),
                "extracted": MetadataValue.int(int(df[output_column].notna().sum())),
            })
            return df

        return Definitions(assets=[_asset])
