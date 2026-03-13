from dataclasses import dataclass
from typing import Optional, List
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
class MarkdownStripperComponent(Component, Model, Resolvable):
    """Strip markdown formatting from text columns, converting to plain text or HTML."""

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
    columns: List[str] = Field(description="Columns to process")
    mode: str = Field(
        default="strip",
        description="'strip' removes markdown syntax, 'to_html' converts MD to HTML, 'to_plaintext' does full conversion.",
    )
    new_column_suffix: Optional[str] = Field(
        default=None,
        description="If set, write results to new columns with this suffix instead of overwriting.",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        columns = self.columns
        mode = self.mode
        new_column_suffix = self.new_column_suffix

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
            import re

            valid_modes = ("strip", "to_html", "to_plaintext")
            if mode not in valid_modes:
                raise ValueError(f"mode must be one of {valid_modes}, got: {mode}")

            # Try markdownify for to_html mode, else use markdown package
            if mode == "to_html":
                try:
                    import markdown as _md
                    def convert(text: str) -> str:
                        return _md.markdown(text)
                except ImportError:
                    raise ImportError("Install markdown: pip install markdown")
            elif mode == "to_plaintext":
                try:
                    import markdown as _md
                    try:
                        from bs4 import BeautifulSoup as _BS
                        def convert(text: str) -> str:
                            html = _md.markdown(text)
                            return _BS(html, "html.parser").get_text(separator=" ", strip=True)
                    except ImportError:
                        def convert(text: str) -> str:
                            html = _md.markdown(text)
                            return re.sub(r"<[^>]+>", " ", html).strip()
                except ImportError:
                    raise ImportError("Install markdown: pip install markdown")
            else:
                # strip mode — use regex fallback
                def convert(text: str) -> str:
                    # Remove code blocks
                    text = re.sub(r"```[\s\S]*?```", "", text)
                    text = re.sub(r"`[^`]+`", "", text)
                    # Remove headers
                    text = re.sub(r"#{1,6}\s*", "", text)
                    # Remove bold/italic
                    text = re.sub(r"\*{1,3}([^*]+)\*{1,3}", r"\1", text)
                    text = re.sub(r"_{1,3}([^_]+)_{1,3}", r"\1", text)
                    # Remove links
                    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
                    # Remove images
                    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", text)
                    # Remove blockquotes
                    text = re.sub(r"^\s*>+\s?", "", text, flags=re.MULTILINE)
                    # Remove horizontal rules
                    text = re.sub(r"^\s*[-*_]{3,}\s*$", "", text, flags=re.MULTILINE)
                    # Remove list markers
                    text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE)
                    text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE)
                    return text.strip()

            def process_value(val) -> Optional[str]:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                return convert(str(val))

            df = upstream.copy()
            for col in columns:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue
                result = df[col].apply(process_value)
                if new_column_suffix:
                    df[f"{col}{new_column_suffix}"] = result
                else:
                    df[col] = result

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "columns": MetadataValue.int(len(df.columns)),
            })
            return df

        return Definitions(assets=[_asset])
