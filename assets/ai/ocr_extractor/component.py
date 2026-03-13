"""OCR Extractor Component.

Extract text from images using Tesseract OCR via pytesseract.
"""

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
class OcrExtractorComponent(Component, Model, Resolvable):
    """Component for extracting text from images using Tesseract OCR.

    Processes a column of image file paths and writes extracted text to a new column.
    Optionally auto-preprocesses images (grayscale + threshold) using PIL for better accuracy.

    Features:
    - Configurable Tesseract language codes (including multi-language e.g. "eng+fra")
    - Configurable page segmentation mode (PSM)
    - Optional auto-preprocessing with PIL (grayscale, threshold)
    - Optional confidence score column

    Use Cases:
    - Document digitization pipelines
    - Receipt / invoice text extraction
    - Scanned form processing
    - License plate recognition
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column: str = Field(description="Column with image file paths")
    output_column: str = Field(
        default="ocr_text", description="Column to write extracted text"
    )
    language: str = Field(
        default="eng",
        description="Tesseract language code(s) e.g. 'eng', 'eng+fra'",
    )
    psm: int = Field(
        default=3,
        description="Tesseract page segmentation mode (0-13; 3=auto, 6=single block, 11=single line)",
    )
    preprocessed: bool = Field(
        default=False,
        description="If False, auto-preprocess (grayscale, threshold) using PIL before OCR",
    )
    confidence_column: Optional[str] = Field(
        default=None,
        description="If set, write mean OCR confidence to this column",
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        image_column = self.image_column
        output_column = self.output_column
        language = self.language
        psm = self.psm
        preprocessed = self.preprocessed
        confidence_column = self.confidence_column

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
            try:
                import pytesseract
            except ImportError:
                raise ImportError("pytesseract required: pip install pytesseract")
            try:
                from PIL import Image
            except ImportError:
                raise ImportError("Pillow required: pip install Pillow")

            df = upstream.copy()
            texts = []
            confidences = []
            config = f"--psm {psm}"

            for path in df[image_column]:
                try:
                    img = Image.open(str(path))
                    if not preprocessed:
                        img = img.convert("L")
                        img = img.point(lambda x: 0 if x < 128 else 255, "1")
                        img = img.convert("L")

                    text = pytesseract.image_to_string(img, lang=language, config=config)
                    texts.append(text.strip())

                    if confidence_column:
                        data = pytesseract.image_to_data(
                            img, lang=language, config=config,
                            output_type=pytesseract.Output.DICT,
                        )
                        confs = [c for c in data["conf"] if isinstance(c, (int, float)) and c >= 0]
                        confidences.append(sum(confs) / len(confs) if confs else 0.0)
                except Exception as e:
                    context.log.warning(f"OCR failed for {path}: {e}")
                    texts.append(None)
                    if confidence_column:
                        confidences.append(None)

            df[output_column] = texts
            if confidence_column:
                df[confidence_column] = confidences

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "language": language,
                    "psm": psm,
                    "preview": MetadataValue.md(df[[image_column, output_column]].head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
