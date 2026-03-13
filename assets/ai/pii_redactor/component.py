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
class PiiRedactorComponent(Component, Model, Resolvable):
    """Detect and redact PII from text columns, replacing entities with type placeholders or custom values."""

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
    text_column: str = Field(description="Column containing text to redact PII from")
    output_column: Optional[str] = Field(
        default=None,
        description="Column to write redacted text. Defaults to overwriting text_column.",
    )
    entity_types: Optional[List[str]] = Field(
        default=None,
        description="Filter to these entity types. Default=all.",
    )
    replacement_style: str = Field(
        default="placeholder",
        description="How to replace PII: placeholder=<PERSON>, mask=****, hash=SHA256 prefix, synthetic=fake value",
    )
    language: str = Field(default="en", description="Language of the text")
    score_threshold: float = Field(default=0.5, description="Minimum confidence score for detected entities")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        text_column = self.text_column
        output_column = self.output_column
        entity_types = self.entity_types
        replacement_style = self.replacement_style
        language = self.language
        score_threshold = self.score_threshold

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
                from presidio_analyzer import AnalyzerEngine
                from presidio_anonymizer import AnonymizerEngine
                from presidio_anonymizer.entities import OperatorConfig
            except ImportError:
                raise ImportError("pip install presidio-analyzer>=2.2.0 presidio-anonymizer>=2.2.0")

            if text_column not in upstream.columns:
                raise ValueError(f"Column '{text_column}' not found in DataFrame.")

            analyzer = AnalyzerEngine()
            anonymizer = AnonymizerEngine()
            out_col = output_column if output_column else text_column

            def _build_operators(results, text: str) -> dict:
                ops = {}
                for r in results:
                    if replacement_style == "placeholder":
                        ops[r.entity_type] = OperatorConfig("replace", {"new_value": f"<{r.entity_type}>"})
                    elif replacement_style == "mask":
                        ops[r.entity_type] = OperatorConfig("mask", {
                            "type": "mask",
                            "masking_char": "*",
                            "chars_to_mask": r.end - r.start,
                            "from_end": False,
                        })
                    elif replacement_style == "hash":
                        ops[r.entity_type] = OperatorConfig("hash", {"hash_type": "sha256"})
                    else:
                        ops[r.entity_type] = OperatorConfig("replace", {"new_value": f"<{r.entity_type}>"})
                return ops

            def redact_text(text: object) -> str:
                if text is None or (isinstance(text, float) and pd.isna(text)):
                    return text
                try:
                    text_str = str(text)
                    results = analyzer.analyze(
                        text=text_str,
                        language=language,
                        entities=entity_types,
                        score_threshold=score_threshold,
                    )
                    if not results:
                        return text_str
                    operators = _build_operators(results, text_str)
                    anonymized = anonymizer.anonymize(
                        text=text_str,
                        analyzer_results=results,
                        operators=operators,
                    )
                    return anonymized.text
                except Exception as e:
                    context.log.warning(f"PII redaction failed for row: {e}")
                    return str(text)

            df = upstream.copy()
            df[out_col] = df[text_column].apply(redact_text)

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "replacement_style": MetadataValue.text(replacement_style),
            })
            return df

        return Definitions(assets=[_asset])
