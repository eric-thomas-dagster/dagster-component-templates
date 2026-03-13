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
class LanguageDetectorComponent(Component, Model, Resolvable):
    """Detect the language of text in a column."""

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
    text_column: str = Field(description="Column containing text to detect language from")
    output_column: str = Field(default="language", description="Column to write ISO 639-1 language code e.g. 'en', 'es'")
    confidence_column: Optional[str] = Field(
        default=None,
        description="If set, write confidence score to this column",
    )
    backend: str = Field(
        default="langdetect",
        description="Detection backend: langdetect, fasttext, or lingua",
    )
    fallback_language: str = Field(
        default="unknown",
        description="Value to use when detection fails",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        text_column = self.text_column
        output_column = self.output_column
        confidence_column = self.confidence_column
        backend = self.backend
        fallback_language = self.fallback_language

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
            if text_column not in upstream.columns:
                raise ValueError(f"Column '{text_column}' not found in DataFrame.")

            if backend == "langdetect":
                try:
                    from langdetect import detect, detect_langs
                except ImportError:
                    raise ImportError("pip install langdetect>=1.0.9")

                def detect_fn(text: object):
                    if text is None or (isinstance(text, float) and pd.isna(text)) or str(text).strip() == "":
                        return fallback_language, None
                    try:
                        if confidence_column:
                            langs = detect_langs(str(text))
                            if langs:
                                return langs[0].lang, round(langs[0].prob, 4)
                            return fallback_language, None
                        else:
                            return detect(str(text)), None
                    except Exception:
                        return fallback_language, None

            elif backend == "fasttext":
                try:
                    import fasttext
                except ImportError:
                    raise ImportError("pip install fasttext")

                def detect_fn(text: object):
                    if text is None or (isinstance(text, float) and pd.isna(text)):
                        return fallback_language, None
                    try:
                        predictions = fasttext.predict(str(text).replace("\n", " "))
                        lang = predictions[0][0].replace("__label__", "")
                        conf = round(float(predictions[1][0]), 4) if confidence_column else None
                        return lang, conf
                    except Exception:
                        return fallback_language, None

            elif backend == "lingua":
                try:
                    from lingua import LanguageDetectorBuilder
                except ImportError:
                    raise ImportError("pip install lingua-language-detector")

                detector = LanguageDetectorBuilder.from_all_languages().build()

                def detect_fn(text: object):
                    if text is None or (isinstance(text, float) and pd.isna(text)):
                        return fallback_language, None
                    try:
                        lang = detector.detect_language_of(str(text))
                        iso = lang.iso_code_639_1.name.lower() if lang else fallback_language
                        conf = None
                        if confidence_column:
                            conf_val = detector.compute_language_confidence(str(text), lang)
                            conf = round(conf_val, 4) if conf_val else None
                        return iso, conf
                    except Exception:
                        return fallback_language, None
            else:
                raise ValueError(f"Unknown backend: {backend}. Choose from langdetect, fasttext, lingua.")

            df = upstream.copy()
            results = df[text_column].apply(detect_fn)
            df[output_column] = results.apply(lambda x: x[0])
            if confidence_column:
                df[confidence_column] = results.apply(lambda x: x[1])

            unique_langs = df[output_column].nunique()
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "unique_languages": MetadataValue.int(unique_langs),
            })
            return df

        return Definitions(assets=[_asset])
