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
class AudioTranscriberComponent(Component, Model, Resolvable):
    """Transcribe audio files from a file path column using OpenAI Whisper (local model)."""

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
    audio_path_column: str = Field(description="Column containing local audio file paths")
    output_column: str = Field(default="transcription", description="Column to write transcribed text")
    language_column: Optional[str] = Field(
        default=None,
        description="Column to write the detected language code",
    )
    model_size: str = Field(
        default="base",
        description="Local Whisper model size: tiny, base, small, medium, or large",
    )
    language: Optional[str] = Field(
        default=None,
        description="Hint language to skip auto-detection (ISO 639-1 code, e.g. 'en')",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        audio_path_column = self.audio_path_column
        output_column = self.output_column
        language_column = self.language_column
        model_size = self.model_size
        language = self.language

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
                import whisper
            except ImportError:
                raise ImportError("pip install openai-whisper>=20230918")

            if audio_path_column not in upstream.columns:
                raise ValueError(f"Column '{audio_path_column}' not found in DataFrame.")

            context.log.info(f"Loading Whisper model: {model_size}")
            model = whisper.load_model(model_size)

            df = upstream.copy()
            transcriptions = []
            detected_languages = []

            for path in df[audio_path_column]:
                if path is None or (isinstance(path, float) and pd.isna(path)):
                    transcriptions.append(None)
                    detected_languages.append(None)
                    continue
                try:
                    kwargs = {}
                    if language:
                        kwargs["language"] = language
                    result = model.transcribe(str(path), **kwargs)
                    transcriptions.append(result.get("text", ""))
                    detected_languages.append(result.get("language"))
                except Exception as e:
                    context.log.warning(f"Failed to transcribe '{path}': {e}")
                    transcriptions.append(None)
                    detected_languages.append(None)

            df[output_column] = transcriptions
            if language_column:
                df[language_column] = detected_languages

            successful = sum(1 for t in transcriptions if t is not None)
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "successful_transcriptions": MetadataValue.int(successful),
                "model_size": MetadataValue.text(model_size),
            })
            return df

        return Definitions(assets=[_asset])
