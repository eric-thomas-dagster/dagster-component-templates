"""LiteLLM Audio Transcription Component.

Transcribe audio files using Whisper via LiteLLM.
Processes a column of audio file paths and writes transcriptions back.
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
class LitellmAudioTranscriptionComponent(Component, Model, Resolvable):
    """Transcribe audio files using Whisper via LiteLLM.

    Reads a DataFrame with a column of local audio file paths, opens each file,
    calls litellm.transcription(), and writes the transcribed text to a new column.

    Example:
        ```yaml
        type: dagster_component_templates.LitellmAudioTranscriptionComponent
        attributes:
          asset_name: transcribed_interviews
          upstream_asset_key: interview_audio_files
          audio_path_column: file_path
          output_column: transcription
          model: whisper-1
          language: en
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    audio_path_column: str = Field(description="Column containing local audio file paths")
    output_column: str = Field(default="transcription", description="Column to write transcribed text")
    model: str = Field(default="whisper-1", description="Transcription model (e.g. whisper-1)")
    language: Optional[str] = Field(default=None, description="ISO 639-1 language code hint (e.g. en, es, fr)")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var name for API key")
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
        audio_path_column = self.audio_path_column
        output_column = self.output_column
        model = self.model
        language = self.language
        api_key_env_var = self.api_key_env_var
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

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            kinds={"ai", "audio"},
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
                import litellm
            except ImportError:
                raise ImportError("pip install litellm>=1.0.0")

            import os

            df = upstream.copy()
            context.log.info(f"Transcribing {len(df)} audio files with model={model}")

            kwargs: dict = {"model": model}
            if language:
                kwargs["language"] = language
            if api_key_env_var:
                kwargs["api_key"] = os.environ[api_key_env_var]

            transcriptions = []
            for i, row in df.iterrows():
                audio_path = str(row[audio_path_column])
                with open(audio_path, "rb") as audio_file:
                    response = litellm.transcription(file=audio_file, **kwargs)
                transcriptions.append(response.text)

                if (i + 1) % 10 == 0:
                    context.log.info(f"Transcribed {i + 1}/{len(df)} files")

            df[output_column] = transcriptions

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "model": MetadataValue.text(model),
                "output_column": MetadataValue.text(output_column),
            })
            return df

        return Definitions(assets=[_asset])
