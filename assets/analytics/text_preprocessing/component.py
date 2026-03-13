"""Text Preprocessing.

Clean and normalize text data for NLP tasks.
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
class TextPreprocessingComponent(Component, Model, Resolvable):
    """Clean and normalize text data for NLP tasks."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column name containing the raw text to process")
    output_column: Optional[str] = Field(default=None, description="Output column name (defaults to overwriting text_column)")
    lowercase: bool = Field(default=True, description="Convert text to lowercase")
    remove_punctuation: bool = Field(default=True, description="Remove punctuation characters")
    remove_numbers: bool = Field(default=False, description="Remove numeric characters")
    remove_stopwords: bool = Field(default=True, description="Remove common stopwords")
    language: str = Field(default="english", description="Stopword language for NLTK")
    stem: bool = Field(default=False, description="Apply Porter stemming to tokens")
    lemmatize: bool = Field(default=False, description="Apply WordNet lemmatization to tokens")
    min_token_length: int = Field(default=2, description="Remove tokens shorter than this length")
    output_tokens: bool = Field(default=False, description="If True, output as list of tokens; if False, rejoin as string")
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

    @classmethod
    def get_description(cls) -> str:
        return "Clean and normalize text data for NLP tasks using NLTK."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        text_column = self.text_column
        output_column = self.output_column
        lowercase = self.lowercase
        remove_punctuation = self.remove_punctuation
        remove_numbers = self.remove_numbers
        remove_stopwords = self.remove_stopwords
        language = self.language
        stem = self.stem
        lemmatize = self.lemmatize
        min_token_length = self.min_token_length
        output_tokens = self.output_tokens
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

            try:
                import nltk
                nltk.download("stopwords", quiet=True)
                nltk.download("punkt", quiet=True)
                nltk.download("wordnet", quiet=True)
                from nltk.corpus import stopwords
                from nltk.stem import PorterStemmer, WordNetLemmatizer
            except ImportError as e:
                raise ImportError("nltk is required: pip install nltk") from e

            df = upstream.copy()

            stop_words = set(stopwords.words(language)) if remove_stopwords else set()
            stemmer = PorterStemmer() if stem else None
            lemmatizer = WordNetLemmatizer() if lemmatize else None

            def process(text):
                if pd.isna(text):
                    return text
                text = str(text)
                if lowercase:
                    text = text.lower()
                if remove_punctuation:
                    text = re.sub(r"[^\w\s]", " ", text)
                if remove_numbers:
                    text = re.sub(r"\d+", " ", text)
                tokens = text.split()
                tokens = [t for t in tokens if t not in stop_words and len(t) >= min_token_length]
                if stem and stemmer:
                    tokens = [stemmer.stem(t) for t in tokens]
                if lemmatize and lemmatizer:
                    tokens = [lemmatizer.lemmatize(t) for t in tokens]
                return tokens if output_tokens else " ".join(tokens)

            out_col = output_column or text_column
            df[out_col] = df[text_column].apply(process)

            non_null = df[out_col].notna().sum()
            avg_len = df[out_col].dropna().apply(lambda x: len(x) if isinstance(x, list) else len(x.split())).mean()

            context.add_output_metadata({
                "rows_processed": MetadataValue.int(int(non_null)),
                "avg_tokens_per_row": MetadataValue.float(float(avg_len) if not pd.isna(avg_len) else 0.0),
                "output_column": MetadataValue.text(out_col),
                "stopwords_removed": MetadataValue.bool(remove_stopwords),
                "stemmed": MetadataValue.bool(stem),
                "lemmatized": MetadataValue.bool(lemmatize),
            })

            return df

        return Definitions(assets=[_asset])
