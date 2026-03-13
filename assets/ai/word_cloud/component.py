"""Word Cloud Component.

Compute word frequency tables, top-N word lists, or TF-IDF weighted word tables from text data.
"""

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
class WordCloudComponent(Component, Model, Resolvable):
    """Component for building word frequency and TF-IDF tables from text.

    Tokenizes, cleans, and counts words across a text column, returning a
    DataFrame suitable for word cloud visualization or further analysis.

    Features:
    - Three output modes: frequency_table, top_n, tfidf
    - Stop word removal via NLTK
    - Custom additional stop words
    - Minimum word length filter
    - Minimum frequency threshold

    Use Cases:
    - Word cloud visualization data
    - Content analysis
    - Topic identification
    - SEO keyword analysis
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing text")
    output_mode: str = Field(
        default="frequency_table",
        description="Output mode: 'frequency_table' (word→frequency), 'top_n' (top N words), 'tfidf' (TF-IDF weighted)",
    )
    top_n: int = Field(default=100, description="Number of top words for output_mode='top_n'")
    min_frequency: int = Field(
        default=1, description="Exclude words with fewer occurrences than this"
    )
    stop_words: Optional[List[str]] = Field(
        default=None, description="Additional stop words to exclude"
    )
    language: str = Field(
        default="english", description="NLTK stopword language e.g. 'english', 'french'"
    )
    min_word_length: int = Field(default=2, description="Minimum word character length")
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
        text_column = self.text_column
        output_mode = self.output_mode
        top_n = self.top_n
        min_frequency = self.min_frequency
        stop_words = self.stop_words
        language = self.language
        min_word_length = self.min_word_length

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
            import re
            from collections import Counter

            try:
                from nltk.corpus import stopwords
                import nltk
                nltk.download("stopwords", quiet=True)
                stop = set(stopwords.words(language))
            except Exception:
                context.log.warning("NLTK stopwords unavailable; proceeding without stop words")
                stop = set()

            if stop_words:
                stop.update(stop_words)

            context.log.info(
                f"Building word frequencies from {len(upstream)} rows, mode={output_mode}"
            )

            all_words = []
            for text in upstream[text_column].dropna():
                words = re.findall(r"\b[a-zA-Z]+\b", str(text).lower())
                words = [w for w in words if w not in stop and len(w) >= min_word_length]
                all_words.extend(words)

            counts = Counter(all_words)
            result = (
                pd.DataFrame(counts.items(), columns=["word", "frequency"])
                .sort_values("frequency", ascending=False)
                .reset_index(drop=True)
            )
            result = result[result["frequency"] >= min_frequency]

            if output_mode == "tfidf":
                try:
                    from sklearn.feature_extraction.text import TfidfVectorizer
                except ImportError:
                    raise ImportError(
                        "scikit-learn required for tfidf mode: pip install scikit-learn"
                    )
                texts = upstream[text_column].fillna("").astype(str).tolist()
                tfidf = TfidfVectorizer(stop_words=list(stop) if stop else None, min_df=1)
                tfidf_matrix = tfidf.fit_transform(texts)
                tfidf_scores = tfidf_matrix.mean(axis=0).A1
                tfidf_result = pd.DataFrame(
                    {"word": tfidf.get_feature_names_out(), "tfidf_score": tfidf_scores}
                ).sort_values("tfidf_score", ascending=False)
                result = result.merge(tfidf_result, on="word", how="left")
                context.log.info(f"Merged TF-IDF scores for {len(result)} words")

            elif output_mode == "top_n":
                result = result.head(top_n)
                context.log.info(f"Returning top {top_n} words")

            elif output_mode == "frequency_table":
                context.log.info(f"Returning frequency table with {len(result)} words")

            else:
                raise ValueError(
                    f"Unknown output_mode '{output_mode}'. Choose: frequency_table, top_n, tfidf"
                )

            context.add_output_metadata(
                {
                    "num_words": len(result),
                    "output_mode": output_mode,
                    "total_tokens": sum(counts.values()),
                    "vocabulary_size": len(counts),
                    "preview": MetadataValue.md(result.head(20).to_markdown()),
                }
            )
            return result

        return Definitions(assets=[_asset])
