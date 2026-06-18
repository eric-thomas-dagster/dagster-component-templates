"""Sentiment Analyzer Component.

Analyze sentiment using LLMs (GPT/Claude) or transformer models (DistilBERT, RoBERTa).
Supports batch processing, custom sentiment categories, aspect-based sentiment, and confidence scoring.
"""

import os
import json
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import ConfigDict, Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class SentimentAnalyzerComponent(Component, Model, Resolvable):
    """Component for analyzing sentiment in text.

    This component analyzes sentiment using either LLM-based (GPT/Claude) or
    transformer-based (DistilBERT, RoBERTa) methods. Returns sentiment labels
    and confidence scores.

    Features:
    - LLM-based or transformer-based sentiment analysis
    - Custom sentiment categories (positive/negative/neutral, or custom)
    - Aspect-based sentiment analysis
    - Confidence scores
    - Batch processing
    - Multi-language support (with appropriate models)
    - Emotion detection (joy, anger, sadness, etc.)

    Use Cases:
    - Customer feedback analysis
    - Social media monitoring
    - Product review analysis
    - Brand sentiment tracking
    - Support ticket prioritization
    - Content moderation

    Example:
        ```yaml
        type: dagster_component_templates.SentimentAnalyzerComponent
        attributes:
          asset_name: analyzed_reviews
          method: transformer
          model: distilbert-base-uncased-finetuned-sst-2-english
          input_column: review_text
          sentiment_categories: "positive,negative"
        ```
    """

    model_config = ConfigDict(populate_by_name=True)
    asset_name: str = Field(
        description="Name of the asset that will hold the sentiment analysis results"
    )

    method: str = Field(
        default="transformer",
        description="Analysis method: llm (GPT/Claude), transformer (local models)"
    )

    model_id: Optional[str] = Field(
        alias="model",
        default=None,
        description="Model name. LLM: gpt-4, claude-3-5-sonnet. Transformer: distilbert-base-uncased-finetuned-sst-2-english, cardiffnlp/twitter-roberta-base-sentiment"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for LLM method. Use ${OPENAI_API_KEY} or ${ANTHROPIC_API_KEY}"
    )

    provider: Optional[str] = Field(
        default=None,
        description="LLM provider: openai, anthropic (only for method=llm)"
    )

    input_column: Union[str, int] = Field(
        default="text",
        description="Column name containing text to analyze"
    )

    sentiment_label_column: Union[str, int] = Field(
        default="sentiment_label",
        description="Column name for sentiment label (e.g., positive, negative)"
    )

    sentiment_score_column: Union[str, int] = Field(
        default="sentiment_score",
        description="Column name for sentiment confidence score (0.0-1.0)"
    )

    sentiment_categories: str = Field(
        default="positive,negative,neutral",
        description="Comma-separated sentiment categories"
    )

    aspect_based: bool = Field(
        default=False,
        description="Enable aspect-based sentiment analysis (LLM only)"
    )

    aspects: Optional[str] = Field(
        default=None,
        description="Comma-separated aspects to analyze (e.g., 'quality,price,service')"
    )

    detect_emotions: bool = Field(
        default=False,
        description="Detect specific emotions (joy, anger, sadness, etc.) - LLM only"
    )

    emotion_categories: Optional[str] = Field(
        default="joy,anger,sadness,fear,surprise",
        description="Comma-separated emotion categories to detect"
    )

    return_reasoning: bool = Field(
        default=False,
        description="Return reasoning/explanation for sentiment (LLM only)"
    )

    batch_size: int = Field(
        default=32,
        description="Batch size for processing"
    )

    confidence_threshold: Optional[float] = Field(
        default=None,
        description="Minimum confidence threshold (0.0-1.0). Below this = 'uncertain'"
    )

    temperature: float = Field(
        default=0.0,
        description="Temperature for LLM (0.0 = deterministic)"
    )

    max_tokens: Optional[int] = Field(
        default=100,
        description="Max tokens for LLM response"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="sentiment_analysis",
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
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

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with text to analyze")

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        method = self.method
        model = self.model_id
        api_key = self.api_key
        provider = self.provider
        input_column = self.input_column
        sentiment_label_column = self.sentiment_label_column
        sentiment_score_column = self.sentiment_score_column
        sentiment_categories_str = self.sentiment_categories
        aspect_based = self.aspect_based
        aspects_str = self.aspects
        detect_emotions = self.detect_emotions
        emotion_categories_str = self.emotion_categories
        return_reasoning = self.return_reasoning
        batch_size = self.batch_size
        confidence_threshold = self.confidence_threshold
        temperature = self.temperature
        max_tokens = self.max_tokens
        description = self.description or f"Sentiment analysis using {method}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "sentiment_analyzer"  # component directory name
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


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def sentiment_analyzer_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that analyzes sentiment in text."""

            context.log.info(f"Starting sentiment analysis with method: {method}")

            input_df = upstream
            context.log.info(f"Received DataFrame: {len(input_df)} rows")

            # Validate input column
            if input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Parse categories
            sentiment_categories = [cat.strip() for cat in sentiment_categories_str.split(',')]
            context.log.info(f"Sentiment categories: {sentiment_categories}")

            aspects = None
            if aspect_based and aspects_str:
                aspects = [asp.strip() for asp in aspects_str.split(',')]
                context.log.info(f"Analyzing aspects: {aspects}")

            emotion_categories = None
            if detect_emotions and emotion_categories_str:
                emotion_categories = [emo.strip() for emo in emotion_categories_str.split(',')]

            # Extract texts
            texts = input_df[input_column].astype(str).tolist()
            context.log.info(f"Analyzing sentiment for {len(texts)} texts")

            # Results storage
            sentiment_labels = []
            sentiment_scores = []
            aspect_sentiments = [] if aspect_based else None
            emotions = [] if detect_emotions else None
            reasoning_list = [] if return_reasoning else None

            if method == "transformer":
                context.log.info(f"Using transformer model: {model or 'default'}")

                try:
                    from transformers import pipeline

                    # Load sentiment pipeline
                    if model:
                        sentiment_pipeline = pipeline("sentiment-analysis", model=model)
                    else:
                        # Default model
                        sentiment_pipeline = pipeline("sentiment-analysis")

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}")

                        results = sentiment_pipeline(batch)

                        for result in results:
                            label = result['label'].lower()
                            score = result['score']

                            # Map model labels to custom categories
                            if label in ['positive', 'pos', 'label_1']:
                                if 'positive' in sentiment_categories:
                                    sentiment_labels.append('positive')
                                else:
                                    sentiment_labels.append(sentiment_categories[0])
                            elif label in ['negative', 'neg', 'label_0']:
                                if 'negative' in sentiment_categories:
                                    sentiment_labels.append('negative')
                                else:
                                    sentiment_labels.append(sentiment_categories[-1])
                            else:
                                if 'neutral' in sentiment_categories:
                                    sentiment_labels.append('neutral')
                                else:
                                    sentiment_labels.append(sentiment_categories[len(sentiment_categories) // 2])

                            sentiment_scores.append(float(score))

                except ImportError:
                    raise ImportError("transformers not installed. Install with: pip install transformers torch")

            elif method == "llm":
                context.log.info(f"Using LLM: {provider}/{model}")

                # Validate LLM parameters
                if not provider or not model:
                    raise ValueError("provider and model required for method=llm")

                # Expand API key
                expanded_api_key = None
                if api_key:
                    expanded_api_key = os.path.expandvars(api_key)
                    if expanded_api_key == api_key and api_key.startswith('${'):
                        var_name = api_key.strip('${}')
                        raise ValueError(f"Environment variable not set: {var_name}")

                # Build prompt template
                prompt_template = f"""Analyze the sentiment of the following text.

Categories: {', '.join(sentiment_categories)}

Text: {{text}}

"""
                if aspect_based and aspects:
                    prompt_template += f"\nAnalyze sentiment for each of these aspects: {', '.join(aspects)}\n"

                if detect_emotions:
                    prompt_template += f"\nDetect these emotions if present: {', '.join(emotion_categories)}\n"

                prompt_template += """
Return your analysis as JSON:
{
  "sentiment": "category",
  "confidence": 0.0-1.0"""

                if aspect_based and aspects:
                    prompt_template += """,
  "aspects": {"aspect1": "sentiment", "aspect2": "sentiment"}"""

                if detect_emotions:
                    prompt_template += """,
  "emotions": ["emotion1", "emotion2"]"""

                if return_reasoning:
                    prompt_template += """,
  "reasoning": "brief explanation" """

                prompt_template += "\n}"

                # Initialize LLM client
                if provider == "openai":
                    try:
                        import openai
                        client = _make_openai_client(expanded_api_key)
                    except ImportError:
                        raise ImportError("openai not installed. Install with: pip install openai")

                elif provider == "anthropic":
                    try:
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)
                    except ImportError:
                        raise ImportError("anthropic not installed. Install with: pip install anthropic")
                else:
                    raise ValueError(f"Unsupported provider: {provider}")

                # Process each text
                for idx, text in enumerate(texts):
                    prompt = prompt_template.replace("{text}", text)

                    try:
                        # Call LLM
                        if provider == "openai":
                            response = client.chat.completions.create(
                                model=model,
                                messages=[
                                    {"role": "system", "content": "You are a sentiment analysis expert. Always return valid JSON."},
                                    {"role": "user", "content": prompt}
                                ],
                                temperature=temperature,
                                max_tokens=max_tokens,
                                response_format={"type": "json_object"}
                            )
                            result_text = response.choices[0].message.content

                        elif provider == "anthropic":
                            response = client.messages.create(
                                model=model,
                                max_tokens=max_tokens or 500,
                                temperature=temperature,
                                messages=[{"role": "user", "content": prompt}]
                            )
                            result_text = response.content[0].text

                        # Parse JSON result
                        result = json.loads(result_text)

                        sentiment_labels.append(result.get('sentiment', 'neutral'))
                        sentiment_scores.append(float(result.get('confidence', 1.0)))

                        if aspect_based and aspect_sentiments is not None:
                            aspect_sentiments.append(result.get('aspects', {}))

                        if detect_emotions and emotions is not None:
                            emotions.append(result.get('emotions', []))

                        if return_reasoning and reasoning_list is not None:
                            reasoning_list.append(result.get('reasoning', ''))

                    except Exception as e:
                        context.log.warning(f"Error analyzing text {idx}: {e}")
                        sentiment_labels.append('unknown')
                        sentiment_scores.append(0.0)
                        if aspect_based and aspect_sentiments is not None:
                            aspect_sentiments.append({})
                        if detect_emotions and emotions is not None:
                            emotions.append([])
                        if return_reasoning and reasoning_list is not None:
                            reasoning_list.append('')

                    if idx % 10 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{len(texts)}")

            else:
                raise ValueError(f"Unknown method: {method}")

            # Apply confidence threshold
            if confidence_threshold:
                for i in range(len(sentiment_labels)):
                    if sentiment_scores[i] < confidence_threshold:
                        sentiment_labels[i] = 'uncertain'

            # Create result DataFrame
            result_df = input_df.copy()
            result_df[sentiment_label_column] = sentiment_labels
            result_df[sentiment_score_column] = sentiment_scores

            if aspect_based and aspect_sentiments:
                result_df['aspect_sentiments'] = aspect_sentiments

            if detect_emotions and emotions:
                result_df['emotions'] = emotions

            if return_reasoning and reasoning_list:
                result_df['sentiment_reasoning'] = reasoning_list

            context.log.info(f"Sentiment analysis complete: {len(result_df)} texts analyzed")

            # Calculate statistics
            sentiment_counts = pd.Series(sentiment_labels).value_counts().to_dict()
            avg_confidence = sum(sentiment_scores) / len(sentiment_scores)

            # Metadata
            metadata = {
                "method": method,
                "model": model or "default",
                "num_analyzed": len(result_df),
                "sentiment_distribution": sentiment_counts,
                "average_confidence": float(avg_confidence),
                "categories": sentiment_categories,
            }

            if aspect_based:
                metadata["aspect_based"] = True
                metadata["aspects"] = aspects

            if detect_emotions:
                metadata["emotion_detection"] = True

            if include_preview and len(result_df) > 0:
                context.add_output_metadata({
                        **metadata,
                        "preview": MetadataValue.md(result_df.head(10).to_markdown())
                    })
                return result_df
            else:
                context.add_output_metadata(metadata)
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
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
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[sentiment_analyzer_asset])


        return Definitions(assets=[sentiment_analyzer_asset], asset_checks=list(_schema_checks))



def _make_openai_client(api_key):
    """Build an OpenAI or AzureOpenAI client based on env vars.

    Set OPENAI_AZURE_ENDPOINT to route through Azure OpenAI Service. Optional:
    OPENAI_AZURE_API_VERSION (default 2024-10-21). For Entra OAuth, set
    OPENAI_AZURE_USE_ENTRA=1 and the standard AZURE_TENANT_ID/CLIENT_ID/
    CLIENT_SECRET env vars (or rely on managed identity in Azure compute).
    """
    import openai as _openai
    azure_endpoint = os.environ.get("OPENAI_AZURE_ENDPOINT")
    if not azure_endpoint:
        return _openai.OpenAI(api_key=api_key)
    api_version = os.environ.get("OPENAI_AZURE_API_VERSION", "2024-10-21")
    if os.environ.get("OPENAI_AZURE_USE_ENTRA") == "1":
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(),
            "https://cognitiveservices.azure.com/.default",
        )
        return _openai.AzureOpenAI(
            azure_ad_token_provider=token_provider,
            azure_endpoint=azure_endpoint,
            api_version=api_version,
        )
    return _openai.AzureOpenAI(
        api_key=api_key,
        azure_endpoint=azure_endpoint,
        api_version=api_version,
    )
