"""Sentiment Analyzer Component.

Analyze sentiment using LLMs (GPT/Claude) or transformer models (DistilBERT, RoBERTa).
Supports batch processing, custom sentiment categories, aspect-based sentiment, and confidence scoring.
"""

import os
import json
from typing import Optional, List, Dict, Any
import pandas as pd

from dagster import (
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
from pydantic import Field


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

    asset_name: str = Field(
        description="Name of the asset that will hold the sentiment analysis results"
    )

    method: str = Field(
        default="transformer",
        description="Analysis method: llm (GPT/Claude), transformer (local models)"
    )

    model: Optional[str] = Field(
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

    input_column: str = Field(
        default="text",
        description="Column name containing text to analyze"
    )

    sentiment_label_column: str = Field(
        default="sentiment_label",
        description="Column name for sentiment label (e.g., positive, negative)"
    )

    sentiment_score_column: str = Field(
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        method = self.method
        model = self.model
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
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def sentiment_analyzer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that analyzes sentiment in text."""

            context.log.info(f"Starting sentiment analysis with method: {method}")

            # Get input DataFrame
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Sentiment Analyzer requires an upstream DataFrame")

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
                        client = openai.OpenAI(api_key=expanded_api_key)
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
                    prompt = prompt_template.format(text=text)

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

            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(result_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[sentiment_analyzer_asset])
