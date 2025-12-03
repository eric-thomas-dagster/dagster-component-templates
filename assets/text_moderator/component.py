"""Text Moderator Component.

Detect toxic content, hate speech, PII, profanity, and inappropriate content using
OpenAI Moderation API, Perspective API, transformer models, or LLM-based methods.
"""

import os
import json
import time
import re
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


class TextModeratorComponent(Component, Model, Resolvable):
    """Component for content moderation and safety detection.

    This component detects toxic content, hate speech, PII, profanity, and other
    inappropriate content using multiple providers: OpenAI Moderation API,
    Perspective API, transformer models, or LLM-based analysis.

    Features:
    - Toxicity detection (toxic, severe_toxic, obscene, threat, insult, identity_hate)
    - Hate speech detection
    - PII detection (email, phone, SSN, credit card, address)
    - Profanity filtering
    - Multiple providers: OpenAI, Perspective API, transformers, LLM
    - Confidence scores for each category
    - Redaction mode (mask detected content)
    - Custom moderation rules
    - Multi-language support

    Use Cases:
    - User-generated content moderation
    - Comment section filtering
    - Review platform safety
    - Chat moderation
    - Forum post screening
    - Social media content filtering
    - Compliance (GDPR, COPPA, content policies)

    Example:
        ```yaml
        type: dagster_component_templates.TextModeratorComponent
        attributes:
          asset_name: moderated_content
          method: openai_moderation
          input_column: user_comment
          categories: "toxicity,hate_speech,pii,profanity,sexual,violence"
          threshold: 0.7
          redact_pii: true
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold moderated content"
    )

    method: str = Field(
        default="openai_moderation",
        description="Moderation method: openai_moderation, perspective_api, transformer, or llm"
    )

    llm_provider: Optional[str] = Field(
        default=None,
        description="LLM provider: openai or anthropic (for method=llm)"
    )

    llm_model: Optional[str] = Field(
        default=None,
        description="LLM model name (for method=llm)"
    )

    transformer_model: Optional[str] = Field(
        default=None,
        description="Transformer model: unitary/toxic-bert, s-nlp/roberta-toxicity-classifier (for method=transformer)"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for OpenAI, Anthropic, or Perspective API"
    )

    input_column: str = Field(
        default="text",
        description="Column name containing text to moderate"
    )

    categories: str = Field(
        default="toxicity,hate_speech,pii,profanity,sexual,violence",
        description="Comma-separated moderation categories to check"
    )

    threshold: float = Field(
        default=0.7,
        description="Confidence threshold for flagging content (0.0-1.0)"
    )

    redact_pii: bool = Field(
        default=False,
        description="Redact/mask detected PII in output"
    )

    include_scores: bool = Field(
        default=True,
        description="Include confidence scores for each category"
    )

    flag_column: str = Field(
        default="flagged",
        description="Column name for overall flag (true if any category exceeds threshold)"
    )

    batch_size: int = Field(
        default=32,
        description="Batch size for processing"
    )

    temperature: float = Field(
        default=0.0,
        description="Temperature for LLM (0.0 = deterministic)"
    )

    max_tokens: Optional[int] = Field(
        default=300,
        description="Max tokens for LLM response"
    )

    rate_limit_delay: float = Field(
        default=0.1,
        description="Delay in seconds between API calls"
    )

    max_retries: int = Field(
        default=3,
        description="Maximum retries for failed API calls"
    )

    track_costs: bool = Field(
        default=True,
        description="Track API costs (for paid APIs)"
    )

    custom_profanity_list: Optional[str] = Field(
        default=None,
        description="Comma-separated list of custom profanity words to detect"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="content_moderation",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        method = self.method
        llm_provider = self.llm_provider
        llm_model = self.llm_model
        transformer_model = self.transformer_model
        api_key = self.api_key
        input_column = self.input_column
        categories_str = self.categories
        threshold = self.threshold
        redact_pii = self.redact_pii
        include_scores = self.include_scores
        flag_column = self.flag_column
        batch_size = self.batch_size
        temperature = self.temperature
        max_tokens = self.max_tokens
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        track_costs = self.track_costs
        custom_profanity_str = self.custom_profanity_list
        description = self.description or f"Content moderation using {method}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cost per 1M tokens (for LLM method)
        COST_PER_1M_INPUT = {
            "gpt-4": 30.0, "gpt-4-turbo": 10.0, "gpt-4o": 5.0, "gpt-4o-mini": 0.15, "gpt-3.5-turbo": 0.5,
            "claude-3-opus": 15.0, "claude-3-5-sonnet": 3.0, "claude-3-sonnet": 3.0, "claude-3-haiku": 0.25,
        }
        COST_PER_1M_OUTPUT = {
            "gpt-4": 60.0, "gpt-4-turbo": 30.0, "gpt-4o": 15.0, "gpt-4o-mini": 0.6, "gpt-3.5-turbo": 1.5,
            "claude-3-opus": 75.0, "claude-3-5-sonnet": 15.0, "claude-3-sonnet": 15.0, "claude-3-haiku": 1.25,
        }

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def text_moderator_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that moderates text content for safety."""

            context.log.info(f"Starting content moderation with method: {method}")

            # Get input DataFrame
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Text Moderator requires an upstream DataFrame")

            # Validate input column
            if input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Parse configuration
            categories = [cat.strip() for cat in categories_str.split(',')]
            context.log.info(f"Moderation categories: {categories}")

            custom_profanity = []
            if custom_profanity_str:
                custom_profanity = [word.strip().lower() for word in custom_profanity_str.split(',')]

            # Extract texts
            texts = input_df[input_column].astype(str).tolist()
            context.log.info(f"Moderating {len(texts)} texts")

            # Results storage
            all_flags = []
            all_scores = {cat: [] for cat in categories}
            redacted_texts = [] if redact_pii else None
            total_input_tokens = 0
            total_output_tokens = 0
            api_calls = 0

            # PII patterns for detection
            pii_patterns = {
                'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                'phone': r'\b(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b',
                'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
                'credit_card': r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b',
                'ip_address': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
            }

            if method == "openai_moderation":
                context.log.info("Using OpenAI Moderation API")

                # Expand API key
                expanded_api_key = None
                if api_key:
                    expanded_api_key = os.path.expandvars(api_key)
                    if expanded_api_key == api_key and api_key.startswith('${'):
                        var_name = api_key.strip('${}')
                        raise ValueError(f"Environment variable not set: {var_name}")

                try:
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)

                    # Process each text
                    for idx, text in enumerate(texts):
                        try:
                            response = client.moderations.create(input=text)
                            result = response.results[0]
                            api_calls += 1

                            # Extract scores
                            scores = {
                                'toxicity': max(result.category_scores.hate, result.category_scores.harassment),
                                'hate_speech': result.category_scores.hate,
                                'sexual': result.category_scores.sexual,
                                'violence': result.category_scores.violence,
                                'harassment': result.category_scores.harassment,
                                'self_harm': result.category_scores.self_harm,
                            }

                            # Check PII if requested
                            if 'pii' in categories:
                                pii_found = False
                                for pattern in pii_patterns.values():
                                    if re.search(pattern, text):
                                        pii_found = True
                                        break
                                scores['pii'] = 1.0 if pii_found else 0.0

                            # Check profanity if requested
                            if 'profanity' in categories:
                                profanity_score = 0.0
                                text_lower = text.lower()
                                if custom_profanity:
                                    for word in custom_profanity:
                                        if word in text_lower:
                                            profanity_score = 1.0
                                            break
                                scores['profanity'] = profanity_score

                            # Store scores for requested categories
                            for cat in categories:
                                all_scores[cat].append(scores.get(cat, 0.0))

                            # Overall flag
                            flagged = any(scores.get(cat, 0.0) >= threshold for cat in categories)
                            all_flags.append(flagged)

                            # Redact PII if requested
                            if redact_pii:
                                redacted_text = text
                                for pattern in pii_patterns.values():
                                    redacted_text = re.sub(pattern, '[REDACTED]', redacted_text)
                                redacted_texts.append(redacted_text)

                        except Exception as e:
                            context.log.warning(f"Error moderating text {idx}: {e}")
                            for cat in categories:
                                all_scores[cat].append(0.0)
                            all_flags.append(False)
                            if redact_pii:
                                redacted_texts.append(text)

                        if idx % 10 == 0 and idx > 0:
                            context.log.info(f"Processed {idx}/{len(texts)}")

                        if rate_limit_delay > 0:
                            time.sleep(rate_limit_delay)

                except ImportError:
                    raise ImportError("openai not installed. Install with: pip install openai")

            elif method == "perspective_api":
                context.log.info("Using Perspective API")

                # Expand API key
                expanded_api_key = None
                if api_key:
                    expanded_api_key = os.path.expandvars(api_key)
                else:
                    raise ValueError("API key required for Perspective API")

                try:
                    import requests

                    # Process each text
                    for idx, text in enumerate(texts):
                        try:
                            url = f"https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze?key={expanded_api_key}"

                            data = {
                                'comment': {'text': text},
                                'requestedAttributes': {
                                    'TOXICITY': {},
                                    'SEVERE_TOXICITY': {},
                                    'IDENTITY_ATTACK': {},
                                    'INSULT': {},
                                    'PROFANITY': {},
                                    'THREAT': {},
                                }
                            }

                            response = requests.post(url, json=data)
                            response.raise_for_status()
                            result = response.json()
                            api_calls += 1

                            # Extract scores
                            scores = {
                                'toxicity': result['attributeScores']['TOXICITY']['summaryScore']['value'],
                                'severe_toxicity': result['attributeScores']['SEVERE_TOXICITY']['summaryScore']['value'],
                                'hate_speech': result['attributeScores']['IDENTITY_ATTACK']['summaryScore']['value'],
                                'insult': result['attributeScores']['INSULT']['summaryScore']['value'],
                                'profanity': result['attributeScores']['PROFANITY']['summaryScore']['value'],
                                'threat': result['attributeScores']['THREAT']['summaryScore']['value'],
                            }

                            # Check PII
                            if 'pii' in categories:
                                pii_found = False
                                for pattern in pii_patterns.values():
                                    if re.search(pattern, text):
                                        pii_found = True
                                        break
                                scores['pii'] = 1.0 if pii_found else 0.0

                            # Store scores
                            for cat in categories:
                                all_scores[cat].append(scores.get(cat, 0.0))

                            # Overall flag
                            flagged = any(scores.get(cat, 0.0) >= threshold for cat in categories)
                            all_flags.append(flagged)

                            # Redact PII
                            if redact_pii:
                                redacted_text = text
                                for pattern in pii_patterns.values():
                                    redacted_text = re.sub(pattern, '[REDACTED]', redacted_text)
                                redacted_texts.append(redacted_text)

                        except Exception as e:
                            context.log.warning(f"Error moderating text {idx}: {e}")
                            for cat in categories:
                                all_scores[cat].append(0.0)
                            all_flags.append(False)
                            if redact_pii:
                                redacted_texts.append(text)

                        if idx % 10 == 0 and idx > 0:
                            context.log.info(f"Processed {idx}/{len(texts)}")

                        if rate_limit_delay > 0:
                            time.sleep(rate_limit_delay)

                except ImportError:
                    raise ImportError("requests not installed. Install with: pip install requests")

            elif method == "transformer":
                context.log.info(f"Using transformer model: {transformer_model or 'unitary/toxic-bert'}")

                try:
                    from transformers import pipeline

                    # Load toxicity classifier
                    model_name = transformer_model or "unitary/toxic-bert"
                    classifier = pipeline("text-classification", model=model_name)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}")

                        results = classifier(batch)

                        for text, result in zip(batch, results):
                            # Map result to categories
                            toxicity_score = result['score'] if result['label'].lower() == 'toxic' else 1.0 - result['score']

                            scores = {
                                'toxicity': toxicity_score,
                                'hate_speech': toxicity_score,  # Approximate
                            }

                            # Check PII
                            if 'pii' in categories:
                                pii_found = False
                                for pattern in pii_patterns.values():
                                    if re.search(pattern, text):
                                        pii_found = True
                                        break
                                scores['pii'] = 1.0 if pii_found else 0.0

                            # Check profanity
                            if 'profanity' in categories:
                                profanity_score = 0.0
                                text_lower = text.lower()
                                if custom_profanity:
                                    for word in custom_profanity:
                                        if word in text_lower:
                                            profanity_score = 1.0
                                            break
                                scores['profanity'] = profanity_score

                            # Store scores
                            for cat in categories:
                                all_scores[cat].append(scores.get(cat, 0.0))

                            # Overall flag
                            flagged = any(scores.get(cat, 0.0) >= threshold for cat in categories)
                            all_flags.append(flagged)

                            # Redact PII
                            if redact_pii:
                                redacted_text = text
                                for pattern in pii_patterns.values():
                                    redacted_text = re.sub(pattern, '[REDACTED]', redacted_text)
                                redacted_texts.append(redacted_text)

                except ImportError:
                    raise ImportError("transformers not installed. Install with: pip install transformers torch")

            elif method == "llm":
                context.log.info(f"Using LLM: {llm_provider}/{llm_model}")

                if not llm_provider or not llm_model:
                    raise ValueError("llm_provider and llm_model required for method=llm")

                # Expand API key
                expanded_api_key = None
                if api_key:
                    expanded_api_key = os.path.expandvars(api_key)

                # Build prompt template
                prompt_template = f"""Analyze the following text for content safety issues.

Text: {{text}}

Check for these categories: {', '.join(categories)}

Return your analysis as JSON:
{{
  "scores": {{
"""
                for i, cat in enumerate(categories):
                    prompt_template += f'    "{cat}": 0.0-1.0'
                    if i < len(categories) - 1:
                        prompt_template += ','
                    prompt_template += '\n'

                prompt_template += """  },
  "flagged": true/false,
  "explanation": "brief explanation"
}

Score 0.0 = safe, 1.0 = definitely violates policy."""

                # Initialize LLM client
                if llm_provider == "openai":
                    try:
                        import openai
                        client = openai.OpenAI(api_key=expanded_api_key)
                    except ImportError:
                        raise ImportError("openai not installed. Install with: pip install openai")

                elif llm_provider == "anthropic":
                    try:
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)
                    except ImportError:
                        raise ImportError("anthropic not installed. Install with: pip install anthropic")
                else:
                    raise ValueError(f"Unsupported LLM provider: {llm_provider}")

                # Process each text
                for idx, text in enumerate(texts):
                    prompt = prompt_template.format(text=text)

                    attempt = 0
                    success = False

                    while attempt < max_retries and not success:
                        try:
                            # Call LLM
                            if llm_provider == "openai":
                                response = client.chat.completions.create(
                                    model=llm_model,
                                    messages=[
                                        {"role": "system", "content": "You are a content moderation expert. Always return valid JSON."},
                                        {"role": "user", "content": prompt}
                                    ],
                                    temperature=temperature,
                                    max_tokens=max_tokens,
                                    response_format={"type": "json_object"}
                                )
                                result_text = response.choices[0].message.content
                                total_input_tokens += response.usage.prompt_tokens
                                total_output_tokens += response.usage.completion_tokens

                            elif llm_provider == "anthropic":
                                response = client.messages.create(
                                    model=llm_model,
                                    max_tokens=max_tokens or 500,
                                    temperature=temperature,
                                    messages=[{"role": "user", "content": prompt}]
                                )
                                result_text = response.content[0].text
                                total_input_tokens += response.usage.input_tokens
                                total_output_tokens += response.usage.output_tokens

                            api_calls += 1

                            # Parse JSON result
                            result = json.loads(result_text)
                            scores = result.get('scores', {})

                            # Store scores
                            for cat in categories:
                                all_scores[cat].append(float(scores.get(cat, 0.0)))

                            # Overall flag
                            all_flags.append(result.get('flagged', False))

                            # Redact PII
                            if redact_pii:
                                redacted_text = text
                                for pattern in pii_patterns.values():
                                    redacted_text = re.sub(pattern, '[REDACTED]', redacted_text)
                                redacted_texts.append(redacted_text)

                            success = True

                        except Exception as e:
                            attempt += 1
                            if attempt < max_retries:
                                wait_time = (2 ** attempt) * rate_limit_delay
                                context.log.warning(f"Error moderating text {idx}: {e}. Retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                context.log.error(f"Failed to moderate text {idx} after {max_retries} attempts: {e}")
                                for cat in categories:
                                    all_scores[cat].append(0.0)
                                all_flags.append(False)
                                if redact_pii:
                                    redacted_texts.append(text)

                    if idx % 10 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{len(texts)}")

                    if rate_limit_delay > 0 and success:
                        time.sleep(rate_limit_delay)

            else:
                raise ValueError(f"Unknown method: {method}")

            # Create result DataFrame
            result_df = input_df.copy()
            result_df[flag_column] = all_flags

            if include_scores:
                for cat in categories:
                    result_df[f'{cat}_score'] = all_scores[cat]

            if redact_pii and redacted_texts:
                result_df[f'{input_column}_redacted'] = redacted_texts

            context.log.info(f"Moderation complete: {len(result_df)} texts processed")

            # Calculate statistics
            flagged_count = sum(all_flags)
            flagged_rate = (flagged_count / len(all_flags) * 100) if all_flags else 0

            category_stats = {}
            for cat in categories:
                if all_scores[cat]:
                    avg_score = sum(all_scores[cat]) / len(all_scores[cat])
                    flagged_cat = sum(1 for score in all_scores[cat] if score >= threshold)
                    category_stats[cat] = {
                        'avg_score': float(avg_score),
                        'flagged_count': flagged_cat
                    }

            # Calculate costs
            cost_input = 0.0
            cost_output = 0.0
            if track_costs and method == "llm" and total_input_tokens > 0:
                for key in COST_PER_1M_INPUT.keys():
                    if key in llm_model:
                        cost_input = (total_input_tokens / 1_000_000) * COST_PER_1M_INPUT[key]
                        cost_output = (total_output_tokens / 1_000_000) * COST_PER_1M_OUTPUT[key]
                        break

            total_cost = cost_input + cost_output

            # Metadata
            metadata = {
                "method": method,
                "model": llm_model or transformer_model or method,
                "num_texts_moderated": len(result_df),
                "flagged_count": flagged_count,
                "flagged_rate": f"{flagged_rate:.1f}%",
                "threshold": threshold,
                "categories": categories,
                "category_statistics": category_stats,
            }

            if method in ["llm", "openai_moderation", "perspective_api"]:
                metadata["api_calls"] = api_calls

            if method == "llm" and track_costs and total_cost > 0:
                metadata["total_input_tokens"] = total_input_tokens
                metadata["total_output_tokens"] = total_output_tokens
                metadata["estimated_cost_usd"] = f"${total_cost:.4f}"

            if redact_pii:
                metadata["pii_redaction_enabled"] = True

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

        return Definitions(assets=[text_moderator_asset])
