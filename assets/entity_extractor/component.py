"""Entity Extractor Component.

Extract named entities from text using NER (Named Entity Recognition) with
LLM-based (GPT/Claude), spaCy, or transformer-based methods.
"""

import os
import json
import time
from typing import Optional, List, Dict, Any, Union
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


class EntityExtractorComponent(Component, Model, Resolvable):
    """Component for extracting named entities from text.

    This component extracts entities like person names, organizations, locations,
    emails, phone numbers, products, order IDs, and custom entity types.
    It supports LLM-based, spaCy, and transformer-based methods.

    Features:
    - Multiple entity types (person, org, location, email, phone, product, etc.)
    - Multiple methods: LLM, spaCy, or transformer-based
    - Custom entity types and patterns
    - Entity linking to knowledge bases
    - Confidence scores per entity
    - Batch processing
    - Support for structured and unstructured text
    - JSON or flat output formats

    Use Cases:
    - Extract customer information from support tickets
    - Identify products/orders mentioned in feedback
    - PII detection and extraction
    - Knowledge base population
    - Automated form filling
    - Contact information extraction
    - Document indexing and search

    Example:
        ```yaml
        type: dagster_component_templates.EntityExtractorComponent
        attributes:
          asset_name: extracted_entities
          method: spacy
          spacy_model: en_core_web_lg
          input_column: ticket_text
          entity_types: "person,organization,product,email,phone,order_id"
          output_format: structured
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold extracted entities"
    )

    method: str = Field(
        default="spacy",
        description="Extraction method: llm (GPT/Claude), spacy, or transformer"
    )

    llm_provider: Optional[str] = Field(
        default=None,
        description="LLM provider: openai or anthropic (for method=llm)"
    )

    llm_model: Optional[str] = Field(
        default=None,
        description="LLM model name (for method=llm)"
    )

    spacy_model: Optional[str] = Field(
        default=None,
        description="spaCy model: en_core_web_sm, en_core_web_md, en_core_web_lg (for method=spacy)"
    )

    transformer_model: Optional[str] = Field(
        default=None,
        description="Transformer NER model (for method=transformer)"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for LLM method. Use ${OPENAI_API_KEY} or ${ANTHROPIC_API_KEY}"
    )

    input_column: str = Field(
        default="text",
        description="Column name containing text to extract entities from"
    )

    entity_types: str = Field(
        default="person,organization,location,email,phone,product,order_id,date,money",
        description="Comma-separated list of entity types to extract"
    )

    custom_entities: Optional[str] = Field(
        default=None,
        description="JSON string of custom entity patterns: {\"type\": [\"pattern1\", \"pattern2\"]}"
    )

    output_format: str = Field(
        default="structured",
        description="Output format: structured (dict per row) or flat (one column per entity type)"
    )

    link_entities: bool = Field(
        default=False,
        description="Enable entity linking/resolution (LLM only)"
    )

    include_confidence: bool = Field(
        default=True,
        description="Include confidence scores for entities"
    )

    include_spans: bool = Field(
        default=False,
        description="Include character span positions (start, end)"
    )

    deduplicate: bool = Field(
        default=True,
        description="Remove duplicate entities within the same text"
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
        default=500,
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
        description="Track token usage and costs (LLM only)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="entity_extraction",
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
        spacy_model = self.spacy_model
        transformer_model = self.transformer_model
        api_key = self.api_key
        input_column = self.input_column
        entity_types_str = self.entity_types
        custom_entities_str = self.custom_entities
        output_format = self.output_format
        link_entities = self.link_entities
        include_confidence = self.include_confidence
        include_spans = self.include_spans
        deduplicate = self.deduplicate
        batch_size = self.batch_size
        temperature = self.temperature
        max_tokens = self.max_tokens
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        track_costs = self.track_costs
        description = self.description or f"Entity extraction using {method}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cost per 1M tokens
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
        def entity_extractor_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that extracts named entities from text."""

            context.log.info(f"Starting entity extraction with method: {method}")

            # Get input DataFrame
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Entity Extractor requires an upstream DataFrame")

            # Validate input column
            if input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Parse configuration
            entity_types = [etype.strip() for etype in entity_types_str.split(',')]
            context.log.info(f"Extracting entity types: {entity_types}")

            custom_entities = {}
            if custom_entities_str:
                try:
                    custom_entities = json.loads(custom_entities_str)
                    context.log.info(f"Using custom entity patterns: {list(custom_entities.keys())}")
                except json.JSONDecodeError as e:
                    context.log.warning(f"Failed to parse custom_entities: {e}")

            # Extract texts
            texts = input_df[input_column].astype(str).tolist()
            context.log.info(f"Extracting entities from {len(texts)} texts")

            # Results storage
            all_entities = []
            total_input_tokens = 0
            total_output_tokens = 0

            if method == "llm":
                context.log.info(f"Using LLM: {llm_provider}/{llm_model}")

                if not llm_provider or not llm_model:
                    raise ValueError("llm_provider and llm_model required for method=llm")

                # Expand API key
                expanded_api_key = None
                if api_key:
                    expanded_api_key = os.path.expandvars(api_key)
                    if expanded_api_key == api_key and api_key.startswith('${'):
                        var_name = api_key.strip('${}')
                        raise ValueError(f"Environment variable not set: {var_name}")

                # Build prompt template
                prompt_template = f"""Extract named entities from the following text.

Text: {{text}}

Entity types to extract: {', '.join(entity_types)}

Return entities as JSON array:
[
  {{"type": "entity_type", "value": "extracted_text", "confidence": 0.0-1.0"""

                if include_spans:
                    prompt_template += """, "start": 0, "end": 10"""

                if link_entities:
                    prompt_template += """, "linked_id": "optional_knowledge_base_id" """

                prompt_template += """}},
  ...
]

Return empty array [] if no entities found."""

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
                    entities = []

                    while attempt < max_retries and not success:
                        try:
                            # Call LLM
                            if llm_provider == "openai":
                                response = client.chat.completions.create(
                                    model=llm_model,
                                    messages=[
                                        {"role": "system", "content": "You are a named entity recognition expert. Always return valid JSON array."},
                                        {"role": "user", "content": prompt}
                                    ],
                                    temperature=temperature,
                                    max_tokens=max_tokens,
                                    response_format={"type": "json_object"} if "gpt" in llm_model else None
                                )
                                result_text = response.choices[0].message.content
                                total_input_tokens += response.usage.prompt_tokens
                                total_output_tokens += response.usage.completion_tokens

                            elif llm_provider == "anthropic":
                                response = client.messages.create(
                                    model=llm_model,
                                    max_tokens=max_tokens or 1000,
                                    temperature=temperature,
                                    messages=[{"role": "user", "content": prompt}]
                                )
                                result_text = response.content[0].text
                                total_input_tokens += response.usage.input_tokens
                                total_output_tokens += response.usage.output_tokens

                            # Parse JSON result
                            # Handle both direct array and wrapped object
                            try:
                                result = json.loads(result_text)
                                if isinstance(result, dict):
                                    entities = result.get('entities', [])
                                else:
                                    entities = result
                            except json.JSONDecodeError:
                                # Try to extract JSON array from text
                                import re
                                match = re.search(r'\[.*\]', result_text, re.DOTALL)
                                if match:
                                    entities = json.loads(match.group(0))
                                else:
                                    entities = []

                            success = True

                        except Exception as e:
                            attempt += 1
                            if attempt < max_retries:
                                wait_time = (2 ** attempt) * rate_limit_delay
                                context.log.warning(f"Error extracting entities from text {idx}: {e}. Retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                context.log.error(f"Failed to extract entities from text {idx} after {max_retries} attempts: {e}")
                                entities = []

                    all_entities.append(entities)

                    if idx % 10 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{len(texts)}")

                    if rate_limit_delay > 0 and success:
                        time.sleep(rate_limit_delay)

            elif method == "spacy":
                context.log.info(f"Using spaCy model: {spacy_model or 'en_core_web_sm'}")

                try:
                    import spacy
                    import re

                    # Load spaCy model
                    model_name = spacy_model or "en_core_web_sm"
                    try:
                        nlp = spacy.load(model_name)
                    except OSError:
                        context.log.info(f"Downloading spaCy model: {model_name}")
                        import subprocess
                        subprocess.run(["python", "-m", "spacy", "download", model_name], check=True)
                        nlp = spacy.load(model_name)

                    # Entity type mapping
                    spacy_entity_map = {
                        "person": ["PERSON"],
                        "organization": ["ORG"],
                        "location": ["GPE", "LOC"],
                        "date": ["DATE"],
                        "money": ["MONEY"],
                        "product": ["PRODUCT"],
                    }

                    # Process texts
                    for text in texts:
                        doc = nlp(text)
                        entities = []

                        # Extract standard entities
                        for ent in doc.ents:
                            for entity_type in entity_types:
                                if entity_type in spacy_entity_map:
                                    if ent.label_ in spacy_entity_map[entity_type]:
                                        entity_obj = {
                                            "type": entity_type,
                                            "value": ent.text
                                        }
                                        if include_confidence:
                                            # spaCy doesn't provide confidence, use 1.0
                                            entity_obj["confidence"] = 1.0
                                        if include_spans:
                                            entity_obj["start"] = ent.start_char
                                            entity_obj["end"] = ent.end_char
                                        entities.append(entity_obj)

                        # Extract emails using regex
                        if "email" in entity_types:
                            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                            for match in re.finditer(email_pattern, text):
                                entity_obj = {
                                    "type": "email",
                                    "value": match.group(0)
                                }
                                if include_confidence:
                                    entity_obj["confidence"] = 1.0
                                if include_spans:
                                    entity_obj["start"] = match.start()
                                    entity_obj["end"] = match.end()
                                entities.append(entity_obj)

                        # Extract phone numbers using regex
                        if "phone" in entity_types:
                            phone_pattern = r'\b(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b'
                            for match in re.finditer(phone_pattern, text):
                                entity_obj = {
                                    "type": "phone",
                                    "value": match.group(0)
                                }
                                if include_confidence:
                                    entity_obj["confidence"] = 1.0
                                if include_spans:
                                    entity_obj["start"] = match.start()
                                    entity_obj["end"] = match.end()
                                entities.append(entity_obj)

                        # Extract order IDs using pattern
                        if "order_id" in entity_types:
                            order_pattern = r'\b(?:order[#:\s]*|#)([A-Z0-9]{6,})\b'
                            for match in re.finditer(order_pattern, text, re.IGNORECASE):
                                entity_obj = {
                                    "type": "order_id",
                                    "value": match.group(1)
                                }
                                if include_confidence:
                                    entity_obj["confidence"] = 0.9
                                if include_spans:
                                    entity_obj["start"] = match.start()
                                    entity_obj["end"] = match.end()
                                entities.append(entity_obj)

                        # Extract custom entities
                        for custom_type, patterns in custom_entities.items():
                            for pattern in patterns:
                                for match in re.finditer(pattern, text, re.IGNORECASE):
                                    entity_obj = {
                                        "type": custom_type,
                                        "value": match.group(0)
                                    }
                                    if include_confidence:
                                        entity_obj["confidence"] = 0.8
                                    if include_spans:
                                        entity_obj["start"] = match.start()
                                        entity_obj["end"] = match.end()
                                    entities.append(entity_obj)

                        all_entities.append(entities)

                except ImportError:
                    raise ImportError("spacy not installed. Install with: pip install spacy")

            elif method == "transformer":
                context.log.info(f"Using transformer model: {transformer_model or 'dslim/bert-base-NER'}")

                try:
                    from transformers import pipeline

                    # Load NER pipeline
                    model_name = transformer_model or "dslim/bert-base-NER"
                    ner_pipeline = pipeline("ner", model=model_name, aggregation_strategy="simple")

                    # Entity type mapping
                    transformer_entity_map = {
                        "person": ["PER", "PERSON"],
                        "organization": ["ORG"],
                        "location": ["LOC", "GPE"],
                        "miscellaneous": ["MISC"],
                    }

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}")

                        batch_results = ner_pipeline(batch)

                        for result in batch_results:
                            entities = []
                            for entity in result:
                                # Map entity type
                                entity_type = "miscellaneous"
                                for etype, labels in transformer_entity_map.items():
                                    if etype in entity_types and entity['entity_group'] in labels:
                                        entity_type = etype
                                        break

                                if entity_type in entity_types or "miscellaneous" in entity_types:
                                    entity_obj = {
                                        "type": entity_type,
                                        "value": entity['word']
                                    }
                                    if include_confidence:
                                        entity_obj["confidence"] = float(entity['score'])
                                    if include_spans:
                                        entity_obj["start"] = entity['start']
                                        entity_obj["end"] = entity['end']
                                    entities.append(entity_obj)

                            all_entities.append(entities)

                except ImportError:
                    raise ImportError("transformers not installed. Install with: pip install transformers torch")

            else:
                raise ValueError(f"Unknown method: {method}")

            # Deduplicate entities if requested
            if deduplicate:
                for i in range(len(all_entities)):
                    seen = set()
                    unique_entities = []
                    for entity in all_entities[i]:
                        key = (entity['type'], entity['value'].lower())
                        if key not in seen:
                            seen.add(key)
                            unique_entities.append(entity)
                    all_entities[i] = unique_entities

            # Create result DataFrame
            result_df = input_df.copy()

            if output_format == "structured":
                # Add entities as JSON column
                result_df['entities'] = [json.dumps(entities) if entities else "[]" for entities in all_entities]

                # Also add entity count
                result_df['entity_count'] = [len(entities) for entities in all_entities]

                # Add count by type
                for entity_type in entity_types:
                    result_df[f'{entity_type}_count'] = [
                        len([e for e in entities if e['type'] == entity_type])
                        for entities in all_entities
                    ]

            else:  # flat format
                # Create separate columns for each entity type
                for entity_type in entity_types:
                    values = []
                    for entities in all_entities:
                        type_entities = [e['value'] for e in entities if e['type'] == entity_type]
                        values.append(','.join(type_entities) if type_entities else '')
                    result_df[f'{entity_type}_entities'] = values

            context.log.info(f"Entity extraction complete: {len(result_df)} texts processed")

            # Calculate statistics
            total_entities = sum(len(entities) for entities in all_entities)
            entity_type_counts = {}
            for entities in all_entities:
                for entity in entities:
                    entity_type = entity['type']
                    entity_type_counts[entity_type] = entity_type_counts.get(entity_type, 0) + 1

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
                "model": llm_model or spacy_model or transformer_model or "default",
                "num_texts_processed": len(result_df),
                "total_entities_extracted": total_entities,
                "avg_entities_per_text": float(total_entities / len(result_df)) if len(result_df) > 0 else 0.0,
                "entity_type_counts": entity_type_counts,
                "output_format": output_format,
            }

            if method == "llm" and track_costs:
                metadata["total_input_tokens"] = total_input_tokens
                metadata["total_output_tokens"] = total_output_tokens
                metadata["estimated_cost_usd"] = f"${total_cost:.4f}"

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

        return Definitions(assets=[entity_extractor_asset])
