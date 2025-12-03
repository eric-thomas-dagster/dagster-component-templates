# Entity Extractor Component

Extract named entities from text using NER (Named Entity Recognition) with LLM-based (GPT/Claude), spaCy, or transformer-based methods.

## Overview

The Entity Extractor Component identifies and extracts entities like person names, organizations, locations, emails, phone numbers, products, order IDs, and custom entity types from text. It supports three extraction methods optimized for different use cases.

## Features

- **Multiple Entity Types**: person, organization, location, email, phone, product, order_id, date, money, and custom types
- **Three Methods**: LLM (most flexible), spaCy (fastest), transformer (balanced)
- **Custom Entity Patterns**: Define regex patterns for domain-specific entities
- **Entity Linking**: Resolve entities to knowledge bases (LLM only)
- **Confidence Scores**: Get confidence per entity
- **Batch Processing**: Efficient handling of large datasets
- **Flexible Output**: Structured (JSON) or flat (columns) format
- **Span Tracking**: Optional character position tracking
- **Deduplication**: Remove duplicate entities automatically

## Use Cases

1. **Support Ticket Processing**: Extract customer names, order IDs, products mentioned
2. **PII Detection**: Identify emails, phone numbers, addresses for compliance
3. **Document Indexing**: Extract entities for search and categorization
4. **Form Auto-filling**: Extract structured data from unstructured text
5. **Knowledge Base Population**: Build entity databases from documents
6. **Contact Extraction**: Extract contact information from emails/messages
7. **Order Processing**: Identify order IDs, products, and amounts

## Prerequisites

### spaCy Method (Recommended)
- **Python Packages**: `spacy>=3.5.0`, `pandas>=1.5.0`
- **No API Key Required**
- **Cost**: $0 (runs locally)
- **Speed**: Fastest (5-20ms per text)

### Transformer Method
- **Python Packages**: `transformers>=4.30.0`, `torch>=2.0.0`, `pandas>=1.5.0`
- **No API Key Required**
- **Cost**: $0 (runs locally)
- **Speed**: Medium (50-200ms per text)

### LLM Method
- **Python Packages**: `openai>=1.0.0` or `anthropic>=0.18.0`, `pandas>=1.5.0`
- **API Key**: OpenAI or Anthropic API key
- **Cost**: $0.15-$5 per 1M input tokens
- **Speed**: Slower (200-500ms per text)

## Configuration

### spaCy Method (Recommended for Production)

Fast, accurate, and free:

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: extracted_entities

  method: spacy
  spacy_model: en_core_web_lg  # Best accuracy

  input_column: ticket_text

  entity_types: "person,organization,location,email,phone,product,order_id,date,money"

  output_format: structured
  include_confidence: true
  deduplicate: true
  batch_size: 32
```

### LLM Method (Most Flexible)

For custom entity types or complex extraction:

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: llm_entities

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: text

  entity_types: "person,organization,product,issue_type,feature_request,bug_id"

  output_format: structured
  link_entities: true  # Enable entity linking
  include_confidence: true
  include_spans: true

  temperature: 0.0
  max_tokens: 500
```

### Transformer Method

Balanced approach:

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: transformer_entities

  method: transformer
  transformer_model: dslim/bert-base-NER

  input_column: document_text

  entity_types: "person,organization,location,miscellaneous"

  output_format: flat  # Separate column per entity type
  include_confidence: true
  batch_size: 16
```

### Custom Entity Patterns

Define domain-specific entities with regex:

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: custom_entities

  method: spacy
  spacy_model: en_core_web_lg

  input_column: ticket_text

  entity_types: "person,organization,email,phone,ticket_id,account_id,order_id"

  # Custom patterns as JSON
  custom_entities: |
    {
      "ticket_id": ["TICKET-\\d{6}", "TKT\\d+"],
      "account_id": ["ACC\\d{6,8}", "ACCT-[A-Z0-9]+"],
      "order_id": ["ORD\\d{8}", "#\\d{6,}"],
      "product_sku": ["SKU-[A-Z0-9]{6,}"]
    }

  output_format: structured
  include_confidence: true
```

### Flat Output Format

Separate columns for each entity type:

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: flat_entities

  method: spacy
  spacy_model: en_core_web_md

  input_column: text

  entity_types: "person,organization,email,phone"

  output_format: flat  # Creates: person_entities, organization_entities, etc.
  deduplicate: true
```

Output:
```
| text                              | person_entities  | organization_entities | email_entities        | phone_entities  |
|-----------------------------------|------------------|-----------------------|-----------------------|-----------------|
| Contact John at john@acme.com     | John             | Acme                  | john@acme.com         |                 |
| Call Jane at 555-1234             | Jane             |                       |                       | 555-1234        |
```

### Structured Output Format (JSON)

All entities in one JSON column:

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: json_entities

  method: spacy
  spacy_model: en_core_web_lg

  input_column: text

  entity_types: "person,organization,location,email,phone,date,money"

  output_format: structured  # Creates: entities (JSON), entity_count, *_count columns
  include_confidence: true
  include_spans: true
  deduplicate: true
```

Output:
```
| text                              | entities                                                                                | entity_count |
|-----------------------------------|-----------------------------------------------------------------------------------------|--------------|
| Meet John at Acme Corp in NYC     | [{"type":"person","value":"John","confidence":1.0,"start":5,"end":9}, ...]             | 3            |
```

## Output Schema

### Structured Format (`output_format: structured`):

- `entities`: JSON array of extracted entities
- `entity_count`: Total number of entities extracted
- `{entity_type}_count`: Count per entity type (e.g., `person_count`, `email_count`)

Entity JSON structure:
```json
{
  "type": "person",
  "value": "John Smith",
  "confidence": 0.95,
  "start": 10,
  "end": 20,
  "linked_id": "KB:12345"
}
```

### Flat Format (`output_format: flat`):

- `{entity_type}_entities`: Comma-separated values for each type
- Example: `person_entities`, `organization_entities`, `email_entities`

## Method Comparison

| Feature | spaCy | Transformer | LLM |
|---------|-------|-------------|-----|
| **Speed** | Fastest (5-20ms) | Medium (50-200ms) | Slower (200-500ms) |
| **Cost** | $0 | $0 | $0.15-$5 per 1M tokens |
| **Accuracy** | High (85-90%) | High (85-92%) | Highest (90-95%) |
| **Custom Entities** | Regex only | Limited | Full flexibility |
| **Internet Required** | No | No | Yes |
| **Setup Complexity** | Low | Medium | Low |
| **Best For** | Production, high volume | Balanced needs | Complex/custom entities |

### Detailed Comparison

**spaCy** (Recommended for most use cases):
- Pros: Fastest, free, excellent for standard entities, supports custom regex patterns
- Cons: Limited to pre-trained entity types + regex patterns
- Best for: Support tickets, emails, documents with standard entities

**Transformer**:
- Pros: Free, good accuracy, works offline
- Cons: Slower than spaCy, harder to customize, requires GPU for best performance
- Best for: When you need better accuracy than spaCy but can't afford LLM costs

**LLM**:
- Pros: Most flexible, best accuracy, handles complex/custom entities, entity linking
- Cons: API costs, slower, requires internet
- Best for: Complex extraction, custom entity types, when accuracy is critical

## Cost Estimation

### LLM Costs (per 1,000 texts)

Assuming average 150 tokens input, 100 tokens output per text:

| Model | Input Cost | Output Cost | Total per 1k | Monthly (10k/day) |
|-------|------------|-------------|--------------|-------------------|
| GPT-4o-mini | $0.023 | $0.060 | $0.083 | $24.90 |
| GPT-4o | $0.75 | $1.50 | $2.25 | $675 |
| GPT-4 | $4.50 | $6.00 | $10.50 | $3,150 |
| Claude 3 Haiku | $0.038 | $0.125 | $0.163 | $48.90 |

**Recommendation**: Use spaCy for standard entities, LLM only for custom/complex cases

### Infrastructure Costs

- **spaCy**: ~100 MB RAM per process, CPU only ($0.05/hr EC2 instance)
- **Transformer**: ~2 GB RAM, GPU recommended ($0.50/hr for GPU instance)
- **LLM**: No infrastructure needed (API-based)

## Advanced Patterns

### Pipeline: Extract -> Classify -> Route

```yaml
# Step 1: Extract entities from tickets
- type: dagster_component_templates.EntityExtractorComponent
  attributes:
    asset_name: ticket_entities
    method: spacy
    spacy_model: en_core_web_lg
    input_column: ticket_text
    entity_types: "person,email,phone,order_id,product"
    output_format: structured

# Step 2: Classify tickets (use ticket_classifier component)
# Step 3: Route based on entities and classification
```

### Hybrid: spaCy + LLM for Custom Entities

Use spaCy for standard entities, LLM for complex custom entities:

```yaml
# Step 1: Fast spaCy extraction
- type: dagster_component_templates.EntityExtractorComponent
  attributes:
    asset_name: standard_entities
    method: spacy
    entity_types: "person,organization,email,phone,date,money"

# Step 2: LLM for custom entities
- type: dagster_component_templates.EntityExtractorComponent
  attributes:
    asset_name: custom_entities
    method: llm
    entity_types: "issue_type,feature_request,integration_name,api_endpoint"
```

### PII Detection Pipeline

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: pii_detected

  method: spacy
  spacy_model: en_core_web_lg

  input_column: user_content

  # PII entity types
  entity_types: "person,email,phone,location,date"

  # Custom patterns for sensitive data
  custom_entities: |
    {
      "ssn": ["\\d{3}-\\d{2}-\\d{4}"],
      "credit_card": ["\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}"],
      "ip_address": ["\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"]
    }

  output_format: structured
  include_spans: true  # For redaction
```

### Multi-Language Support

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: multilingual_entities

  method: llm  # Best for multi-language
  llm_provider: anthropic
  llm_model: claude-3-haiku-20240307

  input_column: text

  entity_types: "person,organization,location,product"

  # Or use language-specific spaCy models
  # method: spacy
  # spacy_model: es_core_news_lg  # Spanish
  # spacy_model: de_core_news_lg  # German
  # spacy_model: fr_core_news_lg  # French
```

## Integration Examples

### Support Ticket Enrichment

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: enriched_tickets

  method: spacy
  spacy_model: en_core_web_lg

  input_column: description

  entity_types: "person,organization,email,phone,product,order_id,date"

  custom_entities: |
    {
      "ticket_id": ["#\\d{6,}", "TICKET-\\d+"],
      "order_id": ["ORD\\d{8}", "ORDER[#-]\\d+"],
      "account_id": ["ACC\\d{6,}"]
    }

  output_format: structured
  include_confidence: true
```

### Email Processing

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: email_entities

  method: spacy
  spacy_model: en_core_web_lg

  input_column: email_body

  entity_types: "person,organization,email,phone,location,date,money"

  output_format: flat  # Easy to work with for downstream processing
  deduplicate: true
```

### Document Analysis

```yaml
type: dagster_component_templates.EntityExtractorComponent
attributes:
  asset_name: document_entities

  method: transformer
  transformer_model: dbmdz/bert-large-cased-finetuned-conll03-english

  input_column: document_text

  entity_types: "person,organization,location,miscellaneous"

  output_format: structured
  include_confidence: true
  include_spans: true
  batch_size: 8
```

## Performance Tuning

### For High Volume (>100k texts/day)

```yaml
attributes:
  method: spacy
  spacy_model: en_core_web_sm  # Smaller, faster model
  entity_types: "person,organization,email,phone"  # Only what you need
  include_confidence: false
  include_spans: false
  deduplicate: true
  batch_size: 100
```

### For High Accuracy

```yaml
attributes:
  method: llm
  llm_model: gpt-4-turbo
  entity_types: "all_needed_types"
  include_confidence: true
  include_spans: true
  link_entities: true
  temperature: 0.0
```

### For Cost Optimization

```yaml
attributes:
  method: spacy  # Free
  spacy_model: en_core_web_lg  # Best free accuracy
  # Use custom_entities for domain-specific patterns
  output_format: structured
```

## Monitoring & Analytics

Metadata provided:

```python
{
  "method": "spacy",
  "model": "en_core_web_lg",
  "num_texts_processed": 1000,
  "total_entities_extracted": 3500,
  "avg_entities_per_text": 3.5,
  "entity_type_counts": {
    "person": 800,
    "organization": 600,
    "email": 450,
    "phone": 300,
    "order_id": 250,
    "product": 550,
    "date": 350,
    "money": 200
  },
  "output_format": "structured"
}
```

## Troubleshooting

### Low Extraction Rate

**Problem**: Few entities being extracted

**Solutions**:
1. Use better model (en_core_web_lg instead of en_core_web_sm)
2. Add custom entity patterns for domain-specific entities
3. Try LLM method for better flexibility
4. Check if entity types match your data

### Incorrect Entity Types

**Problem**: Entities classified as wrong type

**Solutions**:
1. Use LLM method with explicit instructions
2. Add custom entity patterns with clear regex
3. Use entity linking to validate (LLM only)
4. Post-process with rules to correct types

### Performance Issues

**Problem**: Extraction is too slow

**Solutions**:
1. Use spaCy instead of LLM (10-50x faster)
2. Use smaller spaCy model (en_core_web_sm)
3. Reduce entity types to only what you need
4. Disable confidence/spans if not needed
5. Increase batch_size for transformer method

### High Costs (LLM)

**Problem**: LLM costs too high

**Solutions**:
1. Switch to spaCy or transformer (both free)
2. Use GPT-4o-mini or Claude 3 Haiku
3. Pre-filter texts that likely contain entities
4. Reduce max_tokens
5. Cache results for duplicate texts

## Best Practices

1. **Start with spaCy**: Try spaCy first, use LLM only if needed
2. **Use Custom Patterns**: Add regex patterns for domain-specific entities
3. **Enable Deduplication**: Avoid duplicate entities in output
4. **Choose Right Output Format**: Structured for complex analysis, flat for simple use
5. **Monitor Accuracy**: Review sample extractions regularly
6. **Optimize Entity Types**: Only extract what you need
7. **Consider Privacy**: Handle PII appropriately (encryption, access control)
8. **Test Model Sizes**: Balance accuracy vs performance
9. **Use Confidence Scores**: Filter low-confidence extractions
10. **Batch Processing**: Use appropriate batch sizes for efficiency

## Related Components

- **ticket_classifier**: Classify tickets after entity extraction
- **priority_scorer**: Score based on extracted entities
- **text_moderator**: Detect PII for moderation
- **sentiment_analyzer**: Analyze sentiment of extracted mentions

## Support & Resources

- [spaCy Documentation](https://spacy.io/usage)
- [Hugging Face Transformers](https://huggingface.co/models?pipeline_tag=ner)
- [OpenAI API Documentation](https://platform.openai.com/docs)
- [Anthropic API Documentation](https://docs.anthropic.com)

## License

This component is part of the Dagster Components Templates library.
