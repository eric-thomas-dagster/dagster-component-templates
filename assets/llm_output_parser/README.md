# LLM Output Parser Asset

Parse and validate LLM outputs into structured formats using Dagster's IO manager pattern. Accepts LLM responses from upstream assets and converts them to structured data - just connect LLM components!

## Overview

This component parses LLM responses automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- LLM Prompt Executor (output: string or dict)
- LLM Chain Executor (output: dict with chain results)
- Text File Readers (output: string)
- Any component that outputs text needing parsing

Perfect for:
- Extracting structured data from LLM responses
- Validating JSON outputs
- Converting text to DataFrames
- Parsing lists, tables, and key-value pairs

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **Multiple Parsers**: JSON, CSV, lists, key-value, markdown tables, custom regex
- **Schema Validation**: Validate JSON against schema
- **Code Block Extraction**: Extract code from markdown
- **Markdown Stripping**: Clean markdown formatting
- **Multiple Outputs**: Dictionary, list, or DataFrame
- **Error Handling**: Graceful handling of parse failures

## Input and Output

### Input Requirements

This component accepts LLM output from upstream assets via IO managers. The input should be:
- **String**: Text response from LLM (e.g., JSON string, CSV text, markdown)
- **Dict**: Already parsed response (for additional validation/transformation)

The parser processes the text and extracts structured data according to the `parser_type`.

### Output Format

The component outputs parsed data in the specified format:
- **dict** (default): Python dictionary (for JSON, key-value parsers)
- **list**: Python list (for list parser, CSV rows)
- **dataframe**: Pandas DataFrame (for CSV, markdown tables)

## Configuration

### Required
- **asset_name** (string) - Name of the asset
- **parser_type** (string) - Type of parser: `"json"`, `"csv"`, `"list"`, `"key_value"`, `"markdown_table"`, `"custom"`

### Optional
- **validation_schema** (string) - JSON schema for validation
- **custom_regex** (string) - Custom regex pattern (for custom parser)
- **extract_code_blocks** (boolean) - Extract code from markdown (default: `false`)
- **strip_markdown** (boolean) - Clean markdown formatting (default: `true`)
- **output_format** (string) - `"dict"`, `"list"`, `"dataframe"` (default: `"dict"`)
- **strict_validation** (boolean) - Raise error on failure (default: `false`)
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Examples

### Parse JSON
```yaml
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: parsed_json
  parser_type: json
  validation_schema: '{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "number"}}}'
```

### Extract List
```yaml
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: bullet_list
  parser_type: list
  output_format: list
```

### Parse CSV
```yaml
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: csv_data
  parser_type: csv
  output_format: dataframe
```

### Custom Regex
```yaml
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: custom_extraction
  parser_type: custom
  custom_regex: 'Price: \$(\d+\.?\d*)'
```

## Example Pipelines with IO Manager Pattern

### Pipeline 1: LLM to Structured JSON

```yaml
# Step 1: Execute LLM prompt
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: llm_raw_response
  provider: openai
  model: gpt-4
  prompt: "Extract customer information from this text: 'John Doe, age 30, email john@example.com'. Return as JSON."
  api_key: ${OPENAI_API_KEY}
  response_format: json

# Step 2: Parse and validate JSON output (automatically receives llm_raw_response)
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: parsed_customer_data
  parser_type: json
  validation_schema: '{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "number"}, "email": {"type": "string", "format": "email"}}, "required": ["name", "email"]}'
  strict_validation: true
  output_format: dict
```

**Visual Connection:**
```
llm_raw_response → parsed_customer_data
```

**Output format:**
```python
{
  "name": "John Doe",
  "age": 30,
  "email": "john@example.com"
}
```

### Pipeline 2: Extract Lists from LLM

```yaml
# Step 1: Generate list with LLM
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: recommendations_text
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  prompt: "List 5 best practices for data engineering. Format as a numbered list."
  api_key: ${ANTHROPIC_API_KEY}

# Step 2: Parse list into structured format
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: recommendations_list
  parser_type: list
  output_format: list
```

**Visual Connection:**
```
recommendations_text → recommendations_list
```

**Output format:**
```python
[
  "Use version control for data pipelines",
  "Implement data quality checks",
  "Document data lineage",
  "Monitor pipeline performance",
  "Implement error handling and retries"
]
```

### Pipeline 3: LLM Chain to DataFrame

```yaml
# Step 1: Extract data from document
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: invoice_text
  file_path: /data/invoices/invoice_001.pdf

# Step 2: Extract structured data with LLM
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: extracted_items
  provider: openai
  model: gpt-4
  prompt: "Extract line items from this invoice as CSV with columns: item, quantity, price: {text}"
  api_key: ${OPENAI_API_KEY}

# Step 3: Parse CSV to DataFrame
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: invoice_dataframe
  parser_type: csv
  output_format: dataframe
```

**Visual Connections:**
```
invoice_text → extracted_items → invoice_dataframe
```

**Output format:** Pandas DataFrame
```
       item  quantity  price
0   Widget         10   5.99
1  Gadget          5  12.50
2   Doohickey       2  25.00
```

### Pipeline 4: Parse Markdown Tables

```yaml
# Step 1: Generate comparison table
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: comparison_table
  provider: openai
  model: gpt-4
  prompt: "Create a comparison table of Python, Java, and JavaScript with columns: Language, Type System, Performance. Format as markdown table."
  api_key: ${OPENAI_API_KEY}
  response_format: markdown

# Step 2: Parse markdown table to DataFrame
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: language_comparison_df
  parser_type: markdown_table
  output_format: dataframe
```

**Visual Connection:**
```
comparison_table → language_comparison_df
```

### Pipeline 5: Extract Code from LLM Response

```yaml
# Step 1: Generate code with LLM
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: generated_code
  provider: openai
  model: gpt-4
  system_prompt: "You are an expert Python developer."
  prompt: "Write a Python function to calculate Fibonacci numbers with memoization."
  api_key: ${OPENAI_API_KEY}

# Step 2: Extract code blocks from response
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: clean_code
  parser_type: custom
  extract_code_blocks: true
  strip_markdown: true
  custom_regex: '```python\n(.*?)\n```'
```

**Visual Connection:**
```
generated_code → clean_code
```

### Pipeline 6: Chain Analysis with Parsing

```yaml
# Step 1: Fetch data
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: user_reviews
  api_url: https://api.example.com/reviews
  output_format: dict

# Step 2: Analyze with LLM chain
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: review_analysis
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  chain_steps: '[
    {"prompt": "Categorize these reviews by topic: {user_reviews}", "output_key": "categories"},
    {"prompt": "For each category, extract sentiment as JSON: {categories}", "output_key": "sentiment_json"}
  ]'

# Step 3: Parse sentiment JSON from chain output
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: sentiment_structured
  parser_type: json
  validation_schema: '{"type": "object", "additionalProperties": {"type": "object", "properties": {"sentiment": {"type": "string"}, "count": {"type": "number"}}}}'
  output_format: dict
```

**Visual Connections:**
```
user_reviews → review_analysis → sentiment_structured
```

### Pipeline 7: Parse Key-Value Pairs

```yaml
# Step 1: Extract metadata
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: document_metadata
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  prompt: "Extract metadata from this document. Format as 'Key: Value' pairs."
  api_key: ${ANTHROPIC_API_KEY}

# Step 2: Parse key-value pairs
type: dagster_component_templates.LLMOutputParserComponent
attributes:
  asset_name: metadata_dict
  parser_type: key_value
  output_format: dict
```

**Visual Connection:**
```
document_metadata → metadata_dict
```

**Output format:**
```python
{
  "Author": "Jane Smith",
  "Date": "2024-01-15",
  "Topic": "Machine Learning",
  "Pages": "45"
}
```

## Parser Types

- **json**: Extract and validate JSON
- **csv**: Parse CSV data
- **list**: Extract bullet/numbered lists
- **key_value**: Parse key: value pairs
- **markdown_table**: Parse markdown tables
- **custom**: Use custom regex pattern

## Requirements
- pandas >= 2.0.0, jsonschema >= 4.0.0

## License
MIT License
