# LLM Chain Executor Asset

Execute chains of LLM calls with context passing using Dagster's IO manager pattern. Accepts context data from upstream assets and runs multi-step reasoning workflows - just connect data-producing assets!

## Overview

This component executes sequential LLM operations where each step's output becomes context for the next. Context is automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor!

**Compatible with:**
- Document Text Extractors (output: string)
- Database Query Components (output: dict, DataFrame)
- REST API Fetchers (output: dict, string)
- File Readers (output: string)
- Other LLM components (output: string or dict)

Perfect for:
- Complex reasoning tasks
- Multi-step analysis workflows
- Iterative content generation
- Research and summarization pipelines

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **Multi-Step Execution**: Chain multiple LLM calls
- **Context Passing**: Each step's output available to next
- **Template Support**: Use `{variable}` syntax in prompts
- **Multiple Providers**: OpenAI, Anthropic
- **Flexible Steps**: Configure any number of steps

## Input and Output

### Input Requirements

This component accepts context data from upstream assets via IO managers. The input can be:
- **String**: Text content (e.g., document text, prompts)
- **Dict**: Structured data with any keys (e.g., `{"text": "...", "metadata": {...}}`)
- **DataFrame**: Tabular data (automatically converted to dict)

All input data becomes available as template variables in chain step prompts using `{variable_name}` syntax.

### Output Format

The component outputs a dictionary containing all chain step results:
```python
{
  "step1_output_key": "Result from step 1",
  "step2_output_key": "Result from step 2",
  "step3_output_key": "Result from step 3"
}
```

## Configuration

### Required
- **asset_name** (string) - Name of the asset
- **provider** (string) - LLM provider: `"openai"`, `"anthropic"`
- **model** (string) - Model to use (e.g., `"gpt-4"`, `"claude-3-5-sonnet-20241022"`)
- **chain_steps** (string) - JSON array of steps with `prompt` and `output_key`

### Optional
- **initial_context** (string) - Initial context JSON (if not receiving from upstream assets)
- **temperature** (number) - Response randomness 0.0-2.0 (default: `0.7`)
- **api_key** (string) - API key (use `${VAR_NAME}` for environment variable)
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Usage Examples

### Two-Step Chain
```yaml
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: analysis_chain
  provider: openai
  model: gpt-4
  chain_steps: '[
    {"prompt": "Summarize: {text}", "output_key": "summary"},
    {"prompt": "Generate title for: {summary}", "output_key": "title"}
  ]'
  initial_context: '{"text": "Long document..."}'
  api_key: ${OPENAI_API_KEY}
```

### Analysis Pipeline
```yaml
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: research_pipeline
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  chain_steps: '[
    {"prompt": "Extract key points from: {document}", "output_key": "key_points"},
    {"prompt": "Analyze implications of: {key_points}", "output_key": "analysis"},
    {"prompt": "Generate recommendations based on: {analysis}", "output_key": "recommendations"}
  ]'
  api_key: ${ANTHROPIC_API_KEY}
```

## Example Pipelines with IO Manager Pattern

### Pipeline 1: Document Analysis Chain

```yaml
# Step 1: Extract text from document
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: document_text
  file_path: /data/documents/research_paper.pdf

# Step 2: Run LLM chain for analysis (automatically receives document_text)
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: analysis_results
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  chain_steps: '[
    {"prompt": "Summarize this document in 3 sentences: {text}", "output_key": "summary"},
    {"prompt": "Extract the main thesis from: {summary}", "output_key": "thesis"},
    {"prompt": "List 5 key findings from the document based on: {summary}", "output_key": "key_findings"},
    {"prompt": "Suggest follow-up research questions based on: {thesis} and {key_findings}", "output_key": "research_questions"}
  ]'
  temperature: 0.7
```

**Visual Connection:**
```
document_text → analysis_results
```

**Output format:**
```python
{
  "summary": "Three sentence summary...",
  "thesis": "Main thesis statement...",
  "key_findings": "1. Finding one\n2. Finding two...",
  "research_questions": "1. Question one\n2. Question two..."
}
```

### Pipeline 2: Database to Insights

```yaml
# Step 1: Query database
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: sales_data
  query: "SELECT product, SUM(revenue) as total_revenue FROM sales GROUP BY product"
  connection_string: ${DATABASE_URL}

# Step 2: Run chain analysis on query results
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: sales_insights
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  api_key: ${ANTHROPIC_API_KEY}
  chain_steps: '[
    {"prompt": "Analyze this sales data: {sales_data}. Identify top performers.", "output_key": "top_performers"},
    {"prompt": "Based on {top_performers}, identify trends and patterns.", "output_key": "trends"},
    {"prompt": "Given {trends}, provide 3 strategic recommendations.", "output_key": "recommendations"}
  ]'
  temperature: 0.5
```

**Visual Connection:**
```
sales_data → sales_insights
```

### Pipeline 3: API Data Processing Chain

```yaml
# Step 1: Fetch from REST API
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: customer_feedback
  api_url: https://api.example.com/feedback
  output_format: dict

# Step 2: Process feedback with chain
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: feedback_analysis
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  chain_steps: '[
    {"prompt": "Categorize this feedback: {customer_feedback}", "output_key": "categories"},
    {"prompt": "For each category in {categories}, identify sentiment", "output_key": "sentiment"},
    {"prompt": "Based on {sentiment}, prioritize issues needing attention", "output_key": "priorities"},
    {"prompt": "Generate action items for: {priorities}", "output_key": "action_items"}
  ]'
```

**Visual Connection:**
```
customer_feedback → feedback_analysis
```

### Pipeline 4: Multi-Stage Content Generation

```yaml
# Step 1: Read content brief
type: dagster_component_templates.TextFileReaderComponent
attributes:
  asset_name: content_brief
  file_path: /data/briefs/blog_post_brief.txt

# Step 2: Generate content through chain
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: blog_content
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  chain_steps: '[
    {"prompt": "Create an outline for: {text}", "output_key": "outline"},
    {"prompt": "Write introduction section based on: {outline}", "output_key": "introduction"},
    {"prompt": "Write body sections following: {outline}", "output_key": "body"},
    {"prompt": "Write conclusion that ties together: {introduction} and {body}", "output_key": "conclusion"},
    {"prompt": "Generate SEO metadata for article with: {outline}", "output_key": "metadata"}
  ]'
  temperature: 0.8
```

**Visual Connection:**
```
content_brief → blog_content
```

### Pipeline 5: Chain Multiple Chains

```yaml
# Step 1: Extract data
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: report_text
  file_path: /data/reports/annual_report.pdf

# Step 2: First chain - extract structured data
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: structured_data
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  chain_steps: '[
    {"prompt": "Extract financial metrics from: {text}", "output_key": "financials"},
    {"prompt": "Extract key personnel mentioned in: {text}", "output_key": "personnel"}
  ]'

# Step 3: Second chain - generate insights from structured data
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: final_insights
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  api_key: ${ANTHROPIC_API_KEY}
  chain_steps: '[
    {"prompt": "Analyze financial health based on: {financials}", "output_key": "financial_analysis"},
    {"prompt": "Assess leadership based on: {personnel}", "output_key": "leadership_analysis"},
    {"prompt": "Provide overall assessment combining: {financial_analysis} and {leadership_analysis}", "output_key": "overall_assessment"}
  ]'
```

**Visual Connections:**
```
report_text → structured_data → final_insights
```

## Template Variable Syntax

In chain step prompts, use curly braces to reference:
- **Upstream asset data**: `{upstream_key}` (e.g., `{text}`, `{data}`)
- **Previous step outputs**: `{output_key}` from earlier steps

Example:
```json
[
  {"prompt": "Summarize: {text}", "output_key": "summary"},
  {"prompt": "Expand on: {summary}", "output_key": "expanded"}
]
```

## Requirements
- openai >= 1.0.0, anthropic >= 0.18.0

## License
MIT License
