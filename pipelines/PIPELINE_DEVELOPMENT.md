# Pipeline Development Guide

Complete guide for creating new pipeline templates in the Dagster Component Templates library.

## Overview

Pipeline templates are pre-built, multi-component workflows that solve specific business use cases. Each pipeline combines multiple components into a cohesive data pipeline with configuration options for different platforms, environments, and destinations.

## Table of Contents

- [Pipeline Structure](#pipeline-structure)
- [YAML Formats](#yaml-formats)
- [Metadata Guidelines](#metadata-guidelines)
- [Component Configuration](#component-configuration)
- [Parameters](#parameters)
- [Testing](#testing)
- [Publishing](#publishing)
- [Examples](#examples)

## Pipeline Structure

Each pipeline consists of:

1. **YAML File**: Defines components, parameters, and metadata
2. **Manifest Entry**: Auto-generated from YAML file
3. **GitHub Integration**: Served via GitHub raw URLs

```
pipelines/
├── my_pipeline.yaml           # Pipeline definition
├── generate_manifest.py       # Generates manifest.json
├── manifest.json              # Auto-generated (DO NOT EDIT MANUALLY)
└── PIPELINE_DEVELOPMENT.md    # This guide
```

## YAML Formats

We support two YAML formats. Choose based on your needs:

### Format 1: Structured Pipeline Format (Recommended)

**Best for:** Complex pipelines with many parameters and conditional logic

```yaml
# Pipeline Name
#
# Use Case: What business problem does this solve?
# Business Outcome: What value does it deliver?
# Category: customer_analytics | marketing | content | security | other
# Estimated Savings: $XXX,XXX/year vs traditional solutions

pipeline:
  name: "My Pipeline"
  version: "1.0.0"

  # High-level configuration
  parameters:
    platform:
      type: string
      default: "shopify"
      enum: ["shopify", "woocommerce", "magento"]
      description: "E-commerce platform"
      required: true
      environment_specific: false

    api_key:
      type: string
      default: ""
      description: "API key for authentication"
      required: true
      sensitive: true
      environment_specific: true
      show_if:
        platform: "shopify"

  # Components that make up the pipeline
  components:
    - id: shopify_ingestion
      instance_name: orders_data
      config:
        asset_name: "orders_data"
        resource_type: "orders"
        description: "Ingest orders from Shopify"
        group_name: "ingestion"

    - id: ecommerce_standardizer
      instance_name: standardized_orders
      depends_on: [orders_data]
      config:
        asset_name: "standardized_orders"
        platform: "${platform}"
        description: "Standardize order data"
        group_name: "transformation"
```

### Format 2: Multi-Document YAML Format

**Best for:** Simple pipelines with few parameters

```yaml
# Pipeline Name
#
# Use Case: What business problem does this solve?
# Business Outcome: What value does it deliver?

---
# Component 1: Data Ingestion
type: dagster_component_templates.ShopifyIngestionComponent
attributes:
  asset_name: orders_data
  resource_type: orders
  description: "Ingest orders from Shopify"
  group_name: ingestion

---
# Component 2: Data Transformation
type: dagster_component_templates.EcommerceStandardizerComponent
attributes:
  asset_name: standardized_orders
  platform: shopify
  description: "Standardize order data"
  group_name: transformation
```

## Metadata Guidelines

### Required Metadata (in comments at top of file)

```yaml
# [Pipeline Name] Pipeline
#
# Use Case: [One-line description of the business problem]
# Business Outcome: [One-line description of the value delivered]
```

### Optional Metadata

```yaml
# Category: customer_analytics | marketing | content | security | other
# Estimated Savings: $XXX,XXX/year vs [competitor/solution]
# Description: [Longer description if needed]
```

### Category Guidelines

- **customer_analytics**: LTV, segmentation, churn prediction
- **marketing**: Attribution, campaign ROI, A/B testing
- **content**: Moderation, sentiment analysis, recommendations
- **security**: Fraud detection, anomaly detection
- **customer_support**: Ticket automation, Q&A systems
- **other**: Everything else

## Component Configuration

### Component Structure

```yaml
components:
  - id: component_id              # Component ID from manifest
    instance_name: unique_name    # Unique name for this instance
    depends_on: []                # List of instance_names this depends on
    enabled: true                 # Optional: can be ${parameter} for conditional
    config:                       # Component-specific configuration
      asset_name: "my_asset"
      description: "What this component does"
      # ... other component params
```

### Parameter Substitution

Use `${parameter_name}` to reference pipeline parameters:

```yaml
parameters:
  platform:
    type: string
    default: "shopify"

components:
  - id: ecommerce_standardizer
    config:
      platform: "${platform}"    # Will be replaced with "shopify"
```

### Conditional Components

```yaml
parameters:
  enable_sentiment:
    type: boolean
    default: true

components:
  - id: sentiment_analyzer
    enabled: "${enable_sentiment}"   # Only runs if true
    config:
      # ...
```

### Dependencies

Use `depends_on` to ensure components run in order:

```yaml
components:
  - id: data_ingestion
    instance_name: raw_data
    config:
      asset_name: "raw_data"

  - id: data_transform
    instance_name: clean_data
    depends_on: [raw_data]         # Waits for raw_data
    config:
      asset_name: "clean_data"
      source_asset: "raw_data"     # Reference the dependency
```

## Parameters

### Parameter Types

```yaml
parameters:
  # String
  platform:
    type: string
    default: "shopify"
    description: "Platform name"
    required: true

  # Enum (dropdown)
  environment:
    type: string
    enum: ["dev", "staging", "prod"]
    default: "dev"

  # Number
  threshold:
    type: number
    default: 0.7
    minimum: 0.0
    maximum: 1.0

  # Integer
  batch_size:
    type: integer
    default: 100
    minimum: 1

  # Boolean
  enable_feature:
    type: boolean
    default: true

  # Array
  categories:
    type: array
    items:
      type: string
    default: ["cat1", "cat2"]
```

### Parameter Metadata

#### `environment_specific`

Mark parameters that vary by environment (local/branch/production):

```yaml
api_key:
  type: string
  environment_specific: true     # Different per environment
  sensitive: true
  description: "API key"

prediction_period:
  type: integer
  environment_specific: false    # Same across all environments
  default: 30
```

#### `show_if` (Conditional Visibility)

Show/hide parameters based on other selections:

```yaml
platform:
  type: string
  enum: ["shopify", "woocommerce"]
  default: "shopify"

shopify_domain:
  type: string
  show_if:
    platform: "shopify"          # Only show when platform=shopify

woocommerce_url:
  type: string
  show_if:
    platform: "woocommerce"      # Only show when platform=woocommerce
```

#### `sensitive`

Mark parameters containing credentials:

```yaml
api_key:
  type: string
  sensitive: true                # Will be treated as password field
```

#### `environment_defaults`

Set different defaults per environment:

```yaml
database:
  type: string
  enum: ["duckdb", "postgres", "snowflake"]
  environment_defaults:
    local: "duckdb"
    branch: "postgres"
    production: "snowflake"
```

### Complete Parameter Example

```yaml
parameters:
  # Business logic (shared across environments)
  prediction_period_months:
    type: integer
    default: 24
    minimum: 1
    maximum: 60
    description: "Number of months to predict forward"
    required: true
    environment_specific: false

  # Platform selection (shared)
  ecommerce_platform:
    type: string
    default: "shopify"
    enum: ["shopify", "woocommerce", "magento"]
    description: "E-commerce platform"
    required: true
    environment_specific: false

  # Platform-specific auth (environment-specific)
  shopify_domain:
    type: string
    description: "Your Shopify shop domain"
    required: true
    environment_specific: true
    show_if:
      ecommerce_platform: "shopify"

  shopify_access_token:
    type: string
    description: "Shopify Admin API access token"
    required: true
    sensitive: true
    environment_specific: true
    show_if:
      ecommerce_platform: "shopify"

  # Output destination (environment-specific)
  output_destination:
    type: string
    enum: ["duckdb", "postgres", "snowflake"]
    description: "Database destination"
    required: true
    environment_specific: true
    environment_defaults:
      local: "duckdb"
      branch: "postgres"
      production: "snowflake"
```

## Testing

### 1. Validate YAML Syntax

```bash
python3 -c "import yaml; yaml.safe_load(open('my_pipeline.yaml'))"
```

### 2. Generate Manifest

```bash
cd pipelines
python3 generate_manifest.py
```

Check output:
```
✓ My Pipeline - X components
✅ Generated manifest with 11 pipelines
```

### 3. Verify Manifest Entry

```bash
python3 -c "import json; manifest = json.load(open('manifest.json')); pipe = next(p for p in manifest['pipelines'] if p['id'] == 'my_pipeline'); print(json.dumps(pipe, indent=2))"
```

### 4. Test in Dagster Designer

1. Commit and push to GitHub
2. Wait 1-2 minutes for CDN cache
3. Open Dagster Designer
4. Navigate to Pipeline Templates
5. Find your pipeline
6. Click "Configure & Install"
7. Verify:
   - All parameters show correctly
   - Conditional visibility works
   - Environment tabs work
   - Can install successfully

## Publishing

### 1. Create Pipeline YAML

```bash
cd pipelines
touch my_new_pipeline.yaml
# Edit the file with your pipeline definition
```

### 2. Add Metadata Comments

```yaml
# My New Pipeline
#
# Use Case: Solve specific business problem
# Business Outcome: Deliver measurable value
# Category: customer_analytics
# Estimated Savings: $50,000/year vs Manual Process
```

### 3. Define Components

Add all components with proper configuration.

### 4. Generate Manifest

```bash
python3 generate_manifest.py
```

### 5. Commit to GitHub

```bash
git add my_new_pipeline.yaml manifest.json
git commit -m "Add My New Pipeline

- Solves [problem]
- Includes [X] components
- Supports [platforms]"
git push
```

### 6. Verify in UI

Wait 1-2 minutes, then check Dagster Designer UI.

## Examples

### Example 1: Simple Pipeline (Multi-Doc Format)

```yaml
# Customer Segmentation Pipeline
#
# Use Case: Segment customers by behavior and value
# Business Outcome: Personalize marketing campaigns

---
type: dagster_component_templates.EcommerceStandardizerComponent
attributes:
  asset_name: orders
  platform: shopify
  description: "Standardized order data"

---
type: dagster_component_templates.CustomerSegmentationComponent
attributes:
  asset_name: customer_segments
  transaction_data_asset: orders
  description: "RFM customer segments"

---
type: dagster_component_templates.DLTDataFrameWriterComponent
attributes:
  asset_name: segments_output
  source_asset: customer_segments
  table_name: customer_segments
  destination: ""
  description: "Write segments to database"
```

### Example 2: Complex Pipeline (Structured Format)

```yaml
# Multi-Channel Attribution Pipeline
#
# Use Case: Measure marketing channel effectiveness
# Business Outcome: Optimize marketing spend across channels
# Category: marketing
# Estimated Savings: $200,000/year vs Attribution Platform

pipeline:
  name: "Multi-Channel Attribution"
  version: "1.0.0"

  parameters:
    attribution_model:
      type: string
      enum: ["first_touch", "last_touch", "linear", "time_decay", "algorithmic"]
      default: "linear"
      description: "Attribution model to use"
      required: true
      environment_specific: false

    lookback_days:
      type: integer
      default: 30
      minimum: 7
      maximum: 90
      description: "Attribution lookback window"
      required: false
      environment_specific: false

    # Data sources
    google_ads_enabled:
      type: boolean
      default: true
      description: "Include Google Ads data"
      environment_specific: false

    facebook_ads_enabled:
      type: boolean
      default: true
      description: "Include Facebook Ads data"
      environment_specific: false

    # Google Ads credentials
    google_ads_customer_id:
      type: string
      required: true
      environment_specific: true
      show_if:
        google_ads_enabled: true

    google_ads_refresh_token:
      type: string
      required: true
      sensitive: true
      environment_specific: true
      show_if:
        google_ads_enabled: true

    # Facebook credentials
    facebook_access_token:
      type: string
      required: true
      sensitive: true
      environment_specific: true
      show_if:
        facebook_ads_enabled: true

  components:
    - id: google_ads_ingestion
      instance_name: google_ads_data
      enabled: "${google_ads_enabled}"
      config:
        asset_name: "google_ads_data"
        customer_id: "${google_ads_customer_id}"
        description: "Google Ads campaign data"
        group_name: "marketing_data"

    - id: facebook_ads_ingestion
      instance_name: facebook_ads_data
      enabled: "${facebook_ads_enabled}"
      config:
        asset_name: "facebook_ads_data"
        access_token: "${facebook_access_token}"
        description: "Facebook Ads campaign data"
        group_name: "marketing_data"

    - id: ad_spend_standardizer
      instance_name: unified_ad_data
      config:
        asset_name: "unified_ad_data"
        description: "Unified marketing data"
        group_name: "marketing_data"

    - id: multi_touch_attribution
      instance_name: attribution_results
      depends_on: [unified_ad_data]
      config:
        asset_name: "attribution_results"
        ad_spend_asset: "unified_ad_data"
        attribution_model: "${attribution_model}"
        lookback_days: ${lookback_days}
        description: "Multi-touch attribution analysis"
        group_name: "marketing_analytics"

    - id: dlt_dataframe_writer
      instance_name: attribution_output
      depends_on: [attribution_results]
      config:
        asset_name: "attribution_output"
        source_asset: "attribution_results"
        table_name: "marketing_attribution"
        destination: ""
        description: "Write attribution to database"
        group_name: "marketing_analytics"
```

## Best Practices

### 1. Clear Naming

- Pipeline files: `{purpose}_{type}_pipeline.yaml`
- Instance names: Descriptive and unique
- Asset names: Match instance names for clarity

### 2. Documentation

- Always include use case and business outcome
- Add component-level comments explaining purpose
- Document expected data schemas in comments

### 3. Parameters

- Use `environment_specific: true` for credentials
- Use `show_if` to reduce clutter
- Provide sensible defaults
- Add helpful descriptions

### 4. Dependencies

- Explicitly declare with `depends_on`
- Use `source_asset` in config to reference data
- Test dependency order locally

### 5. Output

- Always include `dlt_dataframe_writer` as final step
- Configure sensible table names
- Support multiple destinations

### 6. Testing

- Test with all parameter combinations
- Verify conditional components work
- Test in all three environments

## Common Patterns

### Pattern 1: Multi-Platform Support

```yaml
parameters:
  platform:
    type: string
    enum: ["shopify", "woocommerce", "magento"]

  shopify_token:
    type: string
    show_if: {platform: "shopify"}

  woocommerce_key:
    type: string
    show_if: {platform: "woocommerce"}
```

### Pattern 2: Optional Components

```yaml
parameters:
  enable_enrichment:
    type: boolean
    default: false

components:
  - id: data_enrichment
    enabled: "${enable_enrichment}"
```

### Pattern 3: Environment-Specific Defaults

```yaml
parameters:
  database:
    type: string
    environment_defaults:
      local: "duckdb"
      branch: "postgres"
      production: "snowflake"
```

## Troubleshooting

### Pipeline Doesn't Appear in UI

1. Check YAML syntax: `python3 -c "import yaml; yaml.safe_load(open('file.yaml'))"`
2. Regenerate manifest: `python3 generate_manifest.py`
3. Verify manifest entry exists
4. Wait 2 minutes for GitHub CDN cache
5. Hard refresh browser (Cmd+Shift+R / Ctrl+Shift+F5)

### Components Show as 0

- Check that components are under `pipeline.components` (structured format)
- Or use `---` separators with `type:` (multi-doc format)
- Regenerate manifest and verify component count

### Parameters Not Showing

- Check manifest.json has pipeline_params
- May need to manually add environment_specific/show_if metadata
- Regenerate will preserve existing metadata

### Conditional Visibility Not Working

- Ensure `show_if` key matches parameter name exactly
- Verify parameter is marked `environment_specific: false` if it's a condition
- Check browser console for errors

## Support

- Issues: [GitHub Issues](https://github.com/eric-thomas-dagster/dagster-component-templates/issues)
- Documentation: [README.md](./README.md)
- Component Library: [Components](../assets/)

## Changelog

- **2024-12-04**: Initial pipeline development guide
- Added support for structured pipeline format
- Added auto-generation from YAML files
- Added environment-specific configuration
