# Content Ingestion

Generate sample user-generated content for content moderation and analysis pipelines.

## Overview

The Content Ingestion component creates realistic sample user-generated content data for testing and developing content moderation systems. It generates diverse content types with varying quality levels.

## Features

- **Realistic Sample Data**: Generates 50 diverse content items
- **Multiple Content Types**: Text posts, comments, reviews, and messages
- **Varied Content Quality**: Mix of high-quality, moderate, and low-quality content
- **Engagement Metrics**: Includes likes, comments, shares, and reports
- **User Attribution**: Associates content with sample users
- **Timestamp Data**: Provides creation timestamps for time-series analysis

## Configuration

### Required Parameters

- `asset_name`: Name of the asset to create

### Optional Parameters

- `description`: Asset description (default: "User-generated content for moderation")
- `group_name`: Asset group for organization (default: "content")
- `include_sample_metadata`: Include data preview in metadata (default: true)

## Usage

```yaml
type: dagster_component_templates.ContentIngestionComponent
attributes:
  asset_name: user_content
  description: "User-generated content for moderation pipeline"
```

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| content_id | int | Unique content identifier |
| user_id | string | User who created the content |
| content_type | string | Type of content (text_post, comment, review, message) |
| content_text | string | The actual content text |
| created_at | datetime | Content creation timestamp |
| likes | int | Number of likes (0-100) |
| comments | int | Number of comments (0-50) |
| shares | int | Number of shares (0-20) |
| reports | int | Number of user reports (0-5) |

## Use Cases

- Content moderation testing
- Sentiment analysis model training
- Engagement pattern analysis
- Spam detection development
- User behavior studies

## Dependencies

- pandas>=1.5.0

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
