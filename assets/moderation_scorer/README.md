# Moderation Scorer

Score and flag user-generated content using rule-based moderation algorithms.

## Overview

The Moderation Scorer component analyzes content from upstream sources and assigns moderation scores based on risk keywords, sentiment, and content quality. It provides automated content moderation decisions.

## Features

- **Rule-Based Scoring**: Analyzes content using configurable risk keywords
- **Sentiment Analysis**: Detects positive and negative language patterns
- **Risk Assessment**: Calculates risk scores (0-1 scale)
- **Automated Decisions**: Classifies content as approved, needs_review, or flagged
- **Flexible Thresholds**: Customizable scoring thresholds
- **Detailed Reasoning**: Provides explanation for moderation decisions

## Configuration

### Required Parameters

- `asset_name`: Name of the moderation results asset

### Optional Parameters

- `source_asset`: Upstream content asset (set via lineage)
- `risk_threshold`: Score threshold for flagging (default: 0.7, range 0-1)
- `review_threshold`: Score threshold for manual review (default: 0.4, range 0-1)
- `description`: Asset description
- `group_name`: Asset group for organization (default: "content_moderation")
- `include_sample_metadata`: Include sample data preview (default: true)

## Usage

```yaml
type: dagster_component_templates.ModerationScorerComponent
attributes:
  asset_name: moderated_content
  source_asset: user_content
  risk_threshold: 0.7
  review_threshold: 0.4
  description: "Automated content moderation scores"
```

## How It Works

1. **Content Loading**: Reads content from upstream asset
2. **Text Analysis**: Scans for risk keywords and sentiment indicators
3. **Score Calculation**:
   - Risk keywords add 0.3 points per match
   - Negative indicators add 0.2 points per match
   - Positive sentiment reduces score by 0.1 per match
4. **Decision Assignment**:
   - Score ≥ risk_threshold (0.7): **flagged** for removal
   - Score ≥ review_threshold (0.4): **needs_review** by moderator
   - Score < review_threshold: **approved**
5. **Reasoning**: Generates explanation based on detected patterns

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| content_id | any | Original content identifier |
| risk_score | float | Moderation risk score (0-1) |
| decision | string | Moderation decision (approved/needs_review/flagged) |
| risk_indicators | list | Detected risk keywords |
| sentiment_indicators | list | Detected sentiment patterns |
| reasoning | string | Explanation of the decision |

## Use Cases

- Automated content moderation
- Queue management for human moderators
- Spam detection
- Community safety
- Compliance enforcement

## Dependencies

- pandas>=1.5.0
