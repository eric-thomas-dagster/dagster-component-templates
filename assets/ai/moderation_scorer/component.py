"""Moderation Scorer Component.

Score content for moderation decisions based on various risk factors.
Uses rule-based scoring and can be extended with ML models.
"""

from typing import Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


class ModerationScorerComponent(Component, Model, Resolvable):
    """Component for scoring content for moderation decisions.

    This component analyzes content and assigns moderation risk scores
    based on various factors like sentiment, keywords, user history, etc.

    Example:
        ```yaml
        type: dagster_component_templates.ModerationScorerComponent
        attributes:
          asset_name: moderation_scores
          source_asset: user_content
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with content to score"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        description = self.description or "Content moderation scores and decisions"

        @asset(
            name=asset_name,
            description=description,
            group_name="content_moderation",
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def moderation_scorer_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            """Asset that scores content for moderation."""
            import re

            context.log.info("Scoring content for moderation")

            content_df = upstream
            context.log.info(f"Loaded {len(content_df)} content items")

            context.log.info(f"Scoring {len(content_df)} content items")

            # Create scores DataFrame
            scores = content_df.copy()

            # Define moderation rules
            risk_keywords = ['spam', 'scam', 'fraud', 'inappropriate', 'offensive', 'violence', 'hate']
            positive_keywords = ['great', 'excellent', 'amazing', 'love', 'wonderful', 'fantastic']
            negative_keywords = ['terrible', 'awful', 'horrible', 'disappointing', 'poor', 'worst']

            # Initialize scores
            scores['risk_score'] = 0.0
            scores['sentiment_score'] = 0.5  # neutral
            scores['moderation_decision'] = 'approved'
            scores['confidence'] = 0.0

            # Score each content item
            if 'content_text' in scores.columns:
                for idx, row in scores.iterrows():
                    text = str(row.get('content_text', '')).lower()
                    risk_score = 0.0

                    # Check for risk keywords
                    for keyword in risk_keywords:
                        if keyword in text:
                            risk_score += 0.3

                    # Calculate sentiment
                    positive_count = sum(1 for kw in positive_keywords if kw in text)
                    negative_count = sum(1 for kw in negative_keywords if kw in text)

                    if positive_count > 0:
                        sentiment = 0.7 + (positive_count * 0.1)
                    elif negative_count > 0:
                        sentiment = 0.3 - (negative_count * 0.1)
                    else:
                        sentiment = 0.5

                    # Text length factor
                    text_length = len(text)
                    if text_length < 10:
                        risk_score += 0.1  # Very short text is suspicious
                    elif text_length > 1000:
                        risk_score += 0.05  # Very long text needs review

                    # Cap risk score at 1.0
                    risk_score = min(risk_score, 1.0)

                    # Determine moderation decision
                    if risk_score > 0.5:
                        decision = 'flagged'
                        confidence = 0.8
                    elif risk_score > 0.3:
                        decision = 'needs_review'
                        confidence = 0.6
                    else:
                        decision = 'approved'
                        confidence = 0.9

                    # Update scores
                    scores.at[idx, 'risk_score'] = round(risk_score, 3)
                    scores.at[idx, 'sentiment_score'] = round(min(max(sentiment, 0.0), 1.0), 3)
                    scores.at[idx, 'moderation_decision'] = decision
                    scores.at[idx, 'confidence'] = confidence

            # Add timestamp
            scores['scored_at'] = pd.Timestamp.now()

            # Calculate summary statistics
            flagged_count = (scores['moderation_decision'] == 'flagged').sum()
            needs_review_count = (scores['moderation_decision'] == 'needs_review').sum()
            approved_count = (scores['moderation_decision'] == 'approved').sum()

            context.log.info(
                f"Moderation complete: {approved_count} approved, "
                f"{needs_review_count} need review, {flagged_count} flagged"
            )

            return Output(
                value=scores,
                metadata={
                    "total_scored": len(scores),
                    "flagged": int(flagged_count),
                    "needs_review": int(needs_review_count),
                    "approved": int(approved_count),
                    "avg_risk_score": float(scores['risk_score'].mean()),
                    "preview": MetadataValue.md(scores.head(10).to_markdown())
                }
            )

        return Definitions(assets=[moderation_scorer_asset])
