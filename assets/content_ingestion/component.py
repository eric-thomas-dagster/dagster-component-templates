"""Content Ingestion Component.

Ingest user-generated content for moderation systems.
Supports various content sources and formats.
"""

from typing import Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
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


class ContentIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting user-generated content for moderation.

    This component ingests text content, images, videos, and other user-generated
    content that requires moderation before being published.

    Example:
        ```yaml
        type: dagster_component_templates.ContentIngestionComponent
        attributes:
          asset_name: user_content
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        description = self.description or "User-generated content for moderation"

        @asset(
            name=asset_name,
            description=description,
            group_name="content_moderation",
        )
        def content_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests user-generated content."""
            import os
            from datetime import datetime, timedelta
            import random

            context.log.info("Ingesting user-generated content for moderation")

            # Check for content source configuration
            content_source = os.getenv("CONTENT_SOURCE", "sample")

            if content_source == "sample":
                context.log.info("Using sample content data")

                # Generate realistic sample content
                content_types = ['text_post', 'comment', 'review', 'message']
                statuses = ['pending_review', 'pending_review', 'pending_review', 'approved']

                sample_texts = [
                    "Great product! Really enjoying it.",
                    "This is an amazing service, highly recommend!",
                    "Not sure about this, seems okay.",
                    "Terrible experience, very disappointed.",
                    "Check out this awesome deal!",
                    "Can someone help me with this issue?",
                    "Love it! Best purchase ever.",
                    "Quality is not what I expected.",
                    "Fast shipping, great packaging.",
                    "Would buy again, very satisfied."
                ]

                num_items = 50
                now = datetime.now()

                df = pd.DataFrame({
                    'content_id': range(1, num_items + 1),
                    'user_id': [f'user_{random.randint(1, 20)}' for _ in range(num_items)],
                    'content_type': [random.choice(content_types) for _ in range(num_items)],
                    'content_text': [random.choice(sample_texts) for _ in range(num_items)],
                    'created_at': [now - timedelta(hours=random.randint(0, 72)) for _ in range(num_items)],
                    'status': [random.choice(statuses) for _ in range(num_items)],
                    'language': ['en'] * num_items,
                    'platform': ['web'] * num_items,
                })

                context.log.info(f"Generated {len(df)} sample content items")

                return Output(
                    value=df,
                    metadata={
                        "row_count": len(df),
                        "content_types": df['content_type'].value_counts().to_dict(),
                        "pending_review": int((df['status'] == 'pending_review').sum()),
                        "preview": MetadataValue.md(df.head(10).to_markdown())
                    }
                )

            else:
                # For other sources, return placeholder
                context.log.warning(f"Content source '{content_source}' not yet implemented")
                return pd.DataFrame({
                    'content_id': [1],
                    'content_text': ['Sample content'],
                    'status': ['pending_review']
                })

        return Definitions(assets=[content_ingestion_asset])
