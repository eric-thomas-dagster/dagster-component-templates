"""Content Ingestion Component.

Ingest user-generated content for moderation systems.
Supports various content sources and formats.
"""

from typing import Optional
import pandas as pd
from dagster import (
    AssetExecutionContext,
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


    retry_policy_max_retries: Optional[int] = Field(


        default=None,


        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",


    )


    retry_policy_delay_seconds: Optional[int] = Field(


        default=None,


        description="Seconds between retries (default 1).",


    )


    retry_policy_backoff: str = Field(


        default="exponential",


        description="Backoff strategy: 'linear' or 'exponential'.",


    )



    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        description = self.description or "User-generated content for moderation"

        # Build partition definition (auto-generated; supports daily, weekly,

        # monthly, hourly partitions out of the box).

        partitions_def = None

        if self.partition_type:

            from dagster import (

                DailyPartitionsDefinition, WeeklyPartitionsDefinition,

                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,

            )

            _pstart = self.partition_start or "2024-01-01"

            if self.partition_type == "daily":

                partitions_def = DailyPartitionsDefinition(start_date=_pstart)

            elif self.partition_type == "weekly":

                partitions_def = WeeklyPartitionsDefinition(start_date=_pstart)

            elif self.partition_type == "monthly":

                partitions_def = MonthlyPartitionsDefinition(start_date=_pstart)

            elif self.partition_type == "hourly":

                partitions_def = HourlyPartitionsDefinition(start_date=_pstart)


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=asset_name,
            description=description,
            group_name="content_moderation",
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
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
