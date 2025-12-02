"""Customer Segmentation Component.

Segments customers using RFM (Recency, Frequency, Monetary) analysis to identify
Champions, Loyal Customers, At-Risk customers, and other actionable segments.
"""

from typing import Any, Optional

import pandas as pd
import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    OpExecutionContext,
    asset,
)
from dagster._core.definitions.definitions_class import Definitions
from dagster_components import Component, ComponentLoadContext, component_type
from dagster_components.core.component_defs_builder import build_defs_from_component
from pydantic import Field


@component_type(name="customer_segmentation")
class CustomerSegmentationComponent(Component):
    """Component that segments customers using RFM analysis."""

    asset_name: str = Field(
        ...,
        description="Name of the customer segmentation asset to create",
    )

    # Input asset references (set via lineage)
    transaction_data_asset: Optional[str] = Field(
        default="",
        description="Transaction/order data with customer_id, date, and amount",
    )

    customer_data_asset: Optional[str] = Field(
        default="",
        description="Customer data for additional attributes (optional)",
    )

    # RFM Configuration
    recency_weight: float = Field(
        default=1.0,
        description="Weight for recency in RFM score",
    )

    frequency_weight: float = Field(
        default=1.0,
        description="Weight for frequency in RFM score",
    )

    monetary_weight: float = Field(
        default=1.0,
        description="Weight for monetary value in RFM score",
    )

    # Scoring method
    scoring_method: str = Field(
        default="quintiles",
        description="Scoring method: quintiles, quartiles, or custom",
    )

    # Analysis period
    analysis_period_days: int = Field(
        default=365,
        description="Number of days to analyze for RFM calculation",
    )

    # Segment definitions
    use_predefined_segments: bool = Field(
        default=True,
        description="Use predefined RFM segments (Champions, Loyal, At Risk, etc.)",
    )

    # Output options
    include_recommendations: bool = Field(
        default=True,
        description="Include action recommendations for each segment",
    )

    calculate_segment_value: bool = Field(
        default=True,
        description="Calculate total and average value per segment",
    )

    # Asset properties
    description: str = Field(
        default="",
        description="Asset description",
    )

    group_name: str = Field(
        default="analytics",
        description="Asset group name",
    )

    def _calculate_rfm_scores(self, transaction_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate RFM scores for each customer."""
        if transaction_data is None or transaction_data.empty:
            return pd.DataFrame()

        # Standardize column names
        df = transaction_data.copy()

        # Map common column variations
        if 'customer_id' not in df.columns:
            for col in ['user_id', 'id', 'customerid']:
                if col in df.columns:
                    df['customer_id'] = df[col]
                    break

        if 'date' not in df.columns:
            for col in ['order_date', 'transaction_date', 'created_at']:
                if col in df.columns:
                    df['date'] = df[col]
                    break

        if 'amount' not in df.columns:
            for col in ['total', 'revenue', 'value', 'order_total']:
                if col in df.columns:
                    df['amount'] = df[col]
                    break

        # Validate required columns
        if 'customer_id' not in df.columns:
            raise ValueError("Transaction data must have customer_id column")
        if 'date' not in df.columns:
            raise ValueError("Transaction data must have date column")
        if 'amount' not in df.columns:
            raise ValueError("Transaction data must have amount column")

        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'])

        # Filter to analysis period
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=self.analysis_period_days)
        df = df[df['date'] >= cutoff_date]

        if df.empty:
            return pd.DataFrame()

        # Calculate RFM metrics
        current_date = df['date'].max() + pd.Timedelta(days=1)

        rfm = df.groupby('customer_id').agg({
            'date': lambda x: (current_date - x.max()).days,  # Recency
            'customer_id': 'count',  # Frequency
            'amount': 'sum'  # Monetary
        }).rename(columns={
            'date': 'recency',
            'customer_id': 'frequency',
            'amount': 'monetary'
        })

        # Calculate RFM scores (1-5, where 5 is best)
        if self.scoring_method == 'quintiles':
            rfm['R_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1], duplicates='drop')
            rfm['F_score'] = pd.qcut(rfm['frequency'], 5, labels=[1,2,3,4,5], duplicates='drop')
            rfm['M_score'] = pd.qcut(rfm['monetary'], 5, labels=[1,2,3,4,5], duplicates='drop')
        elif self.scoring_method == 'quartiles':
            rfm['R_score'] = pd.qcut(rfm['recency'], 4, labels=[4,3,2,1], duplicates='drop')
            rfm['F_score'] = pd.qcut(rfm['frequency'], 4, labels=[1,2,3,4], duplicates='drop')
            rfm['M_score'] = pd.qcut(rfm['monetary'], 4, labels=[1,2,3,4], duplicates='drop')

        # Convert to numeric
        rfm['R_score'] = pd.to_numeric(rfm['R_score'])
        rfm['F_score'] = pd.to_numeric(rfm['F_score'])
        rfm['M_score'] = pd.to_numeric(rfm['M_score'])

        # Calculate weighted RFM score
        rfm['RFM_score'] = (
            rfm['R_score'] * self.recency_weight +
            rfm['F_score'] * self.frequency_weight +
            rfm['M_score'] * self.monetary_weight
        ) / (self.recency_weight + self.frequency_weight + self.monetary_weight)

        # Reset index to make customer_id a column
        rfm = rfm.reset_index()

        return rfm

    def _assign_segments(self, rfm: pd.DataFrame) -> pd.DataFrame:
        """Assign customers to predefined RFM segments."""
        if not self.use_predefined_segments:
            return rfm

        def segment_customer(row):
            r, f, m = row['R_score'], row['F_score'], row['M_score']

            if r >= 4 and f >= 4 and m >= 4:
                return 'Champions'
            elif r >= 4 and f >= 3:
                return 'Loyal Customers'
            elif r >= 4 and f < 3:
                return 'Potential Loyalists'
            elif r >= 3 and f >= 3 and m >= 3:
                return 'Promising'
            elif r >= 3 and f < 3:
                return 'Need Attention'
            elif r < 3 and f >= 4:
                return 'At Risk'
            elif r < 3 and f >= 3:
                return 'About to Sleep'
            elif r < 2 and f < 2 and m >= 4:
                return 'Cant Lose Them'
            elif r < 2:
                return 'Hibernating'
            else:
                return 'Lost'

        rfm['segment'] = rfm.apply(segment_customer, axis=1)

        if self.include_recommendations:
            recommendations = {
                'Champions': 'Reward them. Can be early adopters. Promote brand advocates.',
                'Loyal Customers': 'Upsell higher value products. Ask for reviews. Engage them.',
                'Potential Loyalists': 'Offer membership / loyalty program. Recommend products.',
                'Promising': 'Offer free shipping, Add benefits to build long-term relationship.',
                'Need Attention': 'Make limited time offers. Recommend based on past purchases. Reactivate them.',
                'At Risk': 'Send personalized emails. Offer renewals. Provide helpful resources.',
                'About to Sleep': 'Share valuable resources. Recommend popular products. Reconnect with them.',
                'Cant Lose Them': 'Win them back via renewals or newer products. Survey them. Reach out proactively.',
                'Hibernating': 'Offer other relevant products. Use special offers to revive interest.',
                'Lost': 'Revive interest with a reach out campaign. Ignore otherwise.'
            }
            rfm['recommendation'] = rfm['segment'].map(recommendations)

        return rfm

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build asset definitions."""
        asset_name = self.asset_name

        # Require transaction data
        if not self.transaction_data_asset:
            raise ValueError("Transaction data asset is required for customer segmentation")

        asset_ins = {
            "transaction_data": AssetIn(key=AssetKey.from_user_string(self.transaction_data_asset))
        }

        # Optional customer data
        if self.customer_data_asset:
            asset_ins["customer_data"] = AssetIn(key=AssetKey.from_user_string(self.customer_data_asset))

        component = self

        @asset(
            name=asset_name,
            ins=asset_ins,
            description=self.description or "Customer segmentation using RFM analysis",
            group_name=self.group_name,
        )
        def customer_segmentation_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
            """Segment customers using RFM analysis."""

            context.log.info("Calculating RFM scores...")

            # Get transaction data
            transaction_data = inputs.get('transaction_data')

            # Calculate RFM scores
            rfm = component._calculate_rfm_scores(transaction_data)

            if rfm.empty:
                context.log.warning("No RFM scores calculated")
                return pd.DataFrame()

            context.log.info(f"Calculated RFM scores for {len(rfm)} customers")

            # Assign segments
            if component.use_predefined_segments:
                context.log.info("Assigning RFM segments...")
                rfm = component._assign_segments(rfm)

                # Log segment distribution
                segment_counts = rfm['segment'].value_counts()
                context.log.info("Segment distribution:")
                for segment, count in segment_counts.items():
                    pct = count / len(rfm) * 100
                    context.log.info(f"  {segment}: {count} ({pct:.1f}%)")

                # Calculate segment value if enabled
                if component.calculate_segment_value:
                    segment_value = rfm.groupby('segment').agg({
                        'monetary': ['sum', 'mean', 'count']
                    }).round(2)
                    context.log.info("\nSegment value:")
                    for segment in segment_value.index:
                        total = segment_value.loc[segment, ('monetary', 'sum')]
                        avg = segment_value.loc[segment, ('monetary', 'mean')]
                        context.log.info(f"  {segment}: ${total:,.0f} total, ${avg:,.0f} avg")

            # Add timestamp
            rfm['segmented_at'] = pd.Timestamp.now()

            return rfm

        return build_defs_from_component(
            context=context,
            component=self,
            asset_defs=[customer_segmentation_asset],
        )
