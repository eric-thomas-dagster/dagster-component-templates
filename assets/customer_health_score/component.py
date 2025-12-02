"""Customer Health Score Component.

Calculates customer health scores by analyzing engagement, product usage,
subscription status, and support interactions to predict churn risk and
identify expansion opportunities.
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


@component_type(name="customer_health_score")
class CustomerHealthScoreComponent(Component):
    """Component that calculates customer health scores from multiple data sources."""

    asset_name: str = Field(
        ...,
        description="Name of the customer health score asset to create",
    )

    # Input asset references (set via lineage)
    customer_data_asset: Optional[str] = Field(
        default="",
        description="Customer data asset (CRM, user profiles, etc.)",
    )

    subscription_data_asset: Optional[str] = Field(
        default="",
        description="Subscription/billing data asset",
    )

    product_usage_asset: Optional[str] = Field(
        default="",
        description="Product usage/activity data asset",
    )

    support_ticket_asset: Optional[str] = Field(
        default="",
        description="Support ticket data asset",
    )

    # Configuration
    analysis_period_days: int = Field(
        default=30,
        description="Number of days to analyze for health calculation",
    )

    engagement_weight: float = Field(
        default=0.25,
        description="Weight for engagement metrics (0-1)",
    )

    product_usage_weight: float = Field(
        default=0.25,
        description="Weight for product usage metrics (0-1)",
    )

    payment_health_weight: float = Field(
        default=0.25,
        description="Weight for payment/subscription health (0-1)",
    )

    support_health_weight: float = Field(
        default=0.25,
        description="Weight for support interaction health (0-1)",
    )

    min_health_score: float = Field(
        default=0.0,
        description="Minimum health score (0-100)",
    )

    max_health_score: float = Field(
        default=100.0,
        description="Maximum health score (0-100)",
    )

    churn_risk_threshold: float = Field(
        default=40.0,
        description="Health score below this is considered churn risk",
    )

    expansion_opportunity_threshold: float = Field(
        default=75.0,
        description="Health score above this is considered expansion opportunity",
    )

    include_factor_breakdown: bool = Field(
        default=True,
        description="Include breakdown of contributing factors in output",
    )

    calculate_trend: bool = Field(
        default=True,
        description="Calculate health score trend (requires historical data)",
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

    def _normalize_score(self, value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
        """Normalize a value to 0-100 scale."""
        if pd.isna(value):
            return 50.0  # Neutral score for missing data

        if max_val == min_val:
            return 50.0

        normalized = ((value - min_val) / (max_val - min_val)) * 100
        return np.clip(normalized, self.min_health_score, self.max_health_score)

    def _calculate_engagement_score(self, customer_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate engagement score from customer data."""
        scores = pd.DataFrame()

        if customer_data is None or customer_data.empty:
            return scores

        # Look for common engagement indicators
        engagement_indicators = {
            'last_login_days': {'invert': True, 'weight': 0.4},  # Lower is better
            'login_frequency': {'invert': False, 'weight': 0.3},
            'feature_adoption_rate': {'invert': False, 'weight': 0.3},
            'email_open_rate': {'invert': False, 'weight': 0.2},
            'days_since_last_activity': {'invert': True, 'weight': 0.4},
            'active_days': {'invert': False, 'weight': 0.3},
        }

        scores['customer_id'] = customer_data.get('customer_id', customer_data.get('id'))
        engagement_score = pd.Series(50.0, index=customer_data.index)  # Start neutral

        for indicator, config in engagement_indicators.items():
            if indicator in customer_data.columns:
                values = customer_data[indicator]

                if config['invert']:
                    # Lower values = higher score (e.g., days since login)
                    normalized = 100 - self._normalize_score(
                        values,
                        values.min(),
                        values.max()
                    )
                else:
                    # Higher values = higher score (e.g., login frequency)
                    normalized = self._normalize_score(
                        values,
                        values.min(),
                        values.max()
                    )

                engagement_score += normalized * config['weight']

        scores['engagement_score'] = np.clip(engagement_score, 0, 100)
        return scores

    def _calculate_product_usage_score(self, usage_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate product usage score."""
        scores = pd.DataFrame()

        if usage_data is None or usage_data.empty:
            return scores

        scores['customer_id'] = usage_data.get('customer_id', usage_data.get('user_id'))

        # Look for usage indicators
        usage_indicators = {
            'daily_active_days': {'weight': 0.3},
            'feature_usage_count': {'weight': 0.3},
            'core_feature_usage': {'weight': 0.4},
            'session_count': {'weight': 0.2},
            'session_duration_avg': {'weight': 0.2},
            'actions_per_session': {'weight': 0.2},
        }

        usage_score = pd.Series(50.0, index=usage_data.index)

        for indicator, config in usage_indicators.items():
            if indicator in usage_data.columns:
                values = usage_data[indicator]
                normalized = self._normalize_score(values, values.min(), values.max())
                usage_score += normalized * config['weight']

        scores['product_usage_score'] = np.clip(usage_score, 0, 100)
        return scores

    def _calculate_payment_health_score(self, subscription_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate payment/subscription health score."""
        scores = pd.DataFrame()

        if subscription_data is None or subscription_data.empty:
            return scores

        scores['customer_id'] = subscription_data.get('customer_id', subscription_data.get('id'))
        payment_score = pd.Series(50.0, index=subscription_data.index)

        # Subscription status
        if 'status' in subscription_data.columns:
            status_scores = {
                'active': 100,
                'trialing': 80,
                'past_due': 30,
                'canceled': 0,
                'unpaid': 20,
            }
            payment_score += subscription_data['status'].map(
                lambda s: status_scores.get(str(s).lower(), 50)
            ) * 0.4

        # Payment failures
        if 'payment_failures' in subscription_data.columns:
            failure_penalty = subscription_data['payment_failures'].clip(0, 5) * 10
            payment_score -= failure_penalty * 0.2

        # Subscription tenure (longer = more stable)
        if 'days_subscribed' in subscription_data.columns:
            tenure_score = self._normalize_score(
                subscription_data['days_subscribed'],
                0,
                365  # Cap at 1 year
            )
            payment_score += tenure_score * 0.3

        # Plan value (higher tier = higher score)
        if 'mrr' in subscription_data.columns or 'plan_amount' in subscription_data.columns:
            amount_col = 'mrr' if 'mrr' in subscription_data.columns else 'plan_amount'
            amount_score = self._normalize_score(
                subscription_data[amount_col],
                subscription_data[amount_col].min(),
                subscription_data[amount_col].max()
            )
            payment_score += amount_score * 0.1

        scores['payment_health_score'] = np.clip(payment_score, 0, 100)
        return scores

    def _calculate_support_health_score(self, support_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate support interaction health score."""
        scores = pd.DataFrame()

        if support_data is None or support_data.empty:
            return scores

        scores['customer_id'] = support_data.get('customer_id', support_data.get('user_id'))
        support_score = pd.Series(50.0, index=support_data.index)

        # Ticket volume (moderate is good, too many or too few is bad)
        if 'ticket_count' in support_data.columns:
            # Optimal range is 1-3 tickets per period
            ticket_count = support_data['ticket_count']
            ticket_score = pd.Series(50.0, index=support_data.index)

            # 0 tickets = 70 (neutral to slightly good)
            ticket_score[ticket_count == 0] = 70
            # 1-3 tickets = 80 (healthy engagement)
            ticket_score[ticket_count.between(1, 3)] = 80
            # 4-6 tickets = 60 (slightly concerning)
            ticket_score[ticket_count.between(4, 6)] = 60
            # 7+ tickets = 30 (high risk)
            ticket_score[ticket_count > 6] = 30

            support_score += ticket_score * 0.4

        # Critical issues
        if 'critical_issues' in support_data.columns:
            critical_penalty = support_data['critical_issues'].clip(0, 3) * 15
            support_score -= critical_penalty * 0.3

        # CSAT (Customer Satisfaction Score)
        if 'csat_score' in support_data.columns:
            # CSAT is usually 1-5, normalize to 0-100
            csat_normalized = (support_data['csat_score'] - 1) / 4 * 100
            support_score += csat_normalized * 0.3

        # Open ticket age (older open tickets = lower score)
        if 'avg_open_ticket_age_days' in support_data.columns:
            age_penalty = support_data['avg_open_ticket_age_days'].clip(0, 30)
            age_score = 100 - (age_penalty / 30 * 100)
            support_score += age_score * 0.2

        scores['support_health_score'] = np.clip(support_score, 0, 100)
        return scores

    def _combine_scores(
        self,
        engagement: pd.DataFrame,
        usage: pd.DataFrame,
        payment: pd.DataFrame,
        support: pd.DataFrame,
    ) -> pd.DataFrame:
        """Combine all component scores into overall health score."""

        # Start with all unique customer IDs
        all_customers = set()
        for df in [engagement, usage, payment, support]:
            if not df.empty and 'customer_id' in df.columns:
                all_customers.update(df['customer_id'].unique())

        if not all_customers:
            return pd.DataFrame()

        result = pd.DataFrame({'customer_id': list(all_customers)})

        # Merge all scores
        for df, score_col in [
            (engagement, 'engagement_score'),
            (usage, 'product_usage_score'),
            (payment, 'payment_health_score'),
            (support, 'support_health_score'),
        ]:
            if not df.empty and score_col in df.columns:
                result = result.merge(
                    df[['customer_id', score_col]],
                    on='customer_id',
                    how='left'
                )

        # Fill missing scores with neutral value
        score_columns = [
            'engagement_score',
            'product_usage_score',
            'payment_health_score',
            'support_health_score',
        ]

        for col in score_columns:
            if col not in result.columns:
                result[col] = 50.0
            else:
                result[col] = result[col].fillna(50.0)

        # Calculate weighted overall score
        result['health_score'] = (
            result['engagement_score'] * self.engagement_weight +
            result['product_usage_score'] * self.product_usage_weight +
            result['payment_health_score'] * self.payment_health_weight +
            result['support_health_score'] * self.support_health_weight
        )

        # Normalize to configured range
        result['health_score'] = result['health_score'].clip(
            self.min_health_score,
            self.max_health_score
        )

        # Add risk categories
        result['risk_category'] = pd.cut(
            result['health_score'],
            bins=[0, self.churn_risk_threshold, self.expansion_opportunity_threshold, 100],
            labels=['high_risk', 'moderate', 'healthy'],
            include_lowest=True
        )

        # Add flags
        result['is_churn_risk'] = result['health_score'] < self.churn_risk_threshold
        result['is_expansion_opportunity'] = result['health_score'] > self.expansion_opportunity_threshold

        # Optionally remove factor breakdown
        if not self.include_factor_breakdown:
            result = result[[
                'customer_id',
                'health_score',
                'risk_category',
                'is_churn_risk',
                'is_expansion_opportunity',
            ]]

        # Add timestamp
        result['calculated_at'] = pd.Timestamp.now()

        return result

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build asset definitions."""
        asset_name = self.asset_name

        # Determine which inputs are available
        asset_ins = {}

        if self.customer_data_asset:
            asset_ins["customer_data"] = AssetIn(key=AssetKey.from_user_string(self.customer_data_asset))

        if self.subscription_data_asset:
            asset_ins["subscription_data"] = AssetIn(key=AssetKey.from_user_string(self.subscription_data_asset))

        if self.product_usage_asset:
            asset_ins["product_usage"] = AssetIn(key=AssetKey.from_user_string(self.product_usage_asset))

        if self.support_ticket_asset:
            asset_ins["support_tickets"] = AssetIn(key=AssetKey.from_user_string(self.support_ticket_asset))

        # Require at least one input
        if not asset_ins:
            raise ValueError(
                "At least one input asset must be connected. "
                "Use visual lineage to connect customer data, subscription data, "
                "product usage data, or support ticket data."
            )

        component = self

        @asset(
            name=asset_name,
            ins=asset_ins,
            description=self.description or "Customer health scores with churn risk and expansion opportunity flags",
            group_name=self.group_name,
        )
        def customer_health_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
            """Calculate customer health scores from multiple data sources."""

            context.log.info(f"Calculating customer health scores for {len(inputs)} data sources...")

            # Get inputs (may be None if not connected)
            customer_data = inputs.get('customer_data')
            subscription_data = inputs.get('subscription_data')
            product_usage = inputs.get('product_usage')
            support_tickets = inputs.get('support_tickets')

            # Calculate component scores
            context.log.info("Calculating engagement score...")
            engagement_scores = component._calculate_engagement_score(customer_data)

            context.log.info("Calculating product usage score...")
            usage_scores = component._calculate_product_usage_score(product_usage)

            context.log.info("Calculating payment health score...")
            payment_scores = component._calculate_payment_health_score(subscription_data)

            context.log.info("Calculating support health score...")
            support_scores = component._calculate_support_health_score(support_tickets)

            # Combine into overall health score
            context.log.info("Combining scores...")
            health_scores = component._combine_scores(
                engagement_scores,
                usage_scores,
                payment_scores,
                support_scores
            )

            if health_scores.empty:
                context.log.warning("No customer health scores calculated")
                return pd.DataFrame()

            # Summary statistics
            total_customers = len(health_scores)
            avg_score = health_scores['health_score'].mean()
            churn_risk_count = health_scores['is_churn_risk'].sum()
            expansion_count = health_scores['is_expansion_opportunity'].sum()

            context.log.info(f"✓ Calculated health scores for {total_customers} customers")
            context.log.info(f"  Average health score: {avg_score:.1f}")
            context.log.info(f"  Churn risk: {churn_risk_count} customers")
            context.log.info(f"  Expansion opportunities: {expansion_count} customers")

            return health_scores

        return build_defs_from_component(
            context=context,
            component=self,
            asset_defs=[customer_health_asset],
        )
