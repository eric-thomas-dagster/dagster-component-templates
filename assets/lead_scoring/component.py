"""Lead Scoring Component.

Scores and qualifies leads based on firmographic fit and behavioral intent to
prioritize sales efforts and optimize marketing-to-sales handoff.
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


@component_type(name="lead_scoring")
class LeadScoringComponent(Component):
    """Component that scores and qualifies leads for sales prioritization."""

    asset_name: str = Field(
        ...,
        description="Name of the lead scoring asset to create",
    )

    # Input asset references (set via lineage)
    lead_data_asset: Optional[str] = Field(
        default="",
        description="Lead/contact data from CRM",
    )

    behavioral_data_asset: Optional[str] = Field(
        default="",
        description="Behavioral/activity data (web visits, email opens, etc.)",
    )

    company_data_asset: Optional[str] = Field(
        default="",
        description="Company/firmographic data for B2B scoring",
    )

    # Scoring model
    scoring_model: str = Field(
        default="combined",
        description="Scoring model: fit_only, intent_only, or combined",
    )

    fit_weight: float = Field(
        default=0.4,
        description="Weight for fit score in combined model (0-1)",
    )

    intent_weight: float = Field(
        default=0.6,
        description="Weight for intent score in combined model (0-1)",
    )

    # Fit scoring (demographic/firmographic)
    company_size_weight: float = Field(
        default=0.25,
        description="Weight for company size in fit score",
    )

    industry_weight: float = Field(
        default=0.25,
        description="Weight for industry match in fit score",
    )

    job_title_weight: float = Field(
        default=0.25,
        description="Weight for job title relevance in fit score",
    )

    geography_weight: float = Field(
        default=0.15,
        description="Weight for geography match in fit score",
    )

    budget_weight: float = Field(
        default=0.10,
        description="Weight for budget indicators in fit score",
    )

    # Intent scoring (behavioral)
    email_engagement_weight: float = Field(
        default=0.25,
        description="Weight for email engagement in intent score",
    )

    website_activity_weight: float = Field(
        default=0.30,
        description="Weight for website visits in intent score",
    )

    content_consumption_weight: float = Field(
        default=0.20,
        description="Weight for content downloads in intent score",
    )

    product_interest_weight: float = Field(
        default=0.25,
        description="Weight for product page views in intent score",
    )

    # Qualification thresholds
    mql_threshold: float = Field(
        default=50.0,
        description="Score threshold for Marketing Qualified Lead",
    )

    sql_threshold: float = Field(
        default=70.0,
        description="Score threshold for Sales Qualified Lead",
    )

    hot_lead_threshold: float = Field(
        default=75.0,
        description="Score threshold for hot lead classification",
    )

    warm_lead_threshold: float = Field(
        default=50.0,
        description="Score threshold for warm lead classification",
    )

    # Output options
    include_score_breakdown: bool = Field(
        default=True,
        description="Include fit and intent score breakdown",
    )

    calculate_lead_grade: bool = Field(
        default=True,
        description="Calculate letter grade (A-F) in addition to score",
    )

    # Time decay
    apply_time_decay: bool = Field(
        default=True,
        description="Apply time decay to behavioral signals",
    )

    time_decay_days: int = Field(
        default=30,
        description="Number of days for behavioral signals to decay to 50%",
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
            return 0.0  # Missing demographic data = 0 fit

        if max_val == min_val:
            return 50.0

        normalized = ((value - min_val) / (max_val - min_val)) * 100
        return np.clip(normalized, 0, 100)

    def _calculate_fit_score(self, lead_data: pd.DataFrame, company_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate fit score from demographic/firmographic data."""
        scores = pd.DataFrame()

        if lead_data is None or lead_data.empty:
            return scores

        scores['lead_id'] = lead_data.get('lead_id', lead_data.get('id', lead_data.get('contact_id')))
        fit_score = pd.Series(0.0, index=lead_data.index)

        # Company size scoring
        if 'company_size' in lead_data.columns or 'employee_count' in lead_data.columns:
            size_col = 'company_size' if 'company_size' in lead_data.columns else 'employee_count'
            company_sizes = lead_data[size_col].fillna(0)

            # Score based on ideal company size (typically 50-500 employees for mid-market)
            size_scores = pd.Series(0.0, index=lead_data.index)
            size_scores[company_sizes.between(50, 500)] = 100
            size_scores[company_sizes.between(20, 50)] = 75
            size_scores[company_sizes.between(500, 1000)] = 75
            size_scores[company_sizes.between(10, 20)] = 50
            size_scores[company_sizes > 1000] = 80  # Enterprise
            size_scores[company_sizes < 10] = 25

            fit_score += size_scores * self.company_size_weight

        # Industry match scoring
        if 'industry' in lead_data.columns:
            # Define target industries (configurable in real implementation)
            target_industries = [
                'software', 'technology', 'saas', 'information technology',
                'computer software', 'internet', 'financial services'
            ]

            industry_scores = lead_data['industry'].apply(
                lambda x: 100 if pd.notna(x) and any(target in str(x).lower() for target in target_industries)
                else 30
            )
            fit_score += industry_scores * self.industry_weight

        # Job title relevance scoring
        if 'job_title' in lead_data.columns:
            # Define decision-maker titles
            c_level = ['ceo', 'cto', 'cfo', 'coo', 'cmo', 'chief']
            vp_level = ['vp', 'vice president', 'head of', 'director']
            manager_level = ['manager', 'lead', 'senior']

            def score_title(title):
                if pd.isna(title):
                    return 0
                title_lower = str(title).lower()
                if any(t in title_lower for t in c_level):
                    return 100
                elif any(t in title_lower for t in vp_level):
                    return 85
                elif any(t in title_lower for t in manager_level):
                    return 60
                else:
                    return 30

            title_scores = lead_data['job_title'].apply(score_title)
            fit_score += title_scores * self.job_title_weight

        # Geography match scoring
        if 'country' in lead_data.columns or 'region' in lead_data.columns:
            geo_col = 'country' if 'country' in lead_data.columns else 'region'
            target_countries = ['united states', 'usa', 'us', 'canada', 'united kingdom', 'uk']

            geo_scores = lead_data[geo_col].apply(
                lambda x: 100 if pd.notna(x) and any(country in str(x).lower() for country in target_countries)
                else 50
            )
            fit_score += geo_scores * self.geography_weight

        # Budget indicators
        if 'annual_revenue' in lead_data.columns:
            revenue = lead_data['annual_revenue'].fillna(0)
            # Higher revenue = higher budget likelihood
            revenue_scores = pd.Series(0.0, index=lead_data.index)
            revenue_scores[revenue > 10_000_000] = 100  # $10M+
            revenue_scores[revenue.between(1_000_000, 10_000_000)] = 80  # $1-10M
            revenue_scores[revenue.between(100_000, 1_000_000)] = 60  # $100K-1M
            revenue_scores[revenue < 100_000] = 30

            fit_score += revenue_scores * self.budget_weight

        scores['fit_score'] = fit_score.clip(0, 100)
        return scores

    def _calculate_intent_score(self, behavioral_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate intent score from behavioral data."""
        scores = pd.DataFrame()

        if behavioral_data is None or behavioral_data.empty:
            return scores

        scores['lead_id'] = behavioral_data.get('lead_id', behavioral_data.get('user_id', behavioral_data.get('contact_id')))
        intent_score = pd.Series(0.0, index=behavioral_data.index)

        # Email engagement
        if 'email_opens' in behavioral_data.columns or 'email_clicks' in behavioral_data.columns:
            email_score = pd.Series(0.0, index=behavioral_data.index)

            if 'email_opens' in behavioral_data.columns:
                opens = behavioral_data['email_opens'].fillna(0)
                email_score += self._normalize_score(opens, 0, 10) * 0.4

            if 'email_clicks' in behavioral_data.columns:
                clicks = behavioral_data['email_clicks'].fillna(0)
                email_score += self._normalize_score(clicks, 0, 5) * 0.6

            intent_score += email_score * self.email_engagement_weight

        # Website activity
        if 'page_views' in behavioral_data.columns or 'session_count' in behavioral_data.columns:
            web_score = pd.Series(0.0, index=behavioral_data.index)

            if 'page_views' in behavioral_data.columns:
                views = behavioral_data['page_views'].fillna(0)
                web_score += self._normalize_score(views, 0, 20) * 0.5

            if 'session_count' in behavioral_data.columns:
                sessions = behavioral_data['session_count'].fillna(0)
                web_score += self._normalize_score(sessions, 0, 10) * 0.5

            intent_score += web_score * self.website_activity_weight

        # Content consumption
        if 'content_downloads' in behavioral_data.columns or 'whitepaper_downloads' in behavioral_data.columns:
            content_score = pd.Series(0.0, index=behavioral_data.index)

            if 'content_downloads' in behavioral_data.columns:
                downloads = behavioral_data['content_downloads'].fillna(0)
                content_score += self._normalize_score(downloads, 0, 5)

            if 'whitepaper_downloads' in behavioral_data.columns:
                whitepapers = behavioral_data['whitepaper_downloads'].fillna(0)
                content_score += self._normalize_score(whitepapers, 0, 3)

            intent_score += content_score * self.content_consumption_weight

        # Product interest (high-intent pages)
        if 'pricing_page_views' in behavioral_data.columns or 'demo_requests' in behavioral_data.columns:
            product_score = pd.Series(0.0, index=behavioral_data.index)

            if 'pricing_page_views' in behavioral_data.columns:
                pricing_views = behavioral_data['pricing_page_views'].fillna(0)
                product_score += self._normalize_score(pricing_views, 0, 5) * 0.4

            if 'demo_requests' in behavioral_data.columns:
                demo_requests = behavioral_data['demo_requests'].fillna(0)
                # Demo request is very high intent
                product_score += behavioral_data['demo_requests'].apply(lambda x: 100 if x > 0 else 0) * 0.6

            intent_score += product_score * self.product_interest_weight

        # Apply time decay if enabled
        if self.apply_time_decay and 'last_activity_date' in behavioral_data.columns:
            days_since_activity = (pd.Timestamp.now() - pd.to_datetime(behavioral_data['last_activity_date'])).dt.days
            # Exponential decay: score * 0.5^(days / decay_period)
            decay_factor = 0.5 ** (days_since_activity / self.time_decay_days)
            intent_score = intent_score * decay_factor

        scores['intent_score'] = intent_score.clip(0, 100)
        return scores

    def _combine_scores(self, fit_scores: pd.DataFrame, intent_scores: pd.DataFrame) -> pd.DataFrame:
        """Combine fit and intent scores based on selected model."""
        # Get all unique lead IDs
        all_leads = set()
        if not fit_scores.empty and 'lead_id' in fit_scores.columns:
            all_leads.update(fit_scores['lead_id'].unique())
        if not intent_scores.empty and 'lead_id' in intent_scores.columns:
            all_leads.update(intent_scores['lead_id'].unique())

        if not all_leads:
            return pd.DataFrame()

        result = pd.DataFrame({'lead_id': list(all_leads)})

        # Merge scores
        if not fit_scores.empty:
            result = result.merge(fit_scores[['lead_id', 'fit_score']], on='lead_id', how='left')
        else:
            result['fit_score'] = 0.0

        if not intent_scores.empty:
            result = result.merge(intent_scores[['lead_id', 'intent_score']], on='lead_id', how='left')
        else:
            result['intent_score'] = 0.0

        # Fill missing scores
        result['fit_score'] = result['fit_score'].fillna(0.0)
        result['intent_score'] = result['intent_score'].fillna(0.0)

        # Calculate overall score based on model
        if self.scoring_model == 'fit_only':
            result['lead_score'] = result['fit_score']
        elif self.scoring_model == 'intent_only':
            result['lead_score'] = result['intent_score']
        else:  # combined
            result['lead_score'] = (
                result['fit_score'] * self.fit_weight +
                result['intent_score'] * self.intent_weight
            )

        result['lead_score'] = result['lead_score'].clip(0, 100)

        # Classify leads
        result['lead_temperature'] = pd.cut(
            result['lead_score'],
            bins=[0, self.warm_lead_threshold, self.hot_lead_threshold, 100],
            labels=['cold', 'warm', 'hot'],
            include_lowest=True
        )

        # Qualification flags
        result['is_mql'] = result['lead_score'] >= self.mql_threshold
        result['is_sql'] = result['lead_score'] >= self.sql_threshold

        # Lead grade (A-F)
        if self.calculate_lead_grade:
            result['lead_grade'] = pd.cut(
                result['lead_score'],
                bins=[0, 20, 40, 60, 80, 100],
                labels=['F', 'D', 'C', 'B', 'A'],
                include_lowest=True
            )

        # Optionally remove score breakdown
        if not self.include_score_breakdown:
            result = result.drop(columns=['fit_score', 'intent_score'], errors='ignore')

        # Add timestamp
        result['scored_at'] = pd.Timestamp.now()

        return result

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build asset definitions."""
        asset_name = self.asset_name

        # Determine which inputs are available
        asset_ins = {}

        if self.lead_data_asset:
            asset_ins["lead_data"] = AssetIn(key=AssetKey.from_user_string(self.lead_data_asset))

        if self.behavioral_data_asset:
            asset_ins["behavioral_data"] = AssetIn(key=AssetKey.from_user_string(self.behavioral_data_asset))

        if self.company_data_asset:
            asset_ins["company_data"] = AssetIn(key=AssetKey.from_user_string(self.company_data_asset))

        # Require at least one input
        if not asset_ins:
            raise ValueError(
                "At least one input asset must be connected. "
                "Use visual lineage to connect lead data, behavioral data, or company data."
            )

        component = self

        @asset(
            name=asset_name,
            ins=asset_ins,
            description=self.description or "Lead scores with qualification flags (MQL/SQL) and temperature classification",
            group_name=self.group_name,
        )
        def lead_scoring_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
            """Score and qualify leads based on fit and intent."""

            context.log.info(f"Scoring leads using {component.scoring_model} model...")

            # Get inputs
            lead_data = inputs.get('lead_data')
            behavioral_data = inputs.get('behavioral_data')
            company_data = inputs.get('company_data')

            # Calculate component scores
            context.log.info("Calculating fit score from demographic data...")
            fit_scores = component._calculate_fit_score(lead_data, company_data)

            context.log.info("Calculating intent score from behavioral data...")
            intent_scores = component._calculate_intent_score(behavioral_data)

            # Combine scores
            context.log.info(f"Combining scores with model: {component.scoring_model}")
            lead_scores = component._combine_scores(fit_scores, intent_scores)

            if lead_scores.empty:
                context.log.warning("No lead scores calculated")
                return pd.DataFrame()

            # Summary statistics
            total_leads = len(lead_scores)
            avg_score = lead_scores['lead_score'].mean()
            mql_count = lead_scores['is_mql'].sum()
            sql_count = lead_scores['is_sql'].sum()
            hot_count = (lead_scores['lead_temperature'] == 'hot').sum()

            context.log.info(f"✓ Scored {total_leads} leads")
            context.log.info(f"  Average score: {avg_score:.1f}")
            context.log.info(f"  MQLs: {mql_count}")
            context.log.info(f"  SQLs: {sql_count}")
            context.log.info(f"  Hot leads: {hot_count}")

            # Temperature breakdown
            temp_counts = lead_scores['lead_temperature'].value_counts()
            context.log.info(f"  Temperature: {temp_counts.to_dict()}")

            return lead_scores

        return build_defs_from_component(
            context=context,
            component=self,
            asset_defs=[lead_scoring_asset],
        )
