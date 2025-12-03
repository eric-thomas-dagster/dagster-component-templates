"""Priority Scorer Component.

Score ticket priority/urgency using LLM-based, ML-based (logistic regression, XGBoost),
or rule-based methods with SLA breach prediction and escalation triggers.
"""

import os
import json
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class PriorityScorerComponent(Component, Model, Resolvable):
    """Component for scoring ticket priority and predicting SLA breaches.

    This component calculates priority scores (0-100 or P1/P2/P3/P4) using multiple
    methods: LLM-based, ML-based (logistic regression, XGBoost), or rule-based.
    It considers ticket text, customer tier, sentiment, entities, and SLA requirements.

    Features:
    - Multiple scoring methods: LLM, ML (logistic/XGBoost), or rules
    - Priority scores: numeric (0-100) or categorical (P1/P2/P3/P4)
    - SLA breach prediction
    - Customer tier weighting
    - Escalation triggers
    - Explainability: Show factors contributing to score
    - Multi-factor scoring (text, sentiment, customer tier, entities)

    Use Cases:
    - Support ticket prioritization
    - SLA compliance monitoring
    - Escalation queue management
    - Resource allocation optimization
    - Response time prediction
    - Customer success scoring
    - Issue severity assessment

    Example:
        ```yaml
        type: dagster_component_templates.PriorityScorerComponent
        attributes:
          asset_name: prioritized_tickets
          method: rules
          input_columns: "ticket_text,customer_tier,sentiment_score"
          priority_levels: "P1,P2,P3,P4"
          sla_hours: '{"enterprise": 4, "pro": 24, "basic": 72}'
          escalation_threshold: 80
          include_explanation: true
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold prioritized ticket data"
    )

    method: str = Field(
        default="rules",
        description="Scoring method: llm, ml (logistic/XGBoost), or rules"
    )

    llm_provider: Optional[str] = Field(
        default=None,
        description="LLM provider: openai or anthropic (for method=llm)"
    )

    llm_model: Optional[str] = Field(
        default=None,
        description="LLM model name (for method=llm)"
    )

    ml_model: Optional[str] = Field(
        default=None,
        description="ML model: logistic_regression, xgboost, or random_forest (for method=ml)"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for LLM method"
    )

    input_columns: str = Field(
        default="ticket_text",
        description="Comma-separated list of columns to use for scoring"
    )

    priority_levels: str = Field(
        default="P1,P2,P3,P4",
        description="Comma-separated priority levels or 'numeric' for 0-100 scores"
    )

    sla_hours: Optional[str] = Field(
        default=None,
        description="JSON dict mapping customer tier to SLA hours: {\"enterprise\": 4, \"pro\": 24, \"basic\": 72}"
    )

    customer_tier_column: Optional[str] = Field(
        default=None,
        description="Column name for customer tier (e.g., 'customer_tier')"
    )

    sentiment_score_column: Optional[str] = Field(
        default=None,
        description="Column name for sentiment score (for weighting)"
    )

    urgency_column: Optional[str] = Field(
        default=None,
        description="Column name for urgency level (from classification)"
    )

    escalation_threshold: Optional[float] = Field(
        default=80.0,
        description="Priority score threshold for escalation (0-100)"
    )

    predict_sla_breach: bool = Field(
        default=True,
        description="Predict if ticket will breach SLA"
    )

    include_explanation: bool = Field(
        default=True,
        description="Include explanation of priority score factors"
    )

    tier_weights: Optional[str] = Field(
        default=None,
        description="JSON dict of tier weight multipliers: {\"enterprise\": 1.5, \"pro\": 1.2, \"basic\": 1.0}"
    )

    urgency_weights: Optional[str] = Field(
        default=None,
        description="JSON dict of urgency weight multipliers: {\"critical\": 2.0, \"high\": 1.5, \"medium\": 1.0, \"low\": 0.5}"
    )

    batch_size: int = Field(
        default=50,
        description="Batch size for processing"
    )

    temperature: float = Field(
        default=0.0,
        description="Temperature for LLM (0.0 = deterministic)"
    )

    max_tokens: Optional[int] = Field(
        default=300,
        description="Max tokens for LLM response"
    )

    rate_limit_delay: float = Field(
        default=0.1,
        description="Delay in seconds between API calls"
    )

    max_retries: int = Field(
        default=3,
        description="Maximum retries for failed API calls"
    )

    track_costs: bool = Field(
        default=True,
        description="Track token usage and costs (LLM only)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="priority_scoring",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        method = self.method
        llm_provider = self.llm_provider
        llm_model = self.llm_model
        ml_model = self.ml_model
        api_key = self.api_key
        input_columns_str = self.input_columns
        priority_levels_str = self.priority_levels
        sla_hours_str = self.sla_hours
        customer_tier_column = self.customer_tier_column
        sentiment_score_column = self.sentiment_score_column
        urgency_column = self.urgency_column
        escalation_threshold = self.escalation_threshold
        predict_sla_breach = self.predict_sla_breach
        include_explanation = self.include_explanation
        tier_weights_str = self.tier_weights
        urgency_weights_str = self.urgency_weights
        batch_size = self.batch_size
        temperature = self.temperature
        max_tokens = self.max_tokens
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        track_costs = self.track_costs
        description = self.description or f"Priority scoring using {method}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cost per 1M tokens
        COST_PER_1M_INPUT = {
            "gpt-4": 30.0, "gpt-4-turbo": 10.0, "gpt-4o": 5.0, "gpt-4o-mini": 0.15, "gpt-3.5-turbo": 0.5,
            "claude-3-opus": 15.0, "claude-3-5-sonnet": 3.0, "claude-3-sonnet": 3.0, "claude-3-haiku": 0.25,
        }
        COST_PER_1M_OUTPUT = {
            "gpt-4": 60.0, "gpt-4-turbo": 30.0, "gpt-4o": 15.0, "gpt-4o-mini": 0.6, "gpt-3.5-turbo": 1.5,
            "claude-3-opus": 75.0, "claude-3-5-sonnet": 15.0, "claude-3-sonnet": 15.0, "claude-3-haiku": 1.25,
        }

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def priority_scorer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that scores ticket priority."""

            context.log.info(f"Starting priority scoring with method: {method}")

            # Get input DataFrame
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Priority Scorer requires an upstream DataFrame")

            # Parse configuration
            input_columns = [col.strip() for col in input_columns_str.split(',')]
            priority_levels = None if priority_levels_str == "numeric" else [level.strip() for level in priority_levels_str.split(',')]

            sla_hours = {}
            if sla_hours_str:
                try:
                    sla_hours = json.loads(sla_hours_str)
                except json.JSONDecodeError:
                    context.log.warning(f"Failed to parse sla_hours: {sla_hours_str}")

            tier_weights = {"enterprise": 1.5, "pro": 1.2, "basic": 1.0}
            if tier_weights_str:
                try:
                    tier_weights = json.loads(tier_weights_str)
                except json.JSONDecodeError:
                    context.log.warning(f"Failed to parse tier_weights: {tier_weights_str}")

            urgency_weights = {"critical": 2.0, "high": 1.5, "medium": 1.0, "low": 0.5}
            if urgency_weights_str:
                try:
                    urgency_weights = json.loads(urgency_weights_str)
                except json.JSONDecodeError:
                    context.log.warning(f"Failed to parse urgency_weights: {urgency_weights_str}")

            context.log.info(f"Scoring {len(input_df)} tickets")

            # Validate input columns
            for col in input_columns:
                if col not in input_df.columns:
                    context.log.warning(f"Input column '{col}' not found in DataFrame")

            # Results storage
            priority_scores = []
            priority_labels = []
            sla_breach_predictions = []
            explanations = []
            total_input_tokens = 0
            total_output_tokens = 0

            if method == "llm":
                context.log.info(f"Using LLM: {llm_provider}/{llm_model}")

                if not llm_provider or not llm_model:
                    raise ValueError("llm_provider and llm_model required for method=llm")

                # Expand API key
                expanded_api_key = None
                if api_key:
                    expanded_api_key = os.path.expandvars(api_key)
                    if expanded_api_key == api_key and api_key.startswith('${'):
                        var_name = api_key.strip('${}')
                        raise ValueError(f"Environment variable not set: {var_name}")

                # Build prompt template
                prompt_template = """Analyze the following support ticket and assign a priority score.

Ticket Information:
"""
                for col in input_columns:
                    prompt_template += f"- {col}: {{{col}}}\n"

                if customer_tier_column:
                    prompt_template += f"- Customer Tier: {{{customer_tier_column}}}\n"
                if sentiment_score_column:
                    prompt_template += f"- Sentiment Score: {{{sentiment_score_column}}}\n"
                if urgency_column:
                    prompt_template += f"- Urgency: {{{urgency_column}}}\n"

                prompt_template += "\n"

                if sla_hours:
                    prompt_template += f"SLA Requirements: {json.dumps(sla_hours)} hours by tier\n"

                prompt_template += f"""
Priority Assignment:
"""
                if priority_levels:
                    prompt_template += f"Levels: {', '.join(priority_levels)}\n"
                else:
                    prompt_template += "Score: 0-100 (0=lowest, 100=highest)\n"

                prompt_template += """
Consider:
1. Issue severity and impact
2. Customer tier and SLA requirements
3. Urgency and time sensitivity
4. Sentiment (negative = higher priority)
5. Business impact

Return your analysis as JSON:
{
  "priority_score": 0-100,
"""
                if priority_levels:
                    prompt_template += f"""  "priority_level": "one of {', '.join(priority_levels)}",
"""
                if predict_sla_breach:
                    prompt_template += """  "sla_breach_likely": true/false,
"""
                if include_explanation:
                    prompt_template += """  "explanation": "brief explanation of score",
  "factors": ["factor1", "factor2", ...]
"""
                prompt_template += "}"

                # Initialize LLM client
                if llm_provider == "openai":
                    try:
                        import openai
                        client = openai.OpenAI(api_key=expanded_api_key)
                    except ImportError:
                        raise ImportError("openai not installed. Install with: pip install openai")

                elif llm_provider == "anthropic":
                    try:
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)
                    except ImportError:
                        raise ImportError("anthropic not installed. Install with: pip install anthropic")
                else:
                    raise ValueError(f"Unsupported LLM provider: {llm_provider}")

                # Process each row
                for idx, row in input_df.iterrows():
                    # Build context
                    context_vars = {}
                    for col in input_columns:
                        if col in row.index:
                            context_vars[col] = str(row[col])
                    if customer_tier_column and customer_tier_column in row.index:
                        context_vars[customer_tier_column] = str(row[customer_tier_column])
                    if sentiment_score_column and sentiment_score_column in row.index:
                        context_vars[sentiment_score_column] = str(row[sentiment_score_column])
                    if urgency_column and urgency_column in row.index:
                        context_vars[urgency_column] = str(row[urgency_column])

                    prompt = prompt_template.format(**context_vars)

                    attempt = 0
                    success = False

                    while attempt < max_retries and not success:
                        try:
                            # Call LLM
                            if llm_provider == "openai":
                                response = client.chat.completions.create(
                                    model=llm_model,
                                    messages=[
                                        {"role": "system", "content": "You are a support ticket prioritization expert. Always return valid JSON."},
                                        {"role": "user", "content": prompt}
                                    ],
                                    temperature=temperature,
                                    max_tokens=max_tokens,
                                    response_format={"type": "json_object"}
                                )
                                result_text = response.choices[0].message.content
                                total_input_tokens += response.usage.prompt_tokens
                                total_output_tokens += response.usage.completion_tokens

                            elif llm_provider == "anthropic":
                                response = client.messages.create(
                                    model=llm_model,
                                    max_tokens=max_tokens or 500,
                                    temperature=temperature,
                                    messages=[{"role": "user", "content": prompt}]
                                )
                                result_text = response.content[0].text
                                total_input_tokens += response.usage.input_tokens
                                total_output_tokens += response.usage.output_tokens

                            # Parse JSON result
                            result = json.loads(result_text)

                            priority_scores.append(float(result.get('priority_score', 50)))
                            priority_labels.append(result.get('priority_level', 'P3'))
                            sla_breach_predictions.append(result.get('sla_breach_likely', False))

                            if include_explanation:
                                explanation = result.get('explanation', '')
                                factors = result.get('factors', [])
                                explanations.append(f"{explanation} | Factors: {', '.join(factors)}")

                            success = True

                        except Exception as e:
                            attempt += 1
                            if attempt < max_retries:
                                wait_time = (2 ** attempt) * rate_limit_delay
                                context.log.warning(f"Error scoring ticket {idx}: {e}. Retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                context.log.error(f"Failed to score ticket {idx} after {max_retries} attempts: {e}")
                                priority_scores.append(50.0)
                                priority_labels.append('P3')
                                sla_breach_predictions.append(False)
                                if include_explanation:
                                    explanations.append('')

                    if idx % 10 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{len(input_df)}")

                    if rate_limit_delay > 0 and success:
                        time.sleep(rate_limit_delay)

            elif method == "ml":
                context.log.info(f"Using ML model: {ml_model or 'logistic_regression'}")

                try:
                    from sklearn.preprocessing import StandardScaler
                    from sklearn.linear_model import LogisticRegression
                    from sklearn.ensemble import RandomForestClassifier

                    # For demo purposes, we'll create a simple scoring model
                    # In production, you'd load a pre-trained model
                    model_type = ml_model or "logistic_regression"

                    # Extract features
                    features = []
                    for _, row in input_df.iterrows():
                        feature_vec = []

                        # Text length feature
                        for col in input_columns:
                            if col in row.index:
                                text = str(row[col])
                                feature_vec.append(len(text))
                                feature_vec.append(len(text.split()))

                        # Customer tier feature
                        if customer_tier_column and customer_tier_column in row.index:
                            tier = str(row[customer_tier_column]).lower()
                            tier_weight = tier_weights.get(tier, 1.0)
                            feature_vec.append(tier_weight)
                        else:
                            feature_vec.append(1.0)

                        # Sentiment feature
                        if sentiment_score_column and sentiment_score_column in row.index:
                            feature_vec.append(float(row[sentiment_score_column]))
                        else:
                            feature_vec.append(0.5)

                        # Urgency feature
                        if urgency_column and urgency_column in row.index:
                            urgency = str(row[urgency_column]).lower()
                            urgency_weight = urgency_weights.get(urgency, 1.0)
                            feature_vec.append(urgency_weight)
                        else:
                            feature_vec.append(1.0)

                        features.append(feature_vec)

                    features_array = np.array(features)

                    # Simple heuristic scoring (replace with actual trained model)
                    # Normalize and weight features
                    if len(features_array) > 0:
                        for i in range(len(features_array)):
                            # Calculate priority score based on features
                            text_length = min(features_array[i][0] / 1000, 1.0)
                            tier_weight = features_array[i][2] if len(features_array[i]) > 2 else 1.0
                            sentiment = features_array[i][3] if len(features_array[i]) > 3 else 0.5
                            urgency_weight = features_array[i][4] if len(features_array[i]) > 4 else 1.0

                            # Score formula (can be replaced with ML model prediction)
                            base_score = 50
                            score = base_score
                            score += (1.0 - sentiment) * 20  # Negative sentiment increases priority
                            score += text_length * 10  # Longer tickets may be more complex
                            score *= tier_weight  # Tier multiplier
                            score *= urgency_weight  # Urgency multiplier

                            score = min(max(score, 0), 100)  # Clamp to 0-100
                            priority_scores.append(float(score))

                            # Map to priority level
                            if priority_levels:
                                if score >= 80:
                                    priority_labels.append(priority_levels[0])  # P1
                                elif score >= 60:
                                    priority_labels.append(priority_levels[1] if len(priority_levels) > 1 else priority_levels[0])  # P2
                                elif score >= 40:
                                    priority_labels.append(priority_levels[2] if len(priority_levels) > 2 else priority_levels[0])  # P3
                                else:
                                    priority_labels.append(priority_levels[3] if len(priority_levels) > 3 else priority_levels[-1])  # P4

                            # SLA breach prediction (simple heuristic)
                            if predict_sla_breach:
                                sla_breach_predictions.append(score >= 70)

                            if include_explanation:
                                factors = []
                                if sentiment < 0.4:
                                    factors.append("negative sentiment")
                                if urgency_weight > 1.2:
                                    factors.append("high urgency")
                                if tier_weight > 1.2:
                                    factors.append("premium customer")
                                explanations.append(f"Score: {score:.1f} | Factors: {', '.join(factors)}")

                except ImportError:
                    raise ImportError("scikit-learn not installed. Install with: pip install scikit-learn")

            elif method == "rules":
                context.log.info("Using rule-based scoring")

                # Rule-based scoring
                for _, row in input_df.iterrows():
                    base_score = 50.0

                    # Text analysis
                    text_content = ""
                    for col in input_columns:
                        if col in row.index:
                            text_content += str(row[col]) + " "

                    text_content = text_content.lower()

                    # Urgency keywords
                    urgent_keywords = ["urgent", "critical", "emergency", "asap", "immediately", "down", "broken", "not working"]
                    high_priority_keywords = ["important", "priority", "soon", "issue", "problem", "error"]

                    urgent_count = sum(1 for keyword in urgent_keywords if keyword in text_content)
                    high_priority_count = sum(1 for keyword in high_priority_keywords if keyword in text_content)

                    base_score += urgent_count * 15
                    base_score += high_priority_count * 5

                    # Customer tier
                    if customer_tier_column and customer_tier_column in row.index:
                        tier = str(row[customer_tier_column]).lower()
                        tier_weight = tier_weights.get(tier, 1.0)
                        base_score *= tier_weight

                    # Sentiment
                    if sentiment_score_column and sentiment_score_column in row.index:
                        sentiment = float(row[sentiment_score_column])
                        # Negative sentiment increases priority
                        base_score += (1.0 - sentiment) * 15

                    # Urgency
                    if urgency_column and urgency_column in row.index:
                        urgency = str(row[urgency_column]).lower()
                        urgency_weight = urgency_weights.get(urgency, 1.0)
                        base_score *= urgency_weight

                    # Clamp score
                    base_score = min(max(base_score, 0), 100)
                    priority_scores.append(float(base_score))

                    # Map to priority level
                    if priority_levels:
                        if base_score >= 80:
                            priority_labels.append(priority_levels[0])
                        elif base_score >= 60:
                            priority_labels.append(priority_levels[1] if len(priority_levels) > 1 else priority_levels[0])
                        elif base_score >= 40:
                            priority_labels.append(priority_levels[2] if len(priority_levels) > 2 else priority_levels[0])
                        else:
                            priority_labels.append(priority_levels[3] if len(priority_levels) > 3 else priority_levels[-1])

                    # SLA breach prediction
                    if predict_sla_breach:
                        sla_breach_predictions.append(base_score >= 70)

                    # Explanation
                    if include_explanation:
                        factors = []
                        if urgent_count > 0:
                            factors.append(f"{urgent_count} urgent keywords")
                        if high_priority_count > 0:
                            factors.append(f"{high_priority_count} priority keywords")
                        if customer_tier_column and customer_tier_column in row.index:
                            factors.append(f"{row[customer_tier_column]} tier")
                        if urgency_column and urgency_column in row.index:
                            factors.append(f"{row[urgency_column]} urgency")
                        explanations.append(f"Score: {base_score:.1f} | Factors: {', '.join(factors)}")

            else:
                raise ValueError(f"Unknown method: {method}")

            # Create result DataFrame
            result_df = input_df.copy()
            result_df['priority_score'] = priority_scores

            if priority_levels:
                result_df['priority_level'] = priority_labels

            if predict_sla_breach:
                result_df['sla_breach_likely'] = sla_breach_predictions

            if include_explanation:
                result_df['priority_explanation'] = explanations

            # Escalation flag
            if escalation_threshold is not None:
                result_df['requires_escalation'] = [score >= escalation_threshold for score in priority_scores]

            context.log.info(f"Priority scoring complete: {len(result_df)} tickets scored")

            # Calculate statistics
            avg_score = sum(priority_scores) / len(priority_scores) if priority_scores else 0
            high_priority_count = sum(1 for score in priority_scores if score >= 70)
            escalation_count = sum(1 for score in priority_scores if score >= escalation_threshold) if escalation_threshold else 0

            # Calculate costs
            cost_input = 0.0
            cost_output = 0.0
            if track_costs and method == "llm" and total_input_tokens > 0:
                for key in COST_PER_1M_INPUT.keys():
                    if key in llm_model:
                        cost_input = (total_input_tokens / 1_000_000) * COST_PER_1M_INPUT[key]
                        cost_output = (total_output_tokens / 1_000_000) * COST_PER_1M_OUTPUT[key]
                        break

            total_cost = cost_input + cost_output

            # Metadata
            metadata = {
                "method": method,
                "model": llm_model or ml_model or "rules",
                "num_scored": len(result_df),
                "avg_priority_score": float(avg_score),
                "high_priority_count": high_priority_count,
                "escalation_count": escalation_count,
            }

            if method == "llm" and track_costs:
                metadata["total_input_tokens"] = total_input_tokens
                metadata["total_output_tokens"] = total_output_tokens
                metadata["estimated_cost_usd"] = f"${total_cost:.4f}"

            if priority_levels:
                level_counts = pd.Series(priority_labels).value_counts().to_dict()
                metadata["priority_level_distribution"] = level_counts

            if predict_sla_breach:
                breach_count = sum(sla_breach_predictions)
                metadata["predicted_sla_breaches"] = breach_count
                metadata["breach_rate"] = f"{breach_count / len(result_df) * 100:.1f}%"

            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(result_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[priority_scorer_asset])
