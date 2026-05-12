"""Ticket Classifier Component.

Classify support tickets by category, urgency, department, and sentiment using
LLM-based (GPT/Claude) or transformer-based (BERT, RoBERTa) methods.
"""

import os
import json
import time
from typing import Any, Dict, List, Optional
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class TicketClassifierComponent(Component, Model, Resolvable):
    """Component for classifying support tickets.

    This component performs multi-class classification of support tickets,
    assigning categories, urgency levels, departments, and sentiment.
    It supports both LLM-based (GPT/Claude) and transformer-based (BERT, RoBERTa) methods.

    Features:
    - Multi-class classification (category, urgency, department, sentiment)
    - LLM-based or transformer-based methods
    - Custom category definitions
    - Confidence scores for each prediction
    - Batch processing
    - Multi-language support
    - Integration with ticketing systems (Zendesk, Intercom, Freshdesk)
    - Cost tracking and estimation

    Use Cases:
    - Support ticket routing and prioritization
    - Automated triage for customer support
    - Ticket categorization for analytics
    - Department assignment
    - Urgency detection for SLA management
    - Multi-channel support ticket processing

    Example:
        ```yaml
        type: dagster_component_templates.TicketClassifierComponent
        attributes:
          asset_name: classified_tickets
          method: llm
          llm_provider: openai
          llm_model: gpt-4o-mini
          api_key: "${OPENAI_API_KEY}"
          input_column: ticket_text
          categories: "technical,billing,account,product,other"
          urgency_levels: "low,medium,high,critical"
          departments: "engineering,finance,customer_success,sales"
          include_confidence: true
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold classified ticket data"
    )

    method: str = Field(
        default="llm",
        description="Classification method: llm (GPT/Claude) or transformer (BERT, RoBERTa)"
    )

    llm_provider: Optional[str] = Field(
        default=None,
        description="LLM provider: openai or anthropic (for method=llm)"
    )

    llm_model: Optional[str] = Field(
        default=None,
        description="LLM model: gpt-4o-mini, gpt-4, claude-3-haiku, claude-3-5-sonnet (for method=llm)"
    )

    transformer_model: Optional[str] = Field(
        default=None,
        description="Transformer model: bert-base-uncased, distilbert-base-uncased, roberta-base (for method=transformer)"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key for LLM method. Use ${OPENAI_API_KEY} or ${ANTHROPIC_API_KEY}"
    )

    input_column: str = Field(
        default="ticket_text",
        description="Column name containing ticket text"
    )

    categories: str = Field(
        default="technical,billing,account,product,general,other",
        description="Comma-separated list of ticket categories"
    )

    urgency_levels: str = Field(
        default="low,medium,high,critical",
        description="Comma-separated list of urgency levels"
    )

    departments: Optional[str] = Field(
        default=None,
        description="Comma-separated list of departments for routing (optional)"
    )

    classify_sentiment: bool = Field(
        default=True,
        description="Include sentiment classification (positive, neutral, negative)"
    )

    include_confidence: bool = Field(
        default=True,
        description="Include confidence scores for classifications"
    )

    include_reasoning: bool = Field(
        default=False,
        description="Include reasoning/explanation for classifications (LLM only)"
    )

    multi_label: bool = Field(
        default=False,
        description="Allow multiple categories per ticket (e.g., technical+billing)"
    )

    confidence_threshold: Optional[float] = Field(
        default=None,
        description="Minimum confidence threshold (0.0-1.0). Below this = 'uncertain'"
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
        default=200,
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

    enable_caching: bool = Field(
        default=True,
        description="Cache API responses to avoid redundant calls"
    )

    track_costs: bool = Field(
        default=True,
        description="Track token usage and costs (LLM only)"
    )

    include_metadata_fields: Optional[str] = Field(
        default=None,
        description="Comma-separated list of metadata fields to include in classification (e.g., 'customer_tier,channel')"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="ticket_classification",
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with ticket text to classify")

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



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        method = self.method
        llm_provider = self.llm_provider
        llm_model = self.llm_model
        transformer_model = self.transformer_model
        api_key = self.api_key
        input_column = self.input_column
        categories_str = self.categories
        urgency_levels_str = self.urgency_levels
        departments_str = self.departments
        classify_sentiment = self.classify_sentiment
        include_confidence = self.include_confidence
        include_reasoning = self.include_reasoning
        multi_label = self.multi_label
        confidence_threshold = self.confidence_threshold
        batch_size = self.batch_size
        temperature = self.temperature
        max_tokens = self.max_tokens
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        enable_caching = self.enable_caching
        track_costs = self.track_costs
        include_metadata_fields_str = self.include_metadata_fields
        description = self.description or f"Ticket classification using {method}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key

        # Cost per 1M tokens (approximate)
        COST_PER_1M_INPUT = {
            "gpt-4": 30.0,
            "gpt-4-turbo": 10.0,
            "gpt-4o": 5.0,
            "gpt-4o-mini": 0.15,
            "gpt-3.5-turbo": 0.5,
            "claude-3-opus": 15.0,
            "claude-3-5-sonnet": 3.0,
            "claude-3-sonnet": 3.0,
            "claude-3-haiku": 0.25,
        }
        COST_PER_1M_OUTPUT = {
            "gpt-4": 60.0,
            "gpt-4-turbo": 30.0,
            "gpt-4o": 15.0,
            "gpt-4o-mini": 0.6,
            "gpt-3.5-turbo": 1.5,
            "claude-3-opus": 75.0,
            "claude-3-5-sonnet": 15.0,
            "claude-3-sonnet": 15.0,
            "claude-3-haiku": 1.25,
        }

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "ticket_classifier"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def ticket_classifier_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Asset that classifies support tickets."""

            context.log.info(f"Starting ticket classification with method: {method}")

            input_df = upstream
            context.log.info(f"Received DataFrame: {len(input_df)} rows")

            # Validate input column
            if input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Parse configuration
            categories = [cat.strip() for cat in categories_str.split(',')]
            urgency_levels = [level.strip() for level in urgency_levels_str.split(',')]
            departments = [dept.strip() for dept in departments_str.split(',')] if departments_str else None
            include_metadata_fields = [field.strip() for field in include_metadata_fields_str.split(',')] if include_metadata_fields_str else []

            context.log.info(f"Categories: {categories}")
            context.log.info(f"Urgency levels: {urgency_levels}")
            if departments:
                context.log.info(f"Departments: {departments}")

            # Extract ticket texts
            tickets = input_df[input_column].astype(str).tolist()
            context.log.info(f"Classifying {len(tickets)} tickets")

            # Results storage
            category_results = []
            urgency_results = []
            department_results = [] if departments else None
            sentiment_results = [] if classify_sentiment else None
            category_confidence = [] if include_confidence else None
            urgency_confidence = [] if include_confidence else None
            department_confidence = [] if include_confidence and departments else None
            sentiment_confidence = [] if include_confidence and classify_sentiment else None
            reasoning_list = [] if include_reasoning else None

            # Token tracking
            total_input_tokens = 0
            total_output_tokens = 0

            if method == "llm":
                context.log.info(f"Using LLM: {llm_provider}/{llm_model}")

                # Validate LLM parameters
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
                prompt_template = f"""Classify the following support ticket.

Ticket: {{ticket_text}}
"""

                if include_metadata_fields:
                    prompt_template += "\nAdditional Context:\n"
                    for field in include_metadata_fields:
                        prompt_template += f"- {field}: {{{field}}}\n"

                prompt_template += f"""
Classification Required:
1. Category (choose {"one or more" if multi_label else "one"}): {', '.join(categories)}
2. Urgency Level: {', '.join(urgency_levels)}
"""

                if departments:
                    prompt_template += f"3. Department: {', '.join(departments)}\n"

                if classify_sentiment:
                    prompt_template += "4. Sentiment: positive, neutral, negative\n"

                prompt_template += """
Return your classification as JSON:
{
  "category": """

                if multi_label:
                    prompt_template += '["category1", "category2"]'
                else:
                    prompt_template += '"category"'

                prompt_template += """,
  "urgency": "level","""

                if departments:
                    prompt_template += """
  "department": "dept","""

                if classify_sentiment:
                    prompt_template += """
  "sentiment": "sentiment","""

                if include_confidence:
                    prompt_template += """
  "confidence": {
    "category": 0.0-1.0,
    "urgency": 0.0-1.0"""
                    if departments:
                        prompt_template += """,
    "department": 0.0-1.0"""
                    if classify_sentiment:
                        prompt_template += """,
    "sentiment": 0.0-1.0"""
                    prompt_template += "\n  }"

                if include_reasoning:
                    prompt_template += """,
  "reasoning": "brief explanation of classification" """

                prompt_template += "\n}"

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

                # Process each ticket
                for idx, row in input_df.iterrows():
                    ticket_text = str(row[input_column])

                    # Build context with metadata fields
                    context_vars = {"ticket_text": ticket_text}
                    for field in include_metadata_fields:
                        if field in row.index:
                            context_vars[field] = str(row[field])

                    prompt = prompt_template
                    for _k, _v in context_vars.items():
                        prompt = prompt.replace("{" + _k + "}", str(_v))

                    # Call LLM with retry logic
                    attempt = 0
                    success = False

                    while attempt < max_retries and not success:
                        try:
                            # Call LLM
                            if llm_provider == "openai":
                                response = client.chat.completions.create(
                                    model=llm_model,
                                    messages=[
                                        {"role": "system", "content": "You are a support ticket classification expert. Always return valid JSON."},
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

                            # Extract category
                            category = result.get('category', 'other')
                            if multi_label and isinstance(category, list):
                                category_results.append(','.join(category))
                            else:
                                if isinstance(category, list):
                                    category = category[0]
                                category_results.append(category)

                            # Extract urgency
                            urgency_results.append(result.get('urgency', 'medium'))

                            # Extract department
                            if departments:
                                department_results.append(result.get('department', departments[0]))

                            # Extract sentiment
                            if classify_sentiment:
                                sentiment_results.append(result.get('sentiment', 'neutral'))

                            # Extract confidence scores
                            if include_confidence:
                                confidence = result.get('confidence', {})
                                category_confidence.append(float(confidence.get('category', 1.0)))
                                urgency_confidence.append(float(confidence.get('urgency', 1.0)))
                                if departments:
                                    department_confidence.append(float(confidence.get('department', 1.0)))
                                if classify_sentiment:
                                    sentiment_confidence.append(float(confidence.get('sentiment', 1.0)))

                            # Extract reasoning
                            if include_reasoning:
                                reasoning_list.append(result.get('reasoning', ''))

                            success = True

                        except Exception as e:
                            attempt += 1
                            if attempt < max_retries:
                                wait_time = (2 ** attempt) * rate_limit_delay
                                context.log.warning(f"Error classifying ticket {idx}: {e}. Retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                context.log.error(f"Failed to classify ticket {idx} after {max_retries} attempts: {e}")
                                # Add default values
                                category_results.append('other')
                                urgency_results.append('medium')
                                if departments:
                                    department_results.append(departments[0])
                                if classify_sentiment:
                                    sentiment_results.append('neutral')
                                if include_confidence:
                                    category_confidence.append(0.0)
                                    urgency_confidence.append(0.0)
                                    if departments:
                                        department_confidence.append(0.0)
                                    if classify_sentiment:
                                        sentiment_confidence.append(0.0)
                                if include_reasoning:
                                    reasoning_list.append('')

                    if idx % 10 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{len(tickets)}")

                    # Rate limiting
                    if rate_limit_delay > 0 and success:
                        time.sleep(rate_limit_delay)

            elif method == "transformer":
                context.log.info(f"Using transformer model: {transformer_model or 'default'}")

                try:
                    from transformers import pipeline
                    import torch

                    # Load classification pipeline
                    model_name = transformer_model or "bert-base-uncased"
                    context.log.info(f"Loading model: {model_name}")

                    # For this demo, we'll use zero-shot classification for categories and urgency
                    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

                    # Process in batches
                    for i in range(0, len(tickets), batch_size):
                        batch = tickets[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1}/{(len(tickets) - 1) // batch_size + 1}")

                        for ticket_text in batch:
                            # Classify category
                            cat_result = classifier(ticket_text, categories, multi_label=multi_label)
                            if multi_label:
                                # Get top scoring categories
                                top_cats = [cat_result['labels'][i] for i in range(min(3, len(cat_result['labels']))) if cat_result['scores'][i] > 0.5]
                                category_results.append(','.join(top_cats) if top_cats else cat_result['labels'][0])
                            else:
                                category_results.append(cat_result['labels'][0])

                            if include_confidence:
                                category_confidence.append(float(cat_result['scores'][0]))

                            # Classify urgency
                            urgency_result = classifier(ticket_text, urgency_levels)
                            urgency_results.append(urgency_result['labels'][0])
                            if include_confidence:
                                urgency_confidence.append(float(urgency_result['scores'][0]))

                            # Classify department
                            if departments:
                                dept_result = classifier(ticket_text, departments)
                                department_results.append(dept_result['labels'][0])
                                if include_confidence:
                                    department_confidence.append(float(dept_result['scores'][0]))

                            # Classify sentiment
                            if classify_sentiment:
                                sentiment_pipeline = pipeline("sentiment-analysis")
                                sent_result = sentiment_pipeline(ticket_text)[0]
                                label = sent_result['label'].lower()
                                if 'positive' in label or label == 'label_1':
                                    sentiment_results.append('positive')
                                elif 'negative' in label or label == 'label_0':
                                    sentiment_results.append('negative')
                                else:
                                    sentiment_results.append('neutral')
                                if include_confidence:
                                    sentiment_confidence.append(float(sent_result['score']))

                except ImportError:
                    raise ImportError("transformers not installed. Install with: pip install transformers torch")

            else:
                raise ValueError(f"Unknown method: {method}")

            # Apply confidence threshold
            if confidence_threshold and include_confidence:
                for i in range(len(category_results)):
                    if category_confidence[i] < confidence_threshold:
                        category_results[i] = 'uncertain'
                    if urgency_confidence[i] < confidence_threshold:
                        urgency_results[i] = 'uncertain'

            # Create result DataFrame
            result_df = input_df.copy()
            result_df['ticket_category'] = category_results
            result_df['ticket_urgency'] = urgency_results

            if include_confidence:
                result_df['category_confidence'] = category_confidence
                result_df['urgency_confidence'] = urgency_confidence

            if departments:
                result_df['ticket_department'] = department_results
                if include_confidence:
                    result_df['department_confidence'] = department_confidence

            if classify_sentiment:
                result_df['ticket_sentiment'] = sentiment_results
                if include_confidence:
                    result_df['sentiment_confidence'] = sentiment_confidence

            if include_reasoning:
                result_df['classification_reasoning'] = reasoning_list

            context.log.info(f"Classification complete: {len(result_df)} tickets classified")

            # Calculate statistics
            category_counts = pd.Series(category_results).value_counts().to_dict()
            urgency_counts = pd.Series(urgency_results).value_counts().to_dict()

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
                "model": llm_model or transformer_model or "default",
                "num_classified": len(result_df),
                "category_distribution": category_counts,
                "urgency_distribution": urgency_counts,
            }

            if method == "llm" and track_costs:
                metadata["total_input_tokens"] = total_input_tokens
                metadata["total_output_tokens"] = total_output_tokens
                metadata["estimated_cost_usd"] = f"${total_cost:.4f}"

            if include_confidence:
                metadata["avg_category_confidence"] = float(sum(category_confidence) / len(category_confidence))
                metadata["avg_urgency_confidence"] = float(sum(urgency_confidence) / len(urgency_confidence))

            if departments:
                dept_counts = pd.Series(department_results).value_counts().to_dict()
                metadata["department_distribution"] = dept_counts

            if classify_sentiment:
                sentiment_counts = pd.Series(sentiment_results).value_counts().to_dict()
                metadata["sentiment_distribution"] = sentiment_counts

            if include_preview and len(result_df) > 0:
                context.add_output_metadata({
                        **metadata,
                        "preview": MetadataValue.md(result_df.head(10).to_markdown())
                    })
                return result_df
            else:
                context.add_output_metadata(metadata)
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[ticket_classifier_asset])


        return Definitions(assets=[ticket_classifier_asset], asset_checks=list(_schema_checks))
