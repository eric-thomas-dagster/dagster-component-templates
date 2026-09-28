"""Lead Scoring Component.

Scores and qualifies leads based on firmographic fit and behavioral intent to
prioritize sales efforts and optimize marketing-to-sales handoff.

`scoring_method='heuristic'` (default): the original 3-way join (lead_data +
behavioral_data + company_data, each independently optional) and hand-tuned
fit/intent scoring, unchanged in spirit -- but see the company_data fix note
below. `scoring_method='ml'`: fits a real scikit-learn classifier against a
`target_column` you supply (e.g. a historical `converted`/`became_opportunity`
boolean) over a single, already-joined `upstream_asset_key`/`source` table --
none of the fit/intent formulas are trained against any observed outcome
today, so 'ml' mode requires you bring your own label and feature table.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    OpExecutionContext,
    asset,
    MetadataValue,
    Component,
    Model,
    Resolvable,
    ComponentLoadContext,
    Output,
)
from dagster._core.definitions.definitions_class import Definitions
from pydantic import Field


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine() (SQLAlchemy)
    OR .get_connection() (DB-API), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy) "
            f"or .get_connection() (DB-API); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


# ── scoring_method='ml', execution_mode='sql' ───────────────────────────
# Reuses the exact same audited BQ/Snowflake/Databricks LOGISTIC_REG mapping
# as logistic_regression_model -- lead conversion is a binary classification
# task once you have a real `target_column`, same as any other.
# validation.level: code -- no live warehouse credentials in this dev
# environment; the generated SQL is asserted structurally, not executed.

_SQL_MODEL_DIALECTS = ("snowflake", "bigquery", "databricks")


def _build_sql_mode_statements(
    dialect: str, source_sql: str, output_table: str, model_name: str,
    target_column: str, feature_columns: List[str], test_size: float, max_iter: int,
) -> List[str]:
    if dialect not in _SQL_MODEL_DIALECTS:
        raise ValueError(f"unsupported sql_dialect: {dialect!r}. Valid: {_SQL_MODEL_DIALECTS}")

    if dialect == "bigquery":
        feat_csv = ", ".join(feature_columns)
        return [
            f"CREATE OR REPLACE MODEL `{model_name}`\n"
            f"OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['{target_column}'], "
            f"max_iterations={max_iter}, data_split_method='RANDOM', "
            f"data_split_eval_fraction={test_size}) AS\n"
            f"SELECT {feat_csv}, {target_column}\n"
            f"FROM ({source_sql})",

            f"CREATE OR REPLACE TABLE {output_table} AS\n"
            f"SELECT * FROM ML.PREDICT(MODEL `{model_name}`, (SELECT * FROM ({source_sql})))",
        ]

    if dialect == "snowflake":
        view_name = f"{output_table}_training_view"
        return [
            f"CREATE OR REPLACE VIEW {view_name} AS {source_sql}",

            f"CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION {model_name}(\n"
            f"  INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{view_name}'),\n"
            f"  TARGET_COLNAME => '{target_column}'\n"
            f")",

            f"CREATE OR REPLACE TABLE {output_table} AS\n"
            f"SELECT *, {model_name}!PREDICT(INPUT_DATA => {{*}}) AS prediction\n"
            f"FROM {view_name}",
        ]

    # databricks: predict-only against an already-served endpoint (model_name
    # is that endpoint's name here, not something this statement creates).
    feat_struct = ", ".join(f"'{c}', src.{c}" for c in feature_columns)
    return [
        f"CREATE OR REPLACE TABLE {output_table} AS\n"
        f"SELECT src.*, ai_query('{model_name}', named_struct({feat_struct})) AS predicted_class\n"
        f"FROM ({source_sql}) AS src"
    ]


def _run_sql_mode(context, source_cfg: dict, statements: List[str], output_table: str) -> Output:
    resource_key = source_cfg.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            engine = resource.get_engine()
            with engine.begin() as conn:
                for stmt in statements:
                    conn.exec_driver_sql(stmt)
                row_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {output_table}").scalar()
        elif hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                for stmt in statements:
                    conn.execute(stmt)
                row_count = conn.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
        else:
            raise ValueError(f"resource {resource_key!r} must expose .get_engine() or .get_connection().")
    else:
        env_var = source_cfg.get("database_url_env_var")
        if not env_var:
            raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        engine = create_engine(url)
        with engine.begin() as conn:
            for stmt in statements:
                conn.exec_driver_sql(stmt)
            row_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {output_table}").scalar()

    return Output(
        value=None,
        metadata={
            "dagster/row_count": MetadataValue.int(row_count),
            "execution_mode": MetadataValue.text("sql"),
            "output_table": MetadataValue.text(output_table),
            "generated_sql": MetadataValue.md("\n\n".join(f"```sql\n{s}\n```" for s in statements)),
        },
    )


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


class LeadScoringComponent(Component, Model, Resolvable):
    """Component that scores and qualifies leads for sales prioritization."""

    asset_name: str = Field(
        ...,
        description="Name of the lead scoring asset to create",
    )

    # ── scoring_method='heuristic' (default): 3-way join, each independently optional ──
    lead_data_asset_key: Optional[str] = Field(
        default=None,
        description="Lead/contact data from CRM. Mutually exclusive with `lead_data_source` -- set at most one.",
    )
    lead_data_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull lead/contact data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `lead_data_asset_key` -- set at most one.",
    )

    behavioral_data_asset_key: Optional[str] = Field(
        default=None,
        description="Behavioral/activity data (web visits, email opens, etc.). Mutually exclusive with `behavioral_data_source` -- set at most one.",
    )
    behavioral_data_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull behavioral/activity data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `behavioral_data_asset_key` -- set at most one.",
    )

    company_data_asset_key: Optional[str] = Field(
        default=None,
        description="Company/firmographic data for B2B scoring, joined onto lead_data by a shared id column (company_id/account_id/organization_id, whichever is present in both). Mutually exclusive with `company_data_source` -- set at most one.",
    )
    company_data_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull company/firmographic data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `company_data_asset_key` -- set at most one.",
    )

    scoring_method: str = Field(
        default="heuristic",
        description=(
            "'heuristic' (default): the original 3-way join (lead_data_asset_key/_source, "
            "behavioral_data_asset_key/_source, company_data_asset_key/_source, each "
            "independently optional) and hand-tuned fit/intent scoring below. 'ml': fits a "
            "real scikit-learn classifier against a `target_column` you supply over a single, "
            "already-joined `upstream_asset_key`/`source` table -- none of the fit/intent "
            "formulas are trained against any observed outcome today, so 'ml' mode requires "
            "you bring your own label and feature table, and cannot be combined with the "
            "3-way join fields above."
        ),
    )

    # ── scoring_method='ml': standard single-input dual ingestion + dual execution mode ──
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="scoring_method='ml' only. Upstream asset key providing an already-joined DataFrame with target_column + feature_columns. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "scoring_method='ml' only. Pull rows directly via SQL instead of from an upstream "
            "asset: {kind: warehouse_query, resource_key: <registered resource> OR "
            "database_url_env_var: <env var>, sql: <query>}. Also required (with "
            "execution_mode='sql') to name the FROM-source for the server-side "
            "training/prediction query. Mutually exclusive with `upstream_asset_key` -- set "
            "exactly one."
        ),
    )
    execution_mode: str = Field(
        default="python",
        description=(
            "Only meaningful when scoring_method='ml'. 'python' (default): fits a real "
            "scikit-learn LogisticRegression locally. 'sql': trains AND predicts "
            "server-side via BigQuery/Snowflake ML (Databricks is predict-only). "
            "Requires `source`, `sql_dialect`, `output_table`, and `model_name`."
        ),
    )
    sql_dialect: Optional[str] = Field(
        default=None,
        description=f"Required when scoring_method='ml' and execution_mode='sql'. One of: {_SQL_MODEL_DIALECTS}.",
    )
    output_table: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. Destination table the predictions are written to.",
    )
    model_name: Optional[str] = Field(
        default=None,
        description=(
            "Required when execution_mode='sql'. For snowflake/bigquery: the identifier this "
            "component creates the model under. For databricks: the name of an already-served "
            "Model Serving endpoint -- this dialect trains nothing."
        ),
    )
    target_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Required when scoring_method='ml'. Column name of the historical conversion label (e.g. 'converted') -- this does NOT exist in the heuristic's input schema; you must supply it.",
    )
    feature_columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Required when scoring_method='ml'. List of column names to use as classifier features.",
    )
    test_size: float = Field(default=0.2, description="scoring_method='ml' only. Fraction of data to hold out for evaluation")
    random_state: int = Field(default=42, description="scoring_method='ml' only. Random seed for reproducibility")
    max_iter: int = Field(default=1000, description="scoring_method='ml' only. Maximum number of solver iterations")
    model_path: Optional[str] = Field(
        default=None,
        description=(
            "scoring_method='ml' only. If set, joblib-dump the trained model to this "
            "path after fit. Supports local paths and any fsspec URL (s3://, gs://, abfs://)."
        ),
    )
    output_probabilities: bool = Field(default=True, description="scoring_method='ml' only. Add predicted_proba_<class> columns per class")
    normalize: bool = Field(default=True, description="scoring_method='ml' only. Standardize features with StandardScaler before fitting")

    # ── scoring_method='heuristic' only, below ──
    scoring_model: str = Field(
        default="combined",
        description="scoring_method='heuristic' only. Scoring model: fit_only, intent_only, or combined",
    )

    fit_weight: float = Field(
        default=0.4,
        description="scoring_method='heuristic' only. Weight for fit score in combined model (0-1)",
    )

    intent_weight: float = Field(
        default=0.6,
        description="scoring_method='heuristic' only. Weight for intent score in combined model (0-1)",
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

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )

    group_name: str = Field(
        default="analytics",
        description="Asset group name",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
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
    partition_static_column: Optional[Union[str, int]] = Field(
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    def _normalize_score(self, value: "pd.Series", min_val: float = 0.0, max_val: float = 1.0) -> "pd.Series":
        """Normalize a Series to 0-100 scale.

        FIX (2026-09-28): this used a scalar `if pd.isna(value): ...` guard,
        but every one of its 7 call sites in `_calculate_intent_score` passes
        a full pandas Series (e.g. `behavioral_data['email_opens'].fillna(0)`)
        -- `pd.isna()` on a multi-element Series returns a Series of bools,
        and `if <Series>:` raises `ValueError: The truth value of a Series is
        ambiguous` for any real behavioral_data with more than one row. This
        means intent scoring has never worked on real multi-row data before
        this fix (confirmed live: crashed immediately on a 3-row DataFrame).
        Rewritten to be genuinely vectorized.
        """
        if max_val == min_val:
            return pd.Series(50.0, index=value.index)

        normalized = ((value - min_val) / (max_val - min_val)) * 100
        return normalized.fillna(0.0).clip(0, 100)

    def _calculate_fit_score(self, lead_data: pd.DataFrame, company_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate fit score from demographic/firmographic data.

        FIX (2026-09-28): `company_data` used to be accepted as a parameter
        and passed at the call site, but this function's body never
        referenced it at all -- `company_data_asset_key` was a completely
        non-functional input, advertised in fields/schema/README but with
        zero effect on the output. Now: if company_data is provided, it is
        merged onto lead_data (left join) on whichever of
        company_id/account_id/organization_id is present in both frames,
        and its firmographic columns (company_size/employee_count/industry/
        annual_revenue/country/region) fill in wherever lead_data lacks
        them. If no shared join key exists, company_data is silently
        ignored (same as before this fix), since there is no way to
        associate its rows with leads.
        """
        scores = pd.DataFrame()

        if lead_data is None or lead_data.empty:
            return scores

        working = lead_data
        if company_data is not None and not company_data.empty:
            join_key = next(
                (k for k in ("company_id", "account_id", "organization_id")
                 if k in lead_data.columns and k in company_data.columns),
                None,
            )
            if join_key:
                merged = lead_data.merge(
                    company_data, on=join_key, how="left", suffixes=("", "_company")
                )
                for col in ("company_size", "employee_count", "industry", "annual_revenue", "country", "region"):
                    company_col = f"{col}_company"
                    if company_col in merged.columns:
                        if col in merged.columns:
                            merged[col] = merged[col].fillna(merged[company_col])
                        else:
                            merged[col] = merged[company_col]
                working = merged

        lead_data = working
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
        """Build asset definitions."""
        asset_name = self.asset_name
        scoring_method = self.scoring_method
        upstream_asset_key = self.upstream_asset_key
        source_cfg = self.source
        execution_mode = self.execution_mode
        sql_dialect = self.sql_dialect
        output_table = self.output_table
        model_name = self.model_name
        target_column = self.target_column
        feature_columns = self.feature_columns
        test_size = self.test_size
        random_state = self.random_state
        max_iter = self.max_iter
        model_path = self.model_path
        output_probabilities = self.output_probabilities
        normalize = self.normalize
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        if scoring_method not in ("heuristic", "ml"):
            raise ValueError(f"LeadScoringComponent: scoring_method must be 'heuristic' or 'ml', got {scoring_method!r}.")
        if execution_mode not in ("python", "sql"):
            raise ValueError(f"LeadScoringComponent: execution_mode must be 'python' or 'sql', got {execution_mode!r}.")

        _multi_input_fields_set = any([
            self.lead_data_asset_key, self.lead_data_source,
            self.behavioral_data_asset_key, self.behavioral_data_source,
            self.company_data_asset_key, self.company_data_source,
        ])

        if scoring_method == "ml":
            if _multi_input_fields_set:
                raise ValueError(
                    "LeadScoringComponent: scoring_method='ml' cannot be combined with the "
                    "3-way join fields (lead_data_asset_key/_source, "
                    "behavioral_data_asset_key/_source, company_data_asset_key/_source). Use "
                    "`upstream_asset_key`/`source` to point at a single, already-joined "
                    "feature+label table instead."
                )
            if bool(upstream_asset_key) == bool(source_cfg):
                raise ValueError("LeadScoringComponent: scoring_method='ml' requires exactly one of `upstream_asset_key` or `source`.")
            if not target_column:
                raise ValueError("LeadScoringComponent: scoring_method='ml' requires `target_column` (a historical conversion label the heuristic never needed).")
            if not feature_columns:
                raise ValueError("LeadScoringComponent: scoring_method='ml' requires `feature_columns`.")
            if execution_mode == "sql":
                if not source_cfg:
                    raise ValueError("LeadScoringComponent: execution_mode='sql' requires `source` (a SQL FROM-source).")
                if sql_dialect not in _SQL_MODEL_DIALECTS:
                    raise ValueError(f"LeadScoringComponent: execution_mode='sql' requires sql_dialect to be one of {_SQL_MODEL_DIALECTS}.")
                if not output_table:
                    raise ValueError("LeadScoringComponent: execution_mode='sql' requires `output_table`.")
                if not model_name:
                    raise ValueError("LeadScoringComponent: execution_mode='sql' requires `model_name`.")
        else:
            if upstream_asset_key or source_cfg or target_column or feature_columns:
                raise ValueError(
                    "LeadScoringComponent: `upstream_asset_key`/`source`/`target_column`/"
                    "`feature_columns` require scoring_method='ml'."
                )
            if execution_mode == "sql":
                raise ValueError("LeadScoringComponent: execution_mode='sql' requires scoring_method='ml' (there is no SQL-mode heuristic).")
            for _label, _asset_key, _source in (
                ("lead_data", self.lead_data_asset_key, self.lead_data_source),
                ("behavioral_data", self.behavioral_data_asset_key, self.behavioral_data_source),
                ("company_data", self.company_data_asset_key, self.company_data_source),
            ):
                if _asset_key and _source:
                    raise ValueError(f"LeadScoringComponent: set at most one of `{_label}_asset_key` or `{_label}_source`.")
            if not _multi_input_fields_set:
                raise ValueError(
                    "LeadScoringComponent: at least one of lead_data, behavioral_data, or "
                    "company_data must be connected (via *_asset_key or *_source)."
                )

        component = self

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
        _comp_name = "lead_scoring"  # component directory name
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


        _base_asset_kwargs: Dict[str, Any] = dict(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            partitions_def=partitions_def,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )

        # ── scoring_method='ml' ──────────────────────────────────────────
        if scoring_method == "ml":
            _ml_kwargs = dict(_base_asset_kwargs)
            _ml_kwargs["description"] = self.description or "Lead conversion predictions from a trained classifier"
            if upstream_asset_key:
                _ml_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
            if source_cfg and source_cfg.get("resource_key"):
                _ml_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

            if execution_mode == "sql":
                @asset(**_ml_kwargs)
                def _sql_asset(context: AssetExecutionContext):
                    statements = _build_sql_mode_statements(
                        sql_dialect, source_cfg["sql"], output_table, model_name,
                        target_column, list(feature_columns), test_size, max_iter,
                    )
                    return _run_sql_mode(context, source_cfg, statements, output_table)

                return Definitions(assets=[_sql_asset])

            @asset(**_ml_kwargs)
            def lead_scoring_ml_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
                upstream = kwargs.get("upstream")
                if upstream is None:
                    upstream = _ingest_warehouse_query(source_cfg, context)
                if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                    upstream = upstream.value
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
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

                try:
                    from sklearn.linear_model import LogisticRegression
                    from sklearn.metrics import accuracy_score
                    from sklearn.model_selection import train_test_split
                    from sklearn.preprocessing import StandardScaler
                except ImportError as e:
                    raise ImportError("scikit-learn is required: pip install scikit-learn") from e

                ml_df = upstream.copy()
                X = ml_df[list(feature_columns)].apply(pd.to_numeric, errors="coerce").fillna(0)
                y = ml_df[target_column]

                if len(X) < 5:
                    context.log.warning(
                        f"lead_scoring (ml): only {len(X)} rows available; skipping "
                        "train/test split (whole frame used for both fit and eval)."
                    )
                    X_train = X_test = X
                    y_train = y_test = y
                else:
                    X_train, X_test, y_train, y_test = train_test_split(
                        X, y, test_size=test_size, random_state=random_state
                    )

                scaler = None
                if normalize:
                    scaler = StandardScaler()
                    X_train = scaler.fit_transform(X_train)
                    X_test = scaler.transform(X_test)

                model = LogisticRegression(max_iter=max_iter, random_state=random_state)
                model.fit(X_train, y_train)

                if model_path is not None:
                    import fsspec, joblib
                    with fsspec.open(model_path, "wb") as _fh:
                        joblib.dump(model, _fh)

                accuracy = accuracy_score(y_test, model.predict(X_test))

                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(col), type=str(ml_df.dtypes[col]))
                    for col in ml_df.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(ml_df)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                    "accuracy": MetadataValue.float(float(accuracy)),
                    "train_rows": MetadataValue.int(len(X_train)),
                    "test_rows": MetadataValue.int(len(X_test)),
                }
                _effective_lineage = column_lineage
                if not _effective_lineage:
                    try:
                        _upstream_cols = set(upstream.columns)
                        _effective_lineage = {
                            col.name: [col.name] for col in _col_schema.columns
                            if col.name in _upstream_cols
                        }
                    except Exception:
                        pass
                if _effective_lineage:
                    _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                    if _upstream_key:
                        _lineage_deps = {}
                        for out_col, in_cols in _effective_lineage.items():
                            _lineage_deps[str(out_col)] = [
                                TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                                for ic in in_cols
                            ]
                        _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
                if include_preview and len(ml_df) > 0:
                    try:
                        _prev = ml_df.sample(min(preview_rows, len(ml_df))) if len(ml_df) > preview_rows * 10 else ml_df.head(preview_rows)
                        _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                    except Exception as _e:
                        context.log.warning(f"preview emission failed: {_e}")
                context.add_output_metadata(_metadata)

                X_full = X if scaler is None else scaler.transform(X)
                ml_df["predicted_class"] = model.predict(X_full)
                if output_probabilities:
                    proba = model.predict_proba(X_full)
                    for i, cls in enumerate(model.classes_):
                        ml_df[f"predicted_proba_{cls}"] = proba[:, i]

                return ml_df

            from dagster import build_column_schema_change_checks
            _schema_checks = build_column_schema_change_checks(assets=[lead_scoring_ml_asset])
            return Definitions(assets=[lead_scoring_ml_asset], asset_checks=list(_schema_checks))

        # ── scoring_method='heuristic' ───────────────────────────────────
        asset_ins = {}
        _required_resource_keys = set()
        _source_inputs: Dict[str, dict] = {}

        for _label, _asset_key, _source in (
            ("lead_data", self.lead_data_asset_key, self.lead_data_source),
            ("behavioral_data", self.behavioral_data_asset_key, self.behavioral_data_source),
            ("company_data", self.company_data_asset_key, self.company_data_source),
        ):
            if _asset_key:
                asset_ins[_label] = AssetIn(key=AssetKey.from_user_string(_asset_key))
            elif _source:
                _source_inputs[_label] = _source
                if _source.get("resource_key"):
                    _required_resource_keys.add(_source["resource_key"])

        _heuristic_kwargs = dict(_base_asset_kwargs)
        _heuristic_kwargs["ins"] = asset_ins
        _heuristic_kwargs["description"] = self.description or "Lead scores with qualification flags (MQL/SQL) and temperature classification"
        if _required_resource_keys:
            _heuristic_kwargs["required_resource_keys"] = _required_resource_keys

        @asset(**_heuristic_kwargs)
        def lead_scoring_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
            # Pull any SQL-sourced inputs that aren't wired through `ins=`.
            for _label, _source in _source_inputs.items():
                inputs[_label] = _ingest_warehouse_query(_source, context)

            # Filter each connected input to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                for _key, _frame in list(inputs.items()):
                    if _frame is None:
                        continue
                    if partition_date_column and partition_date_column in _frame.columns and _date_key:
                        _frame = _frame[_frame[partition_date_column].astype(str) == _date_key]
                    if partition_static_column and partition_static_column in _frame.columns and _static_key:
                        _frame = _frame[_frame[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in _frame.columns and not _is_multi:
                        _frame = _frame[_frame[partition_static_column].astype(str) == str(_pk)]
                    inputs[_key] = _frame
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
                return lead_scores

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

            # Metadata parity with the newer sibling components in this repo
            # (this component previously had none: no row_count, no column
            # schema, no schema-change asset checks, and include_preview_metadata/
            # preview_rows were declared fields that were never actually used).
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(lead_scores.dtypes[col]))
                for col in lead_scores.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(lead_scores)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "avg_lead_score": MetadataValue.float(float(avg_score)),
                "mql_count": MetadataValue.int(int(mql_count)),
                "sql_count": MetadataValue.int(int(sql_count)),
                "hot_lead_count": MetadataValue.int(int(hot_count)),
            }
            if include_preview and len(lead_scores) > 0:
                try:
                    _prev_sorted = lead_scores.sort_values('lead_score', ascending=False)
                    _prev = _prev_sorted.sample(min(preview_rows, len(_prev_sorted))) if len(_prev_sorted) > preview_rows * 10 else _prev_sorted.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return lead_scores

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[lead_scoring_asset])
        return Definitions(assets=[lead_scoring_asset], asset_checks=list(_schema_checks))
