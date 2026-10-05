"""Customer Health Score Component.

Calculates customer health scores by analyzing engagement, product usage,
subscription status, and support interactions to predict churn risk and
identify expansion opportunities.

`scoring_method='heuristic'` (default): the original 4-way join (customer_data +
subscription_data + product_usage + support_tickets, each independently
optional) and hand-tuned weighted scoring, unchanged in spirit -- but see the
`_normalize_score` fix note below. `scoring_method='ml'`: fits a real
scikit-learn classifier against a `target_column` you supply (e.g. a
historical `churned`/`expanded` boolean) over a single, already-joined
`upstream_asset_key`/`source` table -- `is_churn_risk`/`is_expansion_opportunity`
are threshold-derived from the same heuristic, not fit against any observed
outcome, so 'ml' mode requires you bring your own label and feature table.
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
        if hasattr(resource, "get_client"):
            # "get_client" means something different per vendor -- there's no
            # universal calling convention, so dispatch on the CLIENT's own
            # shape rather than assume one. Verified against the real APIs,
            # not guessed:
            client = resource.get_client()
            if hasattr(client, "query"):
                # BigQuery (google.cloud.bigquery.Client): .query(sql) returns
                # a QueryJob; .to_dataframe() blocks until done and returns a
                # pandas DataFrame directly -- no .result() call needed first.
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                # Redshift Data API (dagster_aws RedshiftClient):
                # execute_query(sql, fetch_results=True) returns bare
                # List[Tuple] with NO column names attached -- a
                # RealDictCursor factory is required to get dict rows a
                # DataFrame can use with correct column names.
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
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
# as logistic_regression_model -- churn/expansion is a binary classification
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


class CustomerHealthScoreComponent(Component, Model, Resolvable):
    """Component that calculates customer health scores from multiple data sources."""

    asset_name: str = Field(
        ...,
        description="Name of the customer health score asset to create",
    )

    # ── scoring_method='heuristic' (default): 4-way join, each independently optional ──
    customer_data_asset_key: Optional[str] = Field(
        default=None,
        description="Customer data asset (CRM, user profiles, etc.). Mutually exclusive with `customer_data_source` -- set at most one.",
    )
    customer_data_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull customer data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `customer_data_asset_key` -- set at most one.",
    )

    subscription_data_asset_key: Optional[str] = Field(
        default=None,
        description="Subscription/billing data asset. Mutually exclusive with `subscription_data_source` -- set at most one.",
    )
    subscription_data_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull subscription/billing data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `subscription_data_asset_key` -- set at most one.",
    )

    product_usage_asset_key: Optional[str] = Field(
        default=None,
        description="Product usage/activity data asset. Mutually exclusive with `product_usage_source` -- set at most one.",
    )
    product_usage_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull product usage/activity data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `product_usage_asset_key` -- set at most one.",
    )

    support_ticket_asset_key: Optional[str] = Field(
        default=None,
        description="Support ticket data asset. Mutually exclusive with `support_ticket_source` -- set at most one.",
    )
    support_ticket_source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pull support ticket data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `support_ticket_asset_key` -- set at most one.",
    )

    scoring_method: str = Field(
        default="heuristic",
        description=(
            "'heuristic' (default): the 4-way join above and hand-tuned weighted scoring "
            "below. 'ml': fits a real scikit-learn classifier against a `target_column` you "
            "supply over a single, already-joined `upstream_asset_key`/`source` table -- "
            "`is_churn_risk`/`is_expansion_opportunity` are threshold-derived from the same "
            "heuristic below, not fit against any observed outcome, so 'ml' mode requires "
            "you bring your own label and cannot be combined with the 4-way join fields "
            "above."
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
            "scoring_method='ml' only. Pull rows directly via SQL instead of from an "
            "upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR "
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
        description="Required when scoring_method='ml'. Column name of the historical outcome label (e.g. 'churned' or 'expanded') -- this does NOT exist in the heuristic's input schema; you must supply it.",
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
    analysis_period_days: int = Field(
        default=30,
        description="scoring_method='heuristic' only. Number of days to analyze for health calculation. NOTE: accepted but not currently used by the scoring math (pre-existing, documented not fixed -- see README).",
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
        description="scoring_method='heuristic' only. Calculate health score trend (requires historical data). NOTE: accepted but not currently implemented anywhere -- no trend-calculation code exists (pre-existing, documented not fixed -- see README).",
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
        but all 5 call sites (in `_calculate_engagement_score`,
        `_calculate_product_usage_score`, `_calculate_payment_health_score`)
        pass a full pandas Series -- `pd.isna()` on a multi-element Series
        returns a Series of bools, and `if <Series>:` raises `ValueError: The
        truth value of a Series is ambiguous` for any real input with more
        than one row. Same bug, same fix, as lead_scoring's `_normalize_score`
        (same original template, confirmed both were broken independently).
        Rewritten to be genuinely vectorized.
        """
        if max_val == min_val:
            return pd.Series(50.0, index=value.index)

        normalized = ((value - min_val) / (max_val - min_val)) * 100
        return normalized.fillna(50.0).clip(self.min_health_score, self.max_health_score)

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
            raise ValueError(f"CustomerHealthScoreComponent: scoring_method must be 'heuristic' or 'ml', got {scoring_method!r}.")
        if execution_mode not in ("python", "sql"):
            raise ValueError(f"CustomerHealthScoreComponent: execution_mode must be 'python' or 'sql', got {execution_mode!r}.")

        _multi_input_fields_set = any([
            self.customer_data_asset_key, self.customer_data_source,
            self.subscription_data_asset_key, self.subscription_data_source,
            self.product_usage_asset_key, self.product_usage_source,
            self.support_ticket_asset_key, self.support_ticket_source,
        ])

        if scoring_method == "ml":
            if _multi_input_fields_set:
                raise ValueError(
                    "CustomerHealthScoreComponent: scoring_method='ml' cannot be combined "
                    "with the 4-way join fields (customer_data_asset_key/_source, "
                    "subscription_data_asset_key/_source, product_usage_asset_key/_source, "
                    "support_ticket_asset_key/_source). Use `upstream_asset_key`/`source` to "
                    "point at a single, already-joined feature+label table instead."
                )
            if bool(upstream_asset_key) == bool(source_cfg):
                raise ValueError("CustomerHealthScoreComponent: scoring_method='ml' requires exactly one of `upstream_asset_key` or `source`.")
            if not target_column:
                raise ValueError("CustomerHealthScoreComponent: scoring_method='ml' requires `target_column` (a historical outcome label the heuristic never needed).")
            if not feature_columns:
                raise ValueError("CustomerHealthScoreComponent: scoring_method='ml' requires `feature_columns`.")
            if execution_mode == "sql":
                if not source_cfg:
                    raise ValueError("CustomerHealthScoreComponent: execution_mode='sql' requires `source` (a SQL FROM-source).")
                if sql_dialect not in _SQL_MODEL_DIALECTS:
                    raise ValueError(f"CustomerHealthScoreComponent: execution_mode='sql' requires sql_dialect to be one of {_SQL_MODEL_DIALECTS}.")
                if not output_table:
                    raise ValueError("CustomerHealthScoreComponent: execution_mode='sql' requires `output_table`.")
                if not model_name:
                    raise ValueError("CustomerHealthScoreComponent: execution_mode='sql' requires `model_name`.")
        else:
            if upstream_asset_key or source_cfg or target_column or feature_columns:
                raise ValueError(
                    "CustomerHealthScoreComponent: `upstream_asset_key`/`source`/"
                    "`target_column`/`feature_columns` require scoring_method='ml'."
                )
            if execution_mode == "sql":
                raise ValueError("CustomerHealthScoreComponent: execution_mode='sql' requires scoring_method='ml' (there is no SQL-mode heuristic).")
            for _label, _asset_key, _source in (
                ("customer_data", self.customer_data_asset_key, self.customer_data_source),
                ("subscription_data", self.subscription_data_asset_key, self.subscription_data_source),
                ("product_usage", self.product_usage_asset_key, self.product_usage_source),
                ("support_ticket", self.support_ticket_asset_key, self.support_ticket_source),
            ):
                if _asset_key and _source:
                    raise ValueError(f"CustomerHealthScoreComponent: set at most one of `{_label}_asset_key` or `{_label}_source`.")
            if not _multi_input_fields_set:
                raise ValueError(
                    "CustomerHealthScoreComponent: at least one of customer_data, "
                    "subscription_data, product_usage, or support_ticket must be connected "
                    "(via *_asset_key or *_source)."
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
        _comp_name = "customer_health_score"  # component directory name
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
            _ml_kwargs["description"] = self.description or "Customer churn/expansion predictions from a trained classifier"
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
            def customer_health_ml_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
                        f"customer_health_score (ml): only {len(X)} rows available; "
                        "skipping train/test split (whole frame used for both fit and eval)."
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
            _schema_checks = build_column_schema_change_checks(assets=[customer_health_ml_asset])
            return Definitions(assets=[customer_health_ml_asset], asset_checks=list(_schema_checks))

        # ── scoring_method='heuristic' ───────────────────────────────────
        asset_ins = {}
        _required_resource_keys = set()
        _source_inputs: Dict[str, dict] = {}

        for _label, _input_key, _asset_key, _source in (
            ("customer_data", "customer_data", self.customer_data_asset_key, self.customer_data_source),
            ("subscription_data", "subscription_data", self.subscription_data_asset_key, self.subscription_data_source),
            ("product_usage", "product_usage", self.product_usage_asset_key, self.product_usage_source),
            ("support_ticket", "support_tickets", self.support_ticket_asset_key, self.support_ticket_source),
        ):
            if _asset_key:
                asset_ins[_input_key] = AssetIn(key=AssetKey.from_user_string(_asset_key))
            elif _source:
                _source_inputs[_input_key] = _source
                if _source.get("resource_key"):
                    _required_resource_keys.add(_source["resource_key"])

        _heuristic_kwargs = dict(_base_asset_kwargs)
        _heuristic_kwargs["ins"] = asset_ins
        _heuristic_kwargs["description"] = self.description or "Customer health scores with churn risk and expansion opportunity flags"
        if _required_resource_keys:
            _heuristic_kwargs["required_resource_keys"] = _required_resource_keys

        @asset(**_heuristic_kwargs)
        def customer_health_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
            # Pull any SQL-sourced inputs that aren't wired through `ins=`.
            for _input_key, _source in _source_inputs.items():
                inputs[_input_key] = _ingest_warehouse_query(_source, context)

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
                return health_scores

            # Summary statistics
            total_customers = len(health_scores)
            avg_score = health_scores['health_score'].mean()
            churn_risk_count = health_scores['is_churn_risk'].sum()
            expansion_count = health_scores['is_expansion_opportunity'].sum()

            context.log.info(f"✓ Calculated health scores for {total_customers} customers")
            context.log.info(f"  Average health score: {avg_score:.1f}")
            context.log.info(f"  Churn risk: {churn_risk_count} customers")
            context.log.info(f"  Expansion opportunities: {expansion_count} customers")

            # Metadata parity with the newer sibling components in this repo
            # (this component previously had none: no row_count, no column
            # schema, no schema-change asset checks, and include_preview_metadata/
            # preview_rows were declared fields that were never actually used).
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(health_scores.dtypes[col]))
                for col in health_scores.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(health_scores)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "avg_health_score": MetadataValue.float(float(avg_score)),
                "churn_risk_count": MetadataValue.int(int(churn_risk_count)),
                "expansion_opportunity_count": MetadataValue.int(int(expansion_count)),
            }
            if include_preview and len(health_scores) > 0:
                try:
                    _prev_sorted = health_scores.sort_values('health_score', ascending=True)
                    _prev = _prev_sorted.sample(min(preview_rows, len(_prev_sorted))) if len(_prev_sorted) > preview_rows * 10 else _prev_sorted.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return health_scores

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[customer_health_asset])
        return Definitions(assets=[customer_health_asset], asset_checks=list(_schema_checks))
