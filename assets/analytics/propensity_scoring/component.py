"""Propensity Scoring Component.

Calculate propensity scores for various customer actions (purchase, upgrade, referral,
engagement). `scoring_method='heuristic'` (default): the original behavior-pattern
scoring, unchanged. `scoring_method='ml'`: fits a real scikit-learn classifier against
a `target_column` you supply (e.g. a historical `converted`/`did_upgrade`/`referred`
boolean) -- none of the four propensity types are fit against any observed outcome
today, so 'ml' mode requires you bring one.
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
    Output,
)
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
# as logistic_regression_model -- any of the 4 propensity_type outcomes
# (purchase/upgrade/referral/engagement) is a binary classification task
# once you have a real `target_column`, same as any other.
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


class PropensityScoringComponent(Component, Model, Resolvable):
    """Component for calculating customer propensity scores.

    Calculate likelihood scores for customer actions:
    - **Purchase Propensity**: Likelihood to make next purchase
    - **Upgrade Propensity**: Likelihood to upgrade tier/plan
    - **Referral Propensity**: Likelihood to refer others
    - **Engagement Propensity**: Likelihood to engage with content

    Uses heuristic scoring based on:
    - Recent activity levels
    - Historical behavior patterns
    - Engagement metrics
    - RFM characteristics

    Example:
        ```yaml
        type: dagster_component_templates.PropensityScoringComponent
        attributes:
          asset_name: customer_propensity_scores
          upstream_asset_key: customer_behavior
          propensity_type: purchase
          scoring_window_days: 90
          description: "Customer purchase propensity scores"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame with customer behavior data. Mutually exclusive with `source` -- set exactly one."
    )

    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Also required (with execution_mode='sql') to name the FROM-source for the "
            "server-side training/prediction query. Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

    scoring_method: str = Field(
        default="heuristic",
        description=(
            "'heuristic' (default): the original behavior-pattern scoring below, "
            "unchanged. 'ml': fits a real scikit-learn classifier against a "
            "`target_column` you supply. None of the 4 propensity_type formulas are "
            "fit against any observed outcome today -- 'ml' mode requires you bring "
            "one (e.g. a historical `converted`/`did_upgrade`/`referred` boolean), "
            "and requires `target_column` + `feature_columns`."
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
        description="Required when scoring_method='ml'. Column name of the historical outcome label (e.g. 'converted') -- this does NOT exist in the heuristic's input schema; you must supply it.",
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

    propensity_type: str = Field(
        default="purchase",
        description="scoring_method='heuristic' only. Type of propensity: purchase, upgrade, referral, engagement"
    )

    scoring_window_days: int = Field(
        default=90,
        description="scoring_method='heuristic' only. Days of historical data to use for scoring. NOTE: accepted but not currently used by the heuristic scoring math (pre-existing, documented not fixed -- see README)."
    )

    score_threshold_high: float = Field(
        default=70.0,
        description="Score threshold for 'high propensity' classification"
    )

    score_threshold_medium: float = Field(
        default=40.0,
        description="Score threshold for 'medium propensity' classification"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected)"
    )

    last_activity_field: Optional[str] = Field(
        default=None,
        description="Last activity date column (auto-detected)"
    )

    activity_count_field: Optional[str] = Field(
        default=None,
        description="Activity count column (auto-detected)"
    )

    engagement_score_field: Optional[str] = Field(
        default=None,
        description="Engagement score column (optional)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="customer_analytics",
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
        upstream_asset_key = self.upstream_asset_key
        source_cfg = self.source
        scoring_method = self.scoring_method
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
        propensity_type = self.propensity_type
        scoring_window = self.scoring_window_days
        threshold_high = self.score_threshold_high
        threshold_medium = self.score_threshold_medium
        customer_id_field = self.customer_id_field
        last_activity_field = self.last_activity_field
        activity_count_field = self.activity_count_field
        engagement_score_field = self.engagement_score_field
        description = self.description or f"Customer {propensity_type} propensity scores"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        if bool(upstream_asset_key) == bool(source_cfg):
            raise ValueError("PropensityScoringComponent: set exactly one of `upstream_asset_key` or `source`.")
        if scoring_method not in ("heuristic", "ml"):
            raise ValueError(f"PropensityScoringComponent: scoring_method must be 'heuristic' or 'ml', got {scoring_method!r}.")
        if execution_mode not in ("python", "sql"):
            raise ValueError(f"PropensityScoringComponent: execution_mode must be 'python' or 'sql', got {execution_mode!r}.")
        if execution_mode == "sql" and scoring_method != "ml":
            raise ValueError("PropensityScoringComponent: execution_mode='sql' requires scoring_method='ml' (there is no SQL-mode heuristic).")
        if scoring_method == "ml":
            if not target_column:
                raise ValueError("PropensityScoringComponent: scoring_method='ml' requires `target_column` (a historical outcome label the heuristic never needed).")
            if not feature_columns:
                raise ValueError("PropensityScoringComponent: scoring_method='ml' requires `feature_columns`.")
        if execution_mode == "sql":
            if not source_cfg:
                raise ValueError("PropensityScoringComponent: execution_mode='sql' requires `source` (a SQL FROM-source).")
            if sql_dialect not in _SQL_MODEL_DIALECTS:
                raise ValueError(f"PropensityScoringComponent: execution_mode='sql' requires sql_dialect to be one of {_SQL_MODEL_DIALECTS}.")
            if not output_table:
                raise ValueError("PropensityScoringComponent: execution_mode='sql' requires `output_table`.")
            if not model_name:
                raise ValueError("PropensityScoringComponent: execution_mode='sql' requires `model_name`.")

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
        _comp_name = "propensity_scoring"  # component directory name
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



        _asset_kwargs: Dict[str, Any] = dict(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        if upstream_asset_key:
            _asset_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        if source_cfg and source_cfg.get("resource_key"):
            _asset_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

        if execution_mode == "sql":
            @asset(**_asset_kwargs)
            def _sql_asset(context: AssetExecutionContext):
                statements = _build_sql_mode_statements(
                    sql_dialect, source_cfg["sql"], output_table, model_name,
                    target_column, list(feature_columns), test_size, max_iter,
                )
                return _run_sql_mode(context, source_cfg, statements, output_table)

            return Definitions(assets=[_sql_asset])

        @asset(**_asset_kwargs)
        def propensity_scoring_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(source_cfg, context)
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]

            if scoring_method == "ml":
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
                        f"propensity_scoring (ml): only {len(X)} rows available; skipping "
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
                context.add_output_metadata(_metadata)

                X_full = X if scaler is None else scaler.transform(X)
                ml_df["predicted_class"] = model.predict(X_full)
                if output_probabilities:
                    proba = model.predict_proba(X_full)
                    for i, cls in enumerate(model.classes_):
                        ml_df[f"predicted_proba_{cls}"] = proba[:, i]

                return ml_df

            """Asset that calculates customer propensity scores (heuristic scoring)."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} customer records for propensity scoring")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            customer_col = find_column(
                ['customer_id', 'user_id', 'customerId', 'userId', 'id'],
                customer_id_field
            )
            last_activity_col = find_column(
                ['last_activity_date', 'last_activity', 'last_seen', 'last_purchase_date'],
                last_activity_field
            )
            activity_count_col = find_column(
                ['activity_count', 'total_activities', 'event_count', 'interactions'],
                activity_count_field
            )
            engagement_col = find_column(
                ['engagement_score', 'engagement', 'activity_score'],
                engagement_score_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not last_activity_col:
                missing.append("last_activity_date")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Last Activity: {last_activity_col}")

            # Prepare data
            cols_to_use = [customer_col, last_activity_col]
            col_names = ['customer_id', 'last_activity_date']

            if activity_count_col:
                cols_to_use.append(activity_count_col)
                col_names.append('activity_count')

            if engagement_col:
                cols_to_use.append(engagement_col)
                col_names.append('engagement_score')

            propensity_df = df[cols_to_use].copy()
            propensity_df.columns = col_names

            # Parse dates
            propensity_df['last_activity_date'] = pd.to_datetime(propensity_df['last_activity_date'], errors='coerce')
            propensity_df = propensity_df.dropna(subset=['last_activity_date'])

            # Calculate days since last activity
            current_date = pd.Timestamp.now()
            propensity_df['days_since_activity'] = (current_date - propensity_df['last_activity_date']).dt.days

            context.log.info(f"Calculating {propensity_type} propensity for {len(propensity_df)} customers")

            # Calculate propensity score based on type
            if propensity_type == 'purchase':
                # Purchase propensity based on recency and frequency
                # Recency score (0-40 points): Recent activity = high score
                propensity_df['recency_score'] = propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 40 - (days / 7) * 5)  # Decay 5 points per week
                ).clip(0, 40)

                # Frequency score (0-40 points)
                if 'activity_count' in propensity_df.columns:
                    max_activities = propensity_df['activity_count'].quantile(0.95)
                    propensity_df['frequency_score'] = (
                        propensity_df['activity_count'] / max_activities * 40
                    ).clip(0, 40)
                else:
                    propensity_df['frequency_score'] = 20  # Default mid-range

                # Engagement score (0-20 points)
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['engagement_component'] = (
                            propensity_df['engagement_score'] / max_engagement * 20
                        ).clip(0, 20)
                    else:
                        propensity_df['engagement_component'] = 10
                else:
                    propensity_df['engagement_component'] = 10  # Default mid-range

                # Total propensity score (0-100)
                propensity_df['propensity_score'] = (
                    propensity_df['recency_score'] +
                    propensity_df['frequency_score'] +
                    propensity_df['engagement_component']
                ).round(2)

            elif propensity_type == 'upgrade':
                # Upgrade propensity based on engagement and activity growth
                # High engagement = high upgrade propensity
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['propensity_score'] = (
                            propensity_df['engagement_score'] / max_engagement * 70
                        ).clip(0, 70)
                    else:
                        propensity_df['propensity_score'] = 35
                else:
                    propensity_df['propensity_score'] = 35

                # Boost for recent activity
                propensity_df['propensity_score'] += propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 30 - days)  # Up to 30 points for very recent
                ).clip(0, 30)

                propensity_df['propensity_score'] = propensity_df['propensity_score'].round(2).clip(0, 100)

            elif propensity_type == 'referral':
                # Referral propensity based on engagement and satisfaction indicators
                # Highly engaged customers are more likely to refer
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['propensity_score'] = (
                            propensity_df['engagement_score'] / max_engagement * 60
                        ).clip(0, 60)
                    else:
                        propensity_df['propensity_score'] = 30
                else:
                    propensity_df['propensity_score'] = 30

                # Boost for moderate recency (not too new, not too old)
                propensity_df['propensity_score'] += propensity_df['days_since_activity'].apply(
                    lambda days: 40 if 7 <= days <= 60 else (20 if days < 7 else max(0, 40 - (days - 60) / 10))
                ).clip(0, 40)

                propensity_df['propensity_score'] = propensity_df['propensity_score'].round(2).clip(0, 100)

            elif propensity_type == 'engagement':
                # Engagement propensity based on recent activity patterns
                # Recent and frequent activity = high engagement propensity
                propensity_df['recency_score'] = propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 50 - days)  # 50 points max, decays daily
                ).clip(0, 50)

                if 'activity_count' in propensity_df.columns:
                    max_activities = propensity_df['activity_count'].quantile(0.95)
                    propensity_df['frequency_score'] = (
                        propensity_df['activity_count'] / max_activities * 50
                    ).clip(0, 50)
                else:
                    propensity_df['frequency_score'] = 25

                propensity_df['propensity_score'] = (
                    propensity_df['recency_score'] + propensity_df['frequency_score']
                ).round(2).clip(0, 100)

            else:
                context.log.error(f"Unknown propensity type: {propensity_type}")
                return pd.DataFrame()

            # Classify propensity level
            def classify_propensity(score):
                if score >= threshold_high:
                    return 'High'
                elif score >= threshold_medium:
                    return 'Medium'
                else:
                    return 'Low'

            propensity_df['propensity_level'] = propensity_df['propensity_score'].apply(classify_propensity)

            # Select output columns
            output_cols = [
                'customer_id',
                'propensity_score',
                'propensity_level',
                'days_since_activity'
            ]

            result_df = propensity_df[output_cols].copy()

            context.log.info(f"Propensity scoring complete: {len(result_df)} customers scored")

            # Log propensity distribution
            level_dist = result_df['propensity_level'].value_counts()
            context.log.info("\nPropensity Level Distribution:")
            for level, count in level_dist.items():
                pct = round(count / len(result_df) * 100, 1)
                avg_score = round(result_df[result_df['propensity_level'] == level]['propensity_score'].mean(), 2)
                context.log.info(f"  {level}: {count} customers ({pct}%), avg score: {avg_score}")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "propensity_type": propensity_type,
                "avg_propensity_score": round(float(result_df['propensity_score'].mean()), 2),
                "high_propensity_count": int(level_dist.get('High', 0)),
                "medium_propensity_count": int(level_dist.get('Medium', 0)),
                "low_propensity_count": int(level_dist.get('Low', 0)),
                "scoring_window_days": scoring_window,
            }

            # Return with metadata
            if include_preview and len(result_df) > 0:
                # Sort by propensity score descending
                result_sorted = result_df.sort_values('propensity_score', ascending=False)

                _prev = result_sorted.sample(preview_rows) if len(result_sorted) > preview_rows * 10 else result_sorted.head(preview_rows)
                metadata['preview'] = MetadataValue.md(_prev.to_markdown(index=False))
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
            context.add_output_metadata(_metadata)
            return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[propensity_scoring_asset])


        return Definitions(assets=[propensity_scoring_asset], asset_checks=list(_schema_checks))
