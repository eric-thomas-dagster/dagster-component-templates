"""Naive Bayes Model.

Fit a Gaussian Naive Bayes classifier (sklearn `GaussianNB`). **Classification only** — sklearn doesn't ship a Gaussian-NB regressor; setting `task_type=regression` raises.
"""
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
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


# ── execution_mode='sql' ────────────────────────────────────────────────
# Databricks-ONLY, and predict-only -- unlike every other component in this
# family, BigQuery and Snowflake are deliberately NOT offered here, for the
# same reason `decision_tree_model` was skipped entirely from this whole
# initiative: neither has a genuine per-algorithm match for Gaussian Naive
# Bayes. BigQuery ML has no NAIVE_BAYES model type at all. Snowflake's
# `SNOWFLAKE.ML.CLASSIFICATION` is AutoML -- it picks its own algorithm
# internally and never guarantees (or even exposes) Naive Bayes specifically.
# Offering either as "train a Naive Bayes here" would be the same misleading
# "exact equivalent" framing this session's discipline explicitly avoids.
#   - databricks: predict-only via `ai_query('served_endpoint', ...)` against
#     an already-served Model Serving endpoint -- honest regardless of
#     algorithm, since this branch never claims to train or BE any specific
#     algorithm in every other component either; it just calls whatever was
#     already deployed.
# validation.level: code -- no live Databricks workspace in this dev
# environment; the generated SQL is asserted structurally, not executed.

_SQL_MODEL_DIALECTS = ("databricks",)


def _build_sql_mode_statements(
    dialect: str, source_sql: str, output_table: str, model_name: str,
    feature_columns: List[str],
) -> List[str]:
    if dialect not in _SQL_MODEL_DIALECTS:
        raise ValueError(
            f"unsupported sql_dialect: {dialect!r}. Valid: {_SQL_MODEL_DIALECTS} "
            f"(BigQuery ML has no NAIVE_BAYES model type; Snowflake's "
            f"SNOWFLAKE.ML.CLASSIFICATION is AutoML and never guarantees Naive "
            f"Bayes specifically -- neither is a genuine per-algorithm match, "
            f"same reasoning decision_tree_model was skipped for entirely)."
        )

    # databricks: predict-only against an already-served endpoint (model_name
    # is that endpoint's name here, not something this statement creates).
    feat_struct = ", ".join(f"'{c}', src.{c}" for c in feature_columns)
    return [
        f"CREATE OR REPLACE TABLE {output_table} AS\n"
        f"SELECT src.*, ai_query('{model_name}', named_struct({feat_struct})) AS predicted\n"
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


class NaiveBayesModelComponent(Component, Model, Resolvable):
    """Fit a Gaussian Naive Bayes classifier. Classification only (no regression variant)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Also required (with execution_mode='sql') to name the FROM-source for the "
            "server-side prediction query. Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    execution_mode: str = Field(
        default="python",
        description=(
            "'python' (default): fits a real scikit-learn GaussianNB locally. "
            "'sql': predicts server-side via Databricks `ai_query()` against an "
            "already-served Model Serving endpoint -- Databricks-ONLY and "
            "predict-only (trains nothing), since BigQuery ML has no NAIVE_BAYES "
            "model type and Snowflake's ML.CLASSIFICATION is AutoML that never "
            "guarantees Naive Bayes specifically. Requires `source`, "
            "`sql_dialect='databricks'`, `output_table`, and `model_name` (the "
            "endpoint name)."
        ),
    )
    sql_dialect: Optional[str] = Field(
        default=None,
        description=f"Required when execution_mode='sql'. Only {_SQL_MODEL_DIALECTS} is offered.",
    )
    output_table: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. Destination table the predictions are written to.",
    )
    model_name: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. The name of an already-served Databricks Model Serving endpoint -- this dialect trains nothing.",
    )
    target_column: Union[str, int] = Field(description="Column name of the target variable")
    feature_columns: List[Union[str, int]] = Field(description="List of column names to use as features")
    task_type: str = Field(default="classification", description="Task type: 'classification' or 'regression'. GaussianNB has no regression variant -- 'regression' always raises.")
    test_size: float = Field(default=0.2, description="Fraction of data to hold out for evaluation")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    model_path: Optional[str] = Field(
        default=None,
        description=(
            "If set, joblib-dump the trained model to this path after fit. "
            "Supports local paths and any fsspec URL (s3://, gs://, abfs://). "
            "Downstream `model_score` component loads this path to predict on "
            "new data — closes the train-once / score-later loop."
        ),
    )
    output_predictions: bool = Field(default=True, description="Add a `predicted` column to the output")
    output_probabilities: bool = Field(default=True, description="Add `predicted_proba_<class>` columns per class")
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

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Fit a Gaussian Naive Bayes classifier. Classification only (no regression variant)."

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


    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        source_cfg = self.source
        execution_mode = self.execution_mode
        sql_dialect = self.sql_dialect
        output_table = self.output_table
        model_name = self.model_name
        target_column = self.target_column
        feature_columns = self.feature_columns
        model_path = self.model_path
        task_type = self.task_type
        test_size = self.test_size
        random_state = self.random_state
        output_predictions = self.output_predictions
        output_probabilities = self.output_probabilities
        group_name = self.group_name

        if bool(upstream_asset_key) == bool(source_cfg):
            raise ValueError("NaiveBayesModelComponent: set exactly one of `upstream_asset_key` or `source`.")
        if execution_mode not in ("python", "sql"):
            raise ValueError(f"NaiveBayesModelComponent: execution_mode must be 'python' or 'sql', got {execution_mode!r}.")
        if execution_mode == "sql":
            if not source_cfg:
                raise ValueError("NaiveBayesModelComponent: execution_mode='sql' requires `source` (a SQL FROM-source).")
            if sql_dialect not in _SQL_MODEL_DIALECTS:
                raise ValueError(f"NaiveBayesModelComponent: execution_mode='sql' requires sql_dialect to be one of {_SQL_MODEL_DIALECTS}.")
            if not output_table:
                raise ValueError("NaiveBayesModelComponent: execution_mode='sql' requires `output_table`.")
            if not model_name:
                raise ValueError("NaiveBayesModelComponent: execution_mode='sql' requires `model_name`.")
            if not feature_columns:
                raise ValueError("NaiveBayesModelComponent: execution_mode='sql' requires `feature_columns`.")

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
        _comp_name = "naive_bayes_model"  # component directory name
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



        if task_type not in ("classification", "regression"):
            raise ValueError(f"NaiveBayesModelComponent: unknown task_type: {task_type!r}. Use 'classification' or 'regression'.")
        if task_type == "regression":
            raise ValueError(
                'naive_bayes_model only supports classification — '
                'Gaussian Naive Bayes has no regression variant in sklearn. '
                'Set task_type="classification".'
            )

        _asset_kwargs: Dict[str, Any] = dict(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
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
                    list(feature_columns),
                )
                return _run_sql_mode(context, source_cfg, statements, output_table)

            return Definitions(assets=[_sql_asset])

        @asset(**_asset_kwargs)
        def _asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(source_cfg, context)
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
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
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                from sklearn.naive_bayes import GaussianNB
                from sklearn.metrics import accuracy_score
                from sklearn.model_selection import train_test_split
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            df = upstream.copy()
            X = df[feature_columns].fillna(0)
            y = df[target_column]

            if len(X) < 5:
                context.log.warning(
                    f"naive_bayes_model: only {len(X)} rows available; skipping "
                    "train/test split (whole frame used for both fit and eval)."
                )
                X_train = X_test = X
                y_train = y_test = y
            else:
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=test_size, random_state=random_state
                )

            model = GaussianNB()
            model.fit(X_train, y_train)
            if model_path is not None:
                import fsspec, joblib
                with fsspec.open(model_path, "wb") as _fh:
                    joblib.dump(model, _fh)
            accuracy = accuracy_score(y_test, model.predict(X_test))

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
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
            if self.include_preview_metadata and len(df) > 0:
                try:
                    _preview_rows = self.preview_rows
                    _prev = df.sample(min(_preview_rows, len(df))) if len(df) > _preview_rows * 10 else df.head(_preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            if output_predictions:
                df["predicted"] = model.predict(X)
            if output_probabilities:
                proba = model.predict_proba(X)
                for i, cls in enumerate(model.classes_):
                    df[f"predicted_proba_{cls}"] = proba[:, i]

            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
