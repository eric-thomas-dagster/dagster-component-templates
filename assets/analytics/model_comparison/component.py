"""Model Comparison.

Train and evaluate multiple sklearn models side-by-side using cross-validation.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

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
    Resolvable,
    asset,
)
from pydantic import Field

_DEFAULT_MODELS = ["logistic_regression", "random_forest", "gradient_boosting", "decision_tree", "naive_bayes"]


@dataclass
class ModelComparisonComponent(Component, Model, Resolvable):
    """Train and evaluate multiple sklearn models side-by-side using cross-validation."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    target_column: str = Field(description="Target column name")
    feature_columns: List[str] = Field(description="Feature columns to use")
    task_type: str = Field(default="classification", description="'classification' or 'regression'")
    models: List[str] = Field(
        default_factory=lambda: list(_DEFAULT_MODELS),
        description="Models to compare: 'logistic_regression', 'random_forest', 'gradient_boosting', 'decision_tree', 'naive_bayes'",
    )
    test_size: float = Field(default=0.2, description="Fraction of data to hold out for evaluation")
    cv_folds: int = Field(default=5, description="Number of cross-validation folds")
    random_state: int = Field(default=42, description="Random seed for reproducibility")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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

    @classmethod
    def get_description(cls) -> str:
        return "Train and evaluate multiple sklearn models side-by-side using cross-validation."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_column = self.target_column
        feature_columns = self.feature_columns
        task_type = self.task_type
        models = self.models
        cv_folds = self.cv_folds
        random_state = self.random_state
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "model_comparison"  # component directory name
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


        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
                from sklearn.model_selection import cross_validate, KFold
                from sklearn.ensemble import (
                    RandomForestClassifier,
                    RandomForestRegressor,
                    GradientBoostingClassifier,
                    GradientBoostingRegressor,
                )
                from sklearn.linear_model import LogisticRegression, LinearRegression
                from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
                from sklearn.naive_bayes import GaussianNB
            except ImportError as e:
                raise ImportError("scikit-learn is required: pip install scikit-learn") from e

            clf_map = {
                "logistic_regression": LogisticRegression,
                "random_forest": RandomForestClassifier,
                "gradient_boosting": GradientBoostingClassifier,
                "decision_tree": DecisionTreeClassifier,
                "naive_bayes": GaussianNB,
            }
            reg_map = {
                "logistic_regression": LinearRegression,
                "random_forest": RandomForestRegressor,
                "gradient_boosting": GradientBoostingRegressor,
                "decision_tree": DecisionTreeRegressor,
                "naive_bayes": None,  # not applicable for regression
            }

            model_map = clf_map if task_type == "classification" else reg_map

            X = upstream[feature_columns].fillna(0)
            y = upstream[target_column]
            cv = KFold(n_splits=cv_folds, shuffle=True, random_state=random_state)

            if task_type == "classification":
                scoring_metrics = ["accuracy", "f1_weighted", "precision_weighted", "recall_weighted"]
            else:
                scoring_metrics = ["r2", "neg_mean_absolute_error"]

            rows = []
            for model_name in models:
                if model_name not in model_map:
                    context.log.warning(f"Unknown model '{model_name}', skipping.")
                    continue
                model_cls = model_map[model_name]
                if model_cls is None:
                    context.log.warning(f"'{model_name}' is not supported for task_type='regression', skipping.")
                    continue

                try:
                    try:
                        model = model_cls(random_state=random_state)
                    except TypeError:
                        model = model_cls()

                    scores = cross_validate(
                        model, X, y,
                        cv=cv,
                        scoring=scoring_metrics,
                        return_train_score=False,
                    )

                    row = {"model": model_name, "fit_time_mean": scores["fit_time"].mean()}
                    for metric in scoring_metrics:
                        key = f"test_{metric}"
                        row[metric] = scores[key].mean()
                    rows.append(row)
                except Exception as exc:
                    context.log.warning(f"Failed to evaluate '{model_name}': {exc}")

            if not rows:
                raise ValueError("No models were successfully evaluated.")

            result = pd.DataFrame(rows)
            primary_metric = scoring_metrics[0]
            result = result.sort_values(primary_metric, ascending=False).reset_index(drop=True)

            best_model = result.iloc[0]["model"]
            best_score = result.iloc[0][primary_metric]

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result.dtypes[col]))
                for col in result.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            return result

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
