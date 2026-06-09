"""BigQueryMLTrainAssetComponent — train a BQML model in pure SQL.

Uses BigQuery ML's `CREATE OR REPLACE MODEL ... AS SELECT ...` to train
a model entirely inside BigQuery — no Python ML stack, no scikit-learn,
no PyTorch. Supports linear/logistic regression, ARIMA forecasting,
KMEANS clustering, DNN classifiers, boosted trees, time-series
anomaly detection, matrix factorization, and more.

Asset's stored value is a small DataFrame with the trained model's
training metrics (loss, R², AUC, etc.) so downstream assets can gate
on model quality.
"""

import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
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


class BigQueryMLTrainAssetComponent(Component, Model, Resolvable):
    """Train a BigQuery ML model from a SELECT statement.

    Maps to:
        CREATE OR REPLACE MODEL `<destination_model_id>`
          OPTIONS(<model_options>)
          AS <select_query>

    BQML model types (set via model_options.MODEL_TYPE):
      LINEAR_REG / LOGISTIC_REG / KMEANS / ARIMA_PLUS / ARIMA_PLUS_XREG /
      DNN_REGRESSOR / DNN_CLASSIFIER / BOOSTED_TREE_REGRESSOR /
      BOOSTED_TREE_CLASSIFIER / RANDOM_FOREST_REGRESSOR / RANDOM_FOREST_CLASSIFIER /
      MATRIX_FACTORIZATION / AUTOENCODER / PCA / AUTOML_REGRESSOR /
      AUTOML_CLASSIFIER / TENSORFLOW (import a saved model)
    """

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="Billing project. Defaults to the SA's project.")
    location: Optional[str] = Field(default=None, description="BQ location. Default: auto-detect.")

    destination_model_id: str = Field(
        description="Fully-qualified BQML model id, e.g. `my-project.ml.my_arima_model`.",
    )
    model_options: Dict[str, Any] = Field(
        description=(
            "OPTIONS clause as a dict. MUST include `MODEL_TYPE`. Example: "
            "{model_type: 'LINEAR_REG', input_label_cols: ['fare_amount'], "
            "data_split_method: 'AUTO_SPLIT'}."
        ),
    )
    select_query: str = Field(
        description="SELECT statement that supplies training data. Supports {placeholder} substitution.",
    )
    query_params: Dict[str, Any] = Field(default_factory=dict)

    deps: Optional[List[str]] = Field(default=None)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError(
                "BigQueryMLTrainAssetComponent: provide credentials, credentials_path, "
                "or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        destination_model_id = self.destination_model_id
        model_options = dict(self.model_options or {})
        select_query = self.select_query
        query_params = dict(self.query_params or {})

        if not any(k.lower() == "model_type" for k in model_options.keys()):
            raise ValueError(
                "BigQueryMLTrainAssetComponent: model_options must include `MODEL_TYPE`. "
                "E.g. {model_type: 'LINEAR_REG'}."
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"BigQuery ML model: {destination_model_id}",
            group_name=self.group_name,
            kinds={"bigquery", "bqml", "ml"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(credentials=sa_creds, project=project_id, location=location)

            try:
                rendered_select = select_query.format(**query_params) if query_params else select_query
            except KeyError as e:
                raise ValueError(f"select_query references missing placeholder {e}; available: {list(query_params.keys())}")

            # Render OPTIONS clause. BQML expects unquoted identifiers and
            # quoted strings, e.g. OPTIONS(model_type='LINEAR_REG', max_iterations=50).
            opts_parts = []
            for k, v in model_options.items():
                if isinstance(v, str):
                    opts_parts.append(f"{k}={json.dumps(v).replace(chr(34), chr(39))}")
                elif isinstance(v, list):
                    items = ", ".join(json.dumps(x).replace(chr(34), chr(39)) if isinstance(x, str) else str(x) for x in v)
                    opts_parts.append(f"{k}=[{items}]")
                elif isinstance(v, bool):
                    opts_parts.append(f"{k}={'true' if v else 'false'}")
                else:
                    opts_parts.append(f"{k}={v}")
            options_clause = ", ".join(opts_parts)

            ddl = (
                f"CREATE OR REPLACE MODEL `{destination_model_id}`\n"
                f"OPTIONS({options_clause})\n"
                f"AS\n{rendered_select}"
            )
            context.log.info(f"Training BQML model {destination_model_id}")
            context.log.info(f"DDL preview: {ddl[:300]!r}...")

            try:
                job = client.query(ddl)
                job.result()
            except Exception as e:
                err_str = str(e)
                if "403" in err_str or "PERMISSION_DENIED" in err_str:
                    sa_email = creds_dict.get("client_email", "<sa>")
                    context.log.error(
                        f"BQML training failed: SA {sa_email!r} needs "
                        f"roles/bigquery.dataEditor + roles/bigquery.jobUser on the project. "
                        f"Or grant roles/bigquery.admin / roles/owner."
                    )
                raise

            # Pull training info: ML.TRAINING_INFO returns one row per training run.
            training_info_df = pd.DataFrame()
            try:
                ti_job = client.query(f"SELECT * FROM ML.TRAINING_INFO(MODEL `{destination_model_id}`)")
                training_info_df = ti_job.to_dataframe(create_bqstorage_client=False)
            except Exception as e:
                context.log.warning(f"could not fetch ML.TRAINING_INFO: {e}")

            # Pull evaluation: ML.EVALUATE returns model fit metrics for the
            # default evaluation set. Some model types (KMEANS, MATRIX_FACTORIZATION)
            # have a different evaluate shape but the call is the same.
            evaluate_df = pd.DataFrame()
            try:
                e_job = client.query(f"SELECT * FROM ML.EVALUATE(MODEL `{destination_model_id}`)")
                evaluate_df = e_job.to_dataframe(create_bqstorage_client=False)
            except Exception as e:
                context.log.warning(f"could not fetch ML.EVALUATE: {e}")

            preview_md = evaluate_df.to_markdown(index=False) if not evaluate_df.empty else "(ML.EVALUATE returned no rows)"

            md = {
                "destination_model_id":  MetadataValue.text(destination_model_id),
                "model_type":            MetadataValue.text(str(model_options.get("model_type") or model_options.get("MODEL_TYPE"))),
                "options":               MetadataValue.json(model_options),
                "bytes_billed":          MetadataValue.int(int(job.total_bytes_billed or 0)),
                "slot_millis":           MetadataValue.int(int(job.slot_millis or 0)),
                "training_iterations":   MetadataValue.int(int(len(training_info_df))),
                "evaluate_metrics":      MetadataValue.md(preview_md or ""),
            }
            # Try to surface the headline metric depending on model type.
            for key in ("r2_score", "mean_absolute_error", "mean_squared_error",
                        "log_loss", "roc_auc", "f1_score", "accuracy", "precision", "recall",
                        "davies_bouldin_index", "mean_absolute_percentage_error"):
                if key in evaluate_df.columns and not evaluate_df.empty:
                    try:
                        md[key] = MetadataValue.float(float(evaluate_df.iloc[0][key]))
                    except (TypeError, ValueError):
                        pass

            return Output(value=evaluate_df if not evaluate_df.empty else training_info_df, metadata=md)

        return Definitions(assets=[_asset])
