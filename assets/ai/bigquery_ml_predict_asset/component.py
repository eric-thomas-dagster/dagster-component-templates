"""BigQueryMLPredictAssetComponent — run ML.PREDICT / ML.FORECAST against a BQML model.

Returns a pandas DataFrame of predictions, with metadata showing the
model id, prediction count, and bytes billed.
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

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


class BigQueryMLPredictAssetComponent(Component, Model, Resolvable):
    """Run ML.PREDICT or ML.FORECAST against a BQML model and return a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    location: Optional[str] = Field(default=None)

    model_id: str = Field(
        description="Fully-qualified BQML model id, e.g. `my-project.ml.my_arima`.",
    )
    operation: Literal["predict", "forecast", "explain_predict", "detect_anomalies"] = Field(
        default="predict",
        description="`predict` → ML.PREDICT; `forecast` → ML.FORECAST (ARIMA models); `explain_predict` → ML.EXPLAIN_PREDICT; `detect_anomalies` → ML.DETECT_ANOMALIES.",
    )

    # Input rows source — three modes:
    input_query: Optional[str] = Field(
        default=None,
        description="Inline SELECT producing the input rows. For ML.FORECAST set horizon-style options instead via `options` and leave this null.",
    )
    input_table_id: Optional[str] = Field(
        default=None,
        description="Fully-qualified table id whose rows are passed as input. Mutually exclusive with input_query.",
    )

    # ML.* function options (e.g. for FORECAST: STRUCT(10 AS horizon, 0.95 AS confidence_level))
    options: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Options struct passed to ML.PREDICT/ML.FORECAST/etc. Example for forecast: {horizon: 10, confidence_level: 0.95}.",
    )
    threshold: Optional[float] = Field(
        default=None,
        description="For LOGISTIC_REG / DNN_CLASSIFIER / boosted-tree classifiers — set the score threshold.",
    )
    keep_input_columns: bool = Field(
        default=True,
        description="If True (default), prediction output preserves all input columns alongside the predicted ones.",
    )
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Optional Dagster lineage hook. Doesn't affect input rows — set input_query or input_table_id for those.",
    )

    deps: Optional[List[str]] = Field(default=None)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError(
                "BigQueryMLPredictAssetComponent: provide credentials, credentials_path, "
                "or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        if self.input_query and self.input_table_id:
            raise ValueError("Set input_query OR input_table_id, not both.")
        if self.operation in ("predict", "explain_predict", "detect_anomalies") and not (self.input_query or self.input_table_id):
            raise ValueError(
                f"operation={self.operation!r} requires either input_query or input_table_id."
            )

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        model_id = self.model_id
        operation = self.operation
        input_query = self.input_query
        input_table_id = self.input_table_id
        options = self.options
        threshold = self.threshold

        ins_kwargs: Dict[str, Any] = {}
        if self.upstream_asset_key:
            ins_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))}

        @asset(
            name=asset_name,
            description=self.description or f"BQML {operation} against {model_id}.",
            group_name=self.group_name,
            kinds={"bigquery", "bqml", "ml"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            **ins_kwargs,
        )
        def _asset(context: AssetExecutionContext, **_kwargs) -> Output:
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(credentials=sa_creds, project=project_id, location=location)

            # Build the input subquery / table reference.
            if input_query:
                input_clause = f"({input_query})"
            elif input_table_id:
                input_clause = f"TABLE `{input_table_id}`"
            else:
                input_clause = ""

            # Build the options struct.
            opts_parts = []
            if options:
                for k, v in options.items():
                    if isinstance(v, str):
                        opts_parts.append(f"{json.dumps(v).replace(chr(34), chr(39))} AS {k}")
                    elif isinstance(v, bool):
                        opts_parts.append(f"{'TRUE' if v else 'FALSE'} AS {k}")
                    else:
                        opts_parts.append(f"{v} AS {k}")
            if threshold is not None and operation == "predict":
                opts_parts.append(f"{threshold} AS threshold")
            options_clause = f"STRUCT({', '.join(opts_parts)})" if opts_parts else ""

            fn_map = {
                "predict":           "ML.PREDICT",
                "forecast":          "ML.FORECAST",
                "explain_predict":   "ML.EXPLAIN_PREDICT",
                "detect_anomalies":  "ML.DETECT_ANOMALIES",
            }
            fn = fn_map[operation]

            args = [f"MODEL `{model_id}`"]
            if input_clause:
                args.append(input_clause)
            if options_clause:
                args.append(options_clause)
            sql = f"SELECT * FROM {fn}({', '.join(args)})"

            context.log.info(f"BQML {operation} → {model_id}")
            context.log.info(f"SQL: {sql[:300]!r}")

            try:
                job = client.query(sql)
                df = job.to_dataframe(create_bqstorage_client=False)
            except Exception as e:
                err_str = str(e)
                if "404" in err_str and "model" in err_str.lower():
                    context.log.error(
                        f"Model {model_id!r} not found. Train it first with "
                        f"BigQueryMLTrainAssetComponent."
                    )
                raise

            preview_md = df.head(10).to_markdown(index=False) if not df.empty else "(empty)"
            return Output(
                value=df,
                metadata={
                    "model_id":         MetadataValue.text(model_id),
                    "operation":        MetadataValue.text(operation),
                    "row_count":        MetadataValue.int(len(df)),
                    "bytes_billed":     MetadataValue.int(int(job.total_bytes_billed or 0)),
                    "preview":          MetadataValue.md(preview_md or ""),
                },
            )

        return Definitions(assets=[_asset])
