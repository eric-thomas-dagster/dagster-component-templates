"""DataFrame → OpenTelemetry Logs (OTLP/HTTP).

Push DataFrame rows as OpenTelemetry log records via the OTLP HTTP
exporter. Same universal-backend story as the metrics sink — works
against OTel collectors and any vendor's OTLP intake.

Each row → one log record. Configurable severity, timestamp, body, and
attributes (labels) per log.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
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
)
from pydantic import Field


_SEVERITY_NUMBER_MAP = {
    "TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21,
}


class DataframeToOtlpLogsComponent(Component, Model, Resolvable):
    """Push DataFrame rows as OpenTelemetry log records via OTLP/HTTP."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    endpoint: str = Field(description="OTLP/HTTP endpoint base URL")
    service_name: str = Field(default="dagster")

    body_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes the log body. If unset, JSON-serializes the row.",
    )
    severity_column: Optional[str] = Field(
        default=None,
        description=(
            "Column with severity strings (TRACE/DEBUG/INFO/WARN/ERROR/FATAL). "
            "Defaults to INFO for every row."
        ),
    )
    timestamp_column: Optional[str] = Field(
        default=None,
        description="Column with epoch-seconds (or pandas datetime). Default: now() per row.",
    )
    attribute_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns whose values become OTel log attributes",
    )
    bearer_token_env_var: Optional[str] = Field(default=None)
    extra_headers: Optional[Dict[str, str]] = Field(default=None)

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        cfg = self
        kinds = self.kinds or ["opentelemetry", "logs", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Push DataFrame rows as OTLP log records",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def otlp_logs(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            import json, logging
            try:
                from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
                from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
                from opentelemetry._logs import set_logger_provider
            except ImportError as e:
                raise ImportError(
                    "OTel SDK required: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http"
                ) from e

            headers = {}
            if cfg.bearer_token_env_var:
                token = os.environ.get(cfg.bearer_token_env_var)
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            if cfg.extra_headers:
                headers.update(cfg.extra_headers)

            exp_kwargs = {"endpoint": f"{cfg.endpoint.rstrip('/')}/v1/logs"}
            if headers:
                exp_kwargs["headers"] = headers

            exporter = OTLPLogExporter(**exp_kwargs)
            resource = Resource.create({"service.name": cfg.service_name})
            provider = LoggerProvider(resource=resource)
            provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
            set_logger_provider(provider)

            # Bridge through stdlib logging — public path for LogRecord creation.
            # Use DEBUG level so all severities make it through (we filter per-row by severity_to_log_level).
            handler = LoggingHandler(level=logging.DEBUG, logger_provider=provider)
            otel_logger = logging.getLogger(f"dagster_otlp_sink.{id(self)}")
            otel_logger.setLevel(logging.DEBUG)
            otel_logger.addHandler(handler)
            otel_logger.propagate = False

            attribute_cols = cfg.attribute_columns or []
            sent = 0
            severity_to_log_level = {
                "TRACE": logging.DEBUG, "DEBUG": logging.DEBUG, "INFO": logging.INFO,
                "WARN": logging.WARNING, "ERROR": logging.ERROR, "FATAL": logging.CRITICAL,
            }

            for _, row in upstream.iterrows():
                body = (
                    str(row[cfg.body_column]) if cfg.body_column
                    else json.dumps({k: (v if not pd.isna(v) else None) for k, v in row.items()}, default=str)
                )
                sev_str = (str(row[cfg.severity_column]).upper() if cfg.severity_column else "INFO")
                level = severity_to_log_level.get(sev_str, logging.INFO)
                attrs = {c: str(row[c]) for c in attribute_cols if c in row}
                otel_logger.log(level, body, extra=attrs)
                sent += 1

            try:
                provider.force_flush(timeout_millis=10_000)
            except Exception as e:
                context.log.warning(f"force_flush warning: {e}")

            context.log.info(f"Exported {sent} OTLP log records to {cfg.endpoint}")
            return MaterializeResult(
                metadata={
                    "logs_exported": MetadataValue.int(sent),
                    "endpoint": MetadataValue.text(cfg.endpoint),
                    "service_name": MetadataValue.text(cfg.service_name),
                }
            )

        return Definitions(assets=[otlp_logs])
