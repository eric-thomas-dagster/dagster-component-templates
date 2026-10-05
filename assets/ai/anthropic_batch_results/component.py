"""Anthropic Batch Results Component.

Fetches and parses the results of a completed Anthropic Message Batch. Runs
after anthropic_batch_status_sensor confirms processing_status == "ended"
(batch_id arrives via run_config), or manually/for backfills against a
statically configured batch_id.
"""
import importlib
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Config,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field


class AnthropicBatchResultsConfig(Config):
    """Run-time config for the results asset. `batch_id` arrives dynamically
    via the status sensor's RunRequest.run_config, or is left unset for a
    manual/backfill run that relies on the component's static `batch_id`
    field instead."""

    batch_id: Optional[str] = None


def _resolve_output_schema(dotted: Optional[str]):
    """`module.path:ClassName` → the class. Same dotted-path-import
    convention as assets/infrastructure/smart_retry/component.py's
    `kind: python` handling."""
    if not dotted:
        return None
    if ":" not in dotted:
        raise ValueError(
            f"anthropic_batch_results: output_schema must be 'module.path:ClassName', got {dotted!r}"
        )
    module_path, cls_name = dotted.rsplit(":", 1)
    mod = importlib.import_module(module_path.strip())
    cls = getattr(mod, cls_name.strip(), None)
    if cls is None:
        raise ValueError(f"anthropic_batch_results: output_schema {dotted!r} does not resolve to a class")
    return cls


class AnthropicBatchResultsComponent(Component, Model, Resolvable):
    """Fetch and parse the results of a completed Anthropic Message Batch.

    Example:
        ```yaml
        type: dagster_component_templates.AnthropicBatchResultsComponent
        attributes:
          asset_name: support_ticket_batch_results
          api_key_env_var: ANTHROPIC_API_KEY
          output_schema: my_project.schemas:TicketClassification
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(
        description=(
            "Output Dagster asset name. The underlying op's name is derived from this via "
            "AssetKey.to_python_identifier() — the paired anthropic_batch_status_sensor's "
            "results_asset_key field must equal this value so its RunRequest.run_config "
            "addresses the correct op."
        )
    )
    batch_id: Optional[str] = Field(
        default=None,
        description=(
            "Static batch_id for manual/backfill runs. Overridden at runtime by the "
            "`batch_id` op config value supplied by anthropic_batch_status_sensor's RunRequest."
        ),
    )
    output_schema: Optional[str] = Field(
        default=None,
        description=(
            "Dotted path 'module.path:ClassName' to a Pydantic model used to validate/parse "
            "each succeeded row's raw_output as JSON. A row that fails to parse or validate "
            "is marked invalid_output=True (raw_output is kept, typed columns left null) "
            "rather than failing the whole asset."
        ),
    )
    api_key_env_var: str = Field(default="ANTHROPIC_API_KEY", description="Env var holding the Anthropic API key")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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
        description="Asset kinds for the Dagster catalog, e.g. ['anthropic', 'python']. Auto-inferred from component name if not set.",
    )
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        static_batch_id = self.batch_id
        output_schema_ref = self.output_schema
        api_key_env_var = self.api_key_env_var
        group_name = self.group_name

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
                _inferred_kinds = ["anthropic"]

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        owners = self.owners or []

        # Resolve the output_schema class once at definition-build time —
        # import errors surface immediately rather than mid-run.
        output_schema_cls = _resolve_output_schema(output_schema_ref)
        schema_fields = list(output_schema_cls.model_fields.keys()) if output_schema_cls is not None else []

        @asset(
            key=AssetKey.from_user_string(asset_name),
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            description=self.description,
        )
        def _asset(context: AssetExecutionContext, config: AnthropicBatchResultsConfig) -> pd.DataFrame:
            batch_id = config.batch_id or static_batch_id
            if not batch_id:
                raise ValueError(
                    "anthropic_batch_results: no batch_id supplied — set the component's static "
                    "`batch_id` field for a manual/backfill run, or trigger this asset via "
                    "anthropic_batch_status_sensor's RunRequest."
                )

            try:
                import anthropic
            except ImportError:
                raise ImportError("pip install anthropic")

            client = anthropic.Anthropic(api_key=os.environ[api_key_env_var])

            batch = client.messages.batches.retrieve(batch_id)
            if batch.processing_status != "ended":
                raise Exception(
                    f"anthropic_batch_results: batch {batch_id} has processing_status="
                    f"{batch.processing_status!r} (not 'ended'). This asset should only run "
                    "once anthropic_batch_status_sensor confirms completion — running it "
                    "manually before the batch has ended is not supported."
                )

            rows = []
            for entry in client.messages.batches.results(batch_id):
                custom_id = entry.custom_id
                result_type = entry.result.type
                raw_output = None
                error = None
                invalid_output = False
                row_extra = {f: None for f in schema_fields}

                if result_type == "succeeded":
                    blocks = getattr(entry.result.message, "content", []) or []
                    texts = [b.text for b in blocks if getattr(b, "type", None) == "text"]
                    raw_output = "\n".join(texts)
                    if output_schema_cls is not None:
                        try:
                            parsed = output_schema_cls.model_validate_json(raw_output)
                            row_extra.update(parsed.model_dump())
                        except Exception:
                            invalid_output = True
                elif result_type == "errored":
                    err = getattr(entry.result, "error", None)
                    err_obj = getattr(err, "error", err)
                    err_type = getattr(err_obj, "type", None)
                    err_message = getattr(err_obj, "message", str(err_obj))
                    error = f"{err_type}: {err_message}" if err_type else str(err_message)
                else:  # canceled / expired
                    error = f"request {result_type}"

                row = {
                    "custom_id": custom_id,
                    "result_type": result_type,
                    "raw_output": raw_output,
                    "error": error,
                    "invalid_output": invalid_output,
                }
                row.update(row_extra)
                rows.append(row)

            columns = ["custom_id", "result_type", "raw_output", "error", "invalid_output"] + schema_fields
            df = pd.DataFrame(rows, columns=columns)

            succeeded_count = int((df["result_type"] == "succeeded").sum()) if len(df) else 0
            errored_count = int((df["result_type"] == "errored").sum()) if len(df) else 0
            invalid_output_count = int(df["invalid_output"].sum()) if len(df) else 0

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "succeeded_count": MetadataValue.int(succeeded_count),
                    "errored_count": MetadataValue.int(errored_count),
                    "invalid_output_count": MetadataValue.int(invalid_output_count),
                }
            )
            return df

        return Definitions(assets=[_asset])
