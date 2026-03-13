"""Snowflake Cortex Asset Component.

Runs Snowflake Cortex LLM functions (COMPLETE, CLASSIFY_TEXT, SENTIMENT, SUMMARIZE,
TRANSLATE, EXTRACT_ANSWER) on a source table and writes enriched results to a target
table — all inside Snowflake, with no data leaving the warehouse.

Supported Cortex functions
--------------------------
- ``COMPLETE``        — LLM completion with a prompt template and configurable model.
- ``SENTIMENT``       — Returns a sentiment score in [-1, 1].
- ``SUMMARIZE``       — Returns a text summary.
- ``CLASSIFY_TEXT``   — Returns the most likely category from a provided list.
- ``TRANSLATE``       — Translates text to a target language.
- ``EXTRACT_ANSWER``  — Extracts an answer to a question from the text.
"""
from __future__ import annotations

from typing import Optional

import dagster as dg


class SnowflakeCortexAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Snowflake Cortex LLM function on a table and write results to a new table.

    Example:
        ```yaml
        type: dagster_component_templates.SnowflakeCortexAssetComponent
        attributes:
          asset_name: support_ticket_sentiment
          snowflake_account_env_var: SNOWFLAKE_ACCOUNT
          snowflake_user_env_var: SNOWFLAKE_USER
          snowflake_password_env_var: SNOWFLAKE_PASSWORD
          snowflake_database: ANALYTICS
          snowflake_schema: PUBLIC
          source_table: RAW_SUPPORT_TICKETS
          target_table: ENRICHED_SUPPORT_TICKETS
          cortex_function: SENTIMENT
          text_column: ticket_body
          output_column: sentiment_score
          group_name: ai_enrichment
          deps:
            - raw/support_tickets
        ```
    """

    snowflake_account_env_var: str = dg.Field(
        default="SNOWFLAKE_ACCOUNT",
        description="Env var name holding the Snowflake account identifier.",
    )
    snowflake_user_env_var: str = dg.Field(
        default="SNOWFLAKE_USER",
        description="Env var name holding the Snowflake username.",
    )
    snowflake_password_env_var: str = dg.Field(
        default="SNOWFLAKE_PASSWORD",
        description="Env var name holding the Snowflake password.",
    )
    snowflake_database: str = dg.Field(description="Snowflake database name.")
    snowflake_schema: str = dg.Field(
        default="PUBLIC",
        description="Snowflake schema name.",
    )
    snowflake_warehouse: Optional[str] = dg.Field(
        default=None,
        description="Snowflake virtual warehouse to use. Uses the user default if None.",
    )
    source_table: str = dg.Field(
        description="Source table name (fully qualified or relative to database/schema)."
    )
    target_table: str = dg.Field(
        description="Target table where enriched results are written."
    )
    cortex_function: str = dg.Field(
        default="COMPLETE",
        description=(
            "Cortex function to apply: COMPLETE, CLASSIFY_TEXT, SENTIMENT, SUMMARIZE, "
            "TRANSLATE, or EXTRACT_ANSWER."
        ),
    )
    model: str = dg.Field(
        default="claude-3-5-sonnet",
        description=(
            "LLM model name for COMPLETE. Examples: claude-3-5-sonnet, mistral-large, llama3-70b."
        ),
    )
    text_column: str = dg.Field(
        description="Column passed as the text argument to the Cortex function."
    )
    output_column: str = dg.Field(
        default="cortex_result",
        description="Name of the output column added to the target table.",
    )
    prompt_template: Optional[str] = dg.Field(
        default=None,
        description=(
            "For COMPLETE: prompt template with a {text} placeholder. "
            "E.g. 'Summarize this in one sentence: {text}'"
        ),
    )
    classify_categories: Optional[list] = dg.Field(
        default=None,
        description="For CLASSIFY_TEXT: list of category strings to classify into.",
    )
    translate_target_language: Optional[str] = dg.Field(
        default=None,
        description='For TRANSLATE: BCP-47 language code, e.g. "en", "fr", "de".',
    )
    extract_answer_question: Optional[str] = dg.Field(
        default=None,
        description="For EXTRACT_ANSWER: the question to answer from the text.",
    )
    if_exists: str = dg.Field(
        default="replace",
        description='Table write mode: "replace" (CREATE OR REPLACE) or "append" (INSERT INTO).',
    )
    batch_size: int = dg.Field(
        default=1000,
        description="Row batch size for logging progress (does not affect SQL execution).",
    )
    group_name: Optional[str] = dg.Field(
        default="ai_enrichment",
        description="Dagster asset group name shown in the UI.",
    )
    asset_name: str = dg.Field(description="Dagster asset key name.")
    deps: Optional[list] = dg.Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    def _build_cortex_expr(self) -> str:
        """Return the SNOWFLAKE.CORTEX.<fn>(...) SQL expression."""
        fn = self.cortex_function.upper()
        col = self.text_column

        if fn == "COMPLETE":
            if self.prompt_template:
                # Embed prompt prefix as a literal; {text} is replaced by the column value
                prefix = self.prompt_template.replace("{text}", "").replace("'", "''")
                return (
                    f"SNOWFLAKE.CORTEX.COMPLETE('{self.model}', "
                    f"CONCAT('{prefix}', {col}))"
                )
            return f"SNOWFLAKE.CORTEX.COMPLETE('{self.model}', {col})"

        if fn == "SENTIMENT":
            return f"SNOWFLAKE.CORTEX.SENTIMENT({col})"

        if fn == "SUMMARIZE":
            return f"SNOWFLAKE.CORTEX.SUMMARIZE({col})"

        if fn == "CLASSIFY_TEXT":
            if not self.classify_categories:
                raise ValueError("classify_categories is required for CLASSIFY_TEXT.")
            cats = ", ".join(f"'{c}'" for c in self.classify_categories)
            return f"SNOWFLAKE.CORTEX.CLASSIFY_TEXT({col}, [{cats}])"

        if fn == "TRANSLATE":
            if not self.translate_target_language:
                raise ValueError("translate_target_language is required for TRANSLATE.")
            # source language '' means auto-detect
            return f"SNOWFLAKE.CORTEX.TRANSLATE({col}, '', '{self.translate_target_language}')"

        if fn == "EXTRACT_ANSWER":
            if not self.extract_answer_question:
                raise ValueError("extract_answer_question is required for EXTRACT_ANSWER.")
            q = self.extract_answer_question.replace("'", "''")
            return f"SNOWFLAKE.CORTEX.EXTRACT_ANSWER({col}, '{q}')"

        raise ValueError(
            f"Unknown cortex_function '{self.cortex_function}'. "
            "Must be one of: COMPLETE, CLASSIFY_TEXT, SENTIMENT, SUMMARIZE, TRANSLATE, EXTRACT_ANSWER."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name or "ai_enrichment",
            kinds={"snowflake", "ai", "sql"},
            deps=dep_keys,
            description=(
                f"Snowflake Cortex {self.cortex_function} on {self.source_table} "
                f"-> {self.target_table}"
            ),
        )
        def _cortex_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import os
            import snowflake.connector

            account = os.environ[self.snowflake_account_env_var]
            user = os.environ[self.snowflake_user_env_var]
            password = os.environ[self.snowflake_password_env_var]

            connect_kwargs: dict = dict(
                account=account,
                user=user,
                password=password,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
            )
            if self.snowflake_warehouse:
                connect_kwargs["warehouse"] = self.snowflake_warehouse

            cortex_expr = self._build_cortex_expr()
            output_col = self.output_column

            if self.if_exists == "replace":
                dml = (
                    f"CREATE OR REPLACE TABLE {self.target_table} AS "
                    f"SELECT *, {cortex_expr} AS {output_col} "
                    f"FROM {self.source_table}"
                )
            else:
                dml = (
                    f"INSERT INTO {self.target_table} "
                    f"SELECT *, {cortex_expr} AS {output_col} "
                    f"FROM {self.source_table}"
                )

            context.log.info(f"Executing Cortex SQL:\n{dml}")

            conn = snowflake.connector.connect(**connect_kwargs)
            try:
                cur = conn.cursor()
                cur.execute(dml)

                # Fetch row count from the target table
                cur.execute(f"SELECT COUNT(*) FROM {self.target_table}")
                rows_processed = cur.fetchone()[0]
                cur.close()
            finally:
                conn.close()

            context.log.info(
                f"Cortex {self.cortex_function} complete — {rows_processed} rows in {self.target_table}"
            )

            return dg.MaterializeResult(
                metadata={
                    "rows_processed": dg.MetadataValue.int(rows_processed),
                    "cortex_function": dg.MetadataValue.text(self.cortex_function),
                    "model": dg.MetadataValue.text(
                        self.model if self.cortex_function.upper() == "COMPLETE" else "n/a"
                    ),
                    "source_table": dg.MetadataValue.text(self.source_table),
                    "target_table": dg.MetadataValue.text(self.target_table),
                }
            )

        return dg.Definitions(assets=[_cortex_asset])
