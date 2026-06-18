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
from typing import Dict, List, Optional, Union

import dagster as dg
from pydantic import ConfigDict, Field


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

    # Internal field is `llm_model` (avoids shadowing the `model` attribute
    # on dg.Resolvable + pydantic's model_* protected namespace). YAML still
    # accepts the Cortex-native `model:` key via the alias + populate_by_name.
    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    snowflake_account_env_var: str = Field(
        default="SNOWFLAKE_ACCOUNT",
        description="Env var name holding the Snowflake account identifier.",
    )
    snowflake_user_env_var: str = Field(
        default="SNOWFLAKE_USER",
        description="Env var name holding the Snowflake username.",
    )
    snowflake_password_env_var: Optional[str] = Field(
        default="SNOWFLAKE_PASSWORD",
        description="Env var name holding the Snowflake password. Leave None / empty when using SSO or keypair.",
    )
    # SSO / keypair / PAT alternatives — for accounts where password auth
    # is disabled. Leave snowflake_password_env_var unset and use one of these.
    snowflake_authenticator: Optional[str] = Field(
        default=None,
        description="Snowflake authenticator: 'SNOWFLAKE_JWT' (keypair), 'externalbrowser' (SSO), 'oauth', etc.",
    )
    snowflake_private_key_file_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the path to a PEM RSA private key file (for snowflake_authenticator='SNOWFLAKE_JWT').",
    )
    snowflake_private_key_file_pwd_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the passphrase for an encrypted private key file (optional).",
    )
    snowflake_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding an OAuth / PAT token (with snowflake_authenticator='oauth' or PAT).",
    )
    snowflake_database: str = Field(description="Snowflake database name.")
    snowflake_schema: str = Field(
        default="PUBLIC",
        description="Snowflake schema name.",
    )
    snowflake_warehouse: Optional[str] = Field(
        default=None,
        description="Snowflake virtual warehouse to use. Uses the user default if None.",
    )
    source_table: str = Field(
        description="Source table name (fully qualified or relative to database/schema)."
    )
    target_table: str = Field(
        description="Target table where enriched results are written."
    )
    cortex_function: str = Field(
        default="COMPLETE",
        description=(
            "Cortex function to apply: COMPLETE, CLASSIFY_TEXT, SENTIMENT, SUMMARIZE, "
            "TRANSLATE, or EXTRACT_ANSWER."
        ),
    )
    llm_model: str = Field(
        default="claude-3-5-sonnet",
        alias="model",
        description=(
            "LLM model name for COMPLETE. Examples: claude-3-5-sonnet, mistral-large, llama3-70b."
        ),
    )
    text_column: Union[str, int] = Field(
        description="Column passed as the text argument to the Cortex function."
    )
    output_column: Union[str, int] = Field(
        default="cortex_result",
        description="Name of the output column added to the target table.",
    )
    prompt_template: Optional[str] = Field(
        default=None,
        description=(
            "For COMPLETE: prompt template with a {text} placeholder. "
            "E.g. 'Summarize this in one sentence: {text}'"
        ),
    )
    classify_categories: Optional[list] = Field(
        default=None,
        description="For CLASSIFY_TEXT: list of category strings to classify into.",
    )
    translate_target_language: Optional[str] = Field(
        default=None,
        description='For TRANSLATE: BCP-47 language code, e.g. "en", "fr", "de".',
    )
    extract_answer_question: Optional[str] = Field(
        default=None,
        description="For EXTRACT_ANSWER: the question to answer from the text.",
    )
    if_exists: str = Field(
        default="replace",
        description='Table write mode: "replace" (CREATE OR REPLACE) or "append" (INSERT INTO).',
    )
    batch_size: int = Field(
        default=1000,
        description="Row batch size for logging progress (does not affect SQL execution).",
    )
    group_name: Optional[str] = Field(
        default="ai_enrichment",
        description="Dagster asset group name shown in the UI.",
    )
    asset_name: str = Field(description="Dagster asset key name.")
    deps: Optional[list] = Field(
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
                    f"SNOWFLAKE.CORTEX.COMPLETE('{self.llm_model}', "
                    f"CONCAT('{prefix}', {col}))"
                )
            return f"SNOWFLAKE.CORTEX.COMPLETE('{self.llm_model}', {col})"

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



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Dagster asset group name.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name or "ai_enrichment",
            kinds={"snowflake", "ai", "sql"},
            deps=dep_keys,
            description=(
                f"Snowflake Cortex {self.cortex_function} on {self.source_table} "
                f"-> {self.target_table}"
            ),
        )
        def _cortex_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import snowflake.connector

            import os
            account = dg.EnvVar(self.snowflake_account_env_var).get_value()
            user = dg.EnvVar(self.snowflake_user_env_var).get_value()

            connect_kwargs: dict = dict(
                account=account,
                user=user,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
            )
            if self.snowflake_warehouse:
                connect_kwargs["warehouse"] = self.snowflake_warehouse

            # Auth: keypair / SSO / token takes precedence over password if BOTH
            # are set. Password path preserved for accounts that still allow it.
            if self.snowflake_authenticator:
                connect_kwargs["authenticator"] = self.snowflake_authenticator
                if self.snowflake_private_key_file_env_var:
                    pk_path = os.environ.get(self.snowflake_private_key_file_env_var)
                    if pk_path:
                        connect_kwargs["private_key_file"] = pk_path
                    if self.snowflake_private_key_file_pwd_env_var:
                        pk_pwd = os.environ.get(self.snowflake_private_key_file_pwd_env_var)
                        if pk_pwd:
                            connect_kwargs["private_key_file_pwd"] = pk_pwd
                elif self.snowflake_token_env_var:
                    tok = os.environ.get(self.snowflake_token_env_var)
                    if tok:
                        connect_kwargs["token"] = tok
            elif self.snowflake_password_env_var and os.environ.get(self.snowflake_password_env_var):
                connect_kwargs["password"] = os.environ[self.snowflake_password_env_var]
            else:
                raise EnvironmentError(
                    "snowflake_cortex_asset: no auth configured. Set either "
                    "snowflake_password_env_var (with password in env) OR "
                    "snowflake_authenticator + private_key_file/token env var."
                )

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
                        self.llm_model if self.cortex_function.upper() == "COMPLETE" else "n/a"
                    ),
                    "source_table": dg.MetadataValue.text(self.source_table),
                    "target_table": dg.MetadataValue.text(self.target_table),
                }
            )

        return dg.Definitions(assets=[_cortex_asset])
