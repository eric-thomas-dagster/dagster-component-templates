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

import re
import dagster as dg
from pydantic import ConfigDict, Field



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

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

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    partition_values: Optional[Union[str, List[str]]] = Field(
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
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
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


        @dg.asset(partitions_def=partitions_def, freshness_policy=_freshness_policy_obj, retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name or "ai_enrichment",
            kinds={"snowflake", "ai", "sql"},
            deps=dep_keys,
            description=(
                self.description or (f"Snowflake Cortex {self.cortex_function} on {self.source_table} "
                f"-> {self.target_table}")
            ),
        )
        def _cortex_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
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
                connect_kwargs["password"] = _require_env(self.snowflake_password_env_var)
            else:
                raise EnvironmentError(
                    "snowflake_cortex_asset: no auth configured. Set either "
                    "snowflake_password_env_var (with password in env) OR "
                    "snowflake_authenticator + private_key_file/token env var."
                )

            cortex_expr = self._build_cortex_expr()
            output_col = self.output_column

            # A partitioned run only processes (and, for if_exists=replace, only replaces) its own slice of the
            # source: rows in the partition's time window and/or with the partition's static-axis value.
            partition_where = _partition_predicate(
                context, partition_date_column, partition_static_column, partition_static_dim
            )
            statements = []
            if partition_where is None:
                if self.if_exists == "replace":
                    statements.append(
                        f"CREATE OR REPLACE TABLE {self.target_table} AS "
                        f"SELECT *, {cortex_expr} AS {output_col} "
                        f"FROM {self.source_table}"
                    )
                else:
                    statements.append(
                        f"INSERT INTO {self.target_table} "
                        f"SELECT *, {cortex_expr} AS {output_col} "
                        f"FROM {self.source_table}"
                    )
            else:
                select = f"SELECT *, {cortex_expr} AS {output_col} FROM {self.source_table} WHERE {partition_where}"
                if self.if_exists == "replace":
                    # Replacing the whole table would wipe the other partitions: replace only this slice.
                    statements.append(f"CREATE TABLE IF NOT EXISTS {self.target_table} AS {select} AND 1 = 0")
                    statements.append(f"DELETE FROM {self.target_table} WHERE {partition_where}")
                statements.append(f"INSERT INTO {self.target_table} {select}")
            dml = ";\n".join(statements)

            context.log.info(f"Executing Cortex SQL:\n{dml}")

            conn = snowflake.connector.connect(**connect_kwargs)
            try:
                cur = conn.cursor()
                for statement in statements:
                    cur.execute(statement)

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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


def _sql_ident(name):
    """A column name that is safe to splice into SQL (letters, digits, underscore, $, optionally dotted)."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*", str(name)):
        raise ValueError(f"unsafe SQL identifier for a partition column: {name!r}")
    return str(name)


def _sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def _partition_predicate(context, partition_date_column, partition_static_column, partition_static_dim):
    """WHERE-clause text selecting the current partition's rows, or None when the run is not partitioned (or no
    partition column is configured). Time partitions use the window dagster computes (correct for a single tick and
    for a backfill range); the static axis is matched against every key being materialized."""
    if not (context.has_partition_key or context.has_partition_key_range):
        return None
    clauses = []
    if partition_date_column:
        try:
            window = context.partition_time_window
        except Exception:
            window = None  # static / dynamic partitions have no time window
        if window is not None:
            col = _sql_ident(partition_date_column)
            clauses.append(
                f"{col} >= {_sql_str(window.start.strftime('%Y-%m-%d %H:%M:%S'))} "
                f"AND {col} < {_sql_str(window.end.strftime('%Y-%m-%d %H:%M:%S'))}"
            )
    if partition_static_column:
        values = []
        for key in context.partition_keys:
            if hasattr(key, "keys_by_dimension"):
                dims = key.keys_by_dimension
                v = next((dims[d] for d in (partition_static_dim, "static_dim", "segment") if d and d in dims), None)
            else:
                v = str(key)
            if v is not None:
                values.append(v)
        if values:
            clauses.append(f"{_sql_ident(partition_static_column)} IN ({', '.join(_sql_str(v) for v in sorted(set(values)))})")
    return " AND ".join(clauses) if clauses else None


# BEGIN partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port.py)
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
# END partitions-def builder

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
