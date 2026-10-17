"""LangChain Chain Asset Component.

Reads rows from an upstream asset DataFrame, runs them through a
configurable LangChain chain (prompt template + LLM + optional output parser),
and returns the enriched DataFrame.

Supports any LangChain-compatible LLM: OpenAI, Anthropic, Azure, Ollama, etc.
"""
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey
from pydantic import Field


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



def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    import pandas as pd
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
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
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


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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


class LangChainChainAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a LangChain chain over rows from an upstream asset DataFrame and return enriched DataFrame.

    Configures a LangChain prompt + LLM + optional output parser chain. Each row
    is formatted using the prompt_template and passed to the chain. The response
    is written to response_column.

    Supports OpenAI, Anthropic, Azure OpenAI, Google, Ollama, and any other
    LangChain-compatible LLM provider.

    Example:
        ```yaml
        type: dagster_component_templates.LangChainChainAssetComponent
        attributes:
          asset_name: summarized_articles
          upstream_asset_key: raw_articles
          llm_provider: openai
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          prompt_template: |
            Summarize the following article in 2-3 sentences:
            {content}
          response_column: summary
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,description="Upstream asset key providing a DataFrame (must return a pandas DataFrame)")

    # LLM configuration
    llm_provider: str = Field(default="openai", description="LLM provider: openai, anthropic, azure_openai, google, ollama")
    model: str = Field(default="gpt-4o-mini", description="Model name (provider-specific)")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var with provider API key")
    api_base_env_var: Optional[str] = Field(default=None, description="Env var with custom API base URL")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    max_tokens: int = Field(default=1024, description="Max tokens per completion")

    # Chain configuration
    prompt_template: str = Field(description="LangChain prompt template string with {column_name} placeholders")
    system_message: Optional[str] = Field(default=None, description="System message for chat models")
    response_column: Union[str, int] = Field(default="chain_output", description="Column name to store chain output")
    parse_json: bool = Field(default=False, description="Attempt to parse LLM response as JSON and expand into columns")

    # Processing
    batch_size: int = Field(default=10, description="Rows per batch for progress logging")
    max_rows: Optional[int] = Field(default=None, description="Limit rows processed (for testing)")
    max_concurrency: int = Field(default=1, description="Concurrent LLM calls (use >1 carefully for rate limits)")

    group_name: Optional[str] = Field(default="ai", description="Asset group name")
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

    partition_values: Optional[Union[str, List[str]]] = Field(
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
    description: Optional[str] = Field(default=None)

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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("LangChainChainAssetComponent: set exactly one of `upstream_asset_key` or `source`.")
        _self = self


        # Build partition definition (auto-generated; supports daily, weekly, monthly,

        # hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(freshness_policy=_freshness_policy_obj, retry_policy=_retry_policy, partitions_def=partitions_def, 
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"LangChain ({_self.llm_provider}/{_self.model}): {_self.prompt_template[:50]}...",
            group_name=_self.group_name,
            kinds={"ai", "langchain"},
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(_self.upstream_asset_key))} if _self.upstream_asset_key else None),
            required_resource_keys=({_self.source["resource_key"]} if (_self.source and _self.source.get("resource_key")) else None),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def langchain_chain_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(_self.source, context)
            # Filter to current partition if partitioned
            if upstream is not None and (context.has_partition_key or context.has_partition_key_range):
                import pandas as pd
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
            import os, json

            df = upstream.copy()

            if _self.max_rows:
                df = df.head(_self.max_rows)

            # Build LangChain LLM
            from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
            from langchain_core.output_parsers import StrOutputParser

            provider = _self.llm_provider.lower()
            api_key = _require_env(_self.api_key_env_var) if _self.api_key_env_var else None
            api_base = os.environ.get(_self.api_base_env_var or "", None) if _self.api_base_env_var else None

            if provider == "openai":
                from langchain_openai import ChatOpenAI
                llm_kwargs: dict = {"model": _self.model, "temperature": _self.temperature, "max_tokens": _self.max_tokens}
                if api_key:
                    llm_kwargs["api_key"] = api_key
                if api_base:
                    llm_kwargs["base_url"] = api_base
                llm = ChatOpenAI(**llm_kwargs)

            elif provider == "anthropic":
                from langchain_anthropic import ChatAnthropic
                llm_kwargs = {"model": _self.model, "temperature": _self.temperature, "max_tokens": _self.max_tokens}
                if api_key:
                    llm_kwargs["api_key"] = api_key
                llm = ChatAnthropic(**llm_kwargs)

            elif provider == "azure_openai":
                from langchain_openai import AzureChatOpenAI
                llm_kwargs = {"azure_deployment": _self.model, "temperature": _self.temperature, "max_tokens": _self.max_tokens}
                if api_key:
                    llm_kwargs["api_key"] = api_key
                if api_base:
                    llm_kwargs["azure_endpoint"] = api_base
                llm = AzureChatOpenAI(**llm_kwargs)

            elif provider == "google":
                from langchain_google_genai import ChatGoogleGenerativeAI
                llm_kwargs = {"model": _self.model, "temperature": _self.temperature}
                if api_key:
                    llm_kwargs["google_api_key"] = api_key
                llm = ChatGoogleGenerativeAI(**llm_kwargs)

            elif provider == "ollama":
                from langchain_ollama import ChatOllama
                llm_kwargs = {"model": _self.model, "temperature": _self.temperature}
                if api_base:
                    llm_kwargs["base_url"] = api_base
                llm = ChatOllama(**llm_kwargs)

            else:
                raise ValueError(f"Unsupported llm_provider: {provider}. Use: openai, anthropic, azure_openai, google, ollama")

            # Build prompt
            if _self.system_message:
                prompt = ChatPromptTemplate.from_messages([
                    ("system", _self.system_message),
                    ("human", _self.prompt_template),
                ])
            else:
                prompt = ChatPromptTemplate.from_template(_self.prompt_template)

            chain = prompt | llm | StrOutputParser()

            context.log.info(f"Running LangChain chain ({provider}/{_self.model}) over {len(df)} rows")

            responses = []
            for i, row in enumerate(df.itertuples(index=False)):
                row_dict = row._asdict()
                try:
                    response = chain.invoke(row_dict)
                except Exception as e:
                    context.log.warning(f"Row {i} failed: {e}")
                    response = None
                responses.append(response)

                if (i + 1) % _self.batch_size == 0:
                    context.log.info(f"Processed {i + 1}/{len(df)} rows")

            if _self.parse_json:
                parsed_rows = []
                for r in responses:
                    try:
                        parsed_rows.append(json.loads(r) if r else {})
                    except Exception:
                        parsed_rows.append({_self.response_column: r})
                if parsed_rows:
                    parsed_df = pd.DataFrame(parsed_rows)
                    for col in parsed_df.columns:
                        df[col] = parsed_df[col].values
            else:
                df[_self.response_column] = responses

            context.log.info(f"Completed chain over {len(df)} rows")
            return df

        return dg.Definitions(assets=[langchain_chain_asset])


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
