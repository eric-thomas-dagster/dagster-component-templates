import json
import os
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


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


class ElasticsearchAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Index data into Elasticsearch or query ES and write results to a database table.

    Supports two modes:

    - ``index`` — accepts a DataFrame from an upstream Dagster asset and bulk-indexes
      the rows as documents into an Elasticsearch index.
    - ``query`` — runs a DSL query against an Elasticsearch index and writes the
      matching hits to a relational database table via SQLAlchemy.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Elasticsearch connection ---------------------------------------------
    elasticsearch_url_env_var: str = Field(
        description=(
            "Name of the environment variable containing the Elasticsearch URL "
            "(e.g. ``http://localhost:9200`` or an Elastic Cloud endpoint)."
        )
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the Elastic Cloud API key. "
            "Leave unset for unauthenticated or username/password-based clusters."
        ),
    )

    # --- Index ----------------------------------------------------------------
    index_name: str = Field(description="Elasticsearch index to operate on.")

    # --- Mode -----------------------------------------------------------------
    mode: str = Field(
        default="index",
        description=(
            "Operation mode. ``index`` bulk-indexes documents from an upstream "
            "Dagster asset DataFrame; ``query`` runs a DSL query and writes hits "
            "to a database table."
        ),
    )

    # --- Index-mode source (upstream asset) -----------------------------------
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Asset key of the upstream asset providing a pandas DataFrame. "
            "In ``index`` mode the DataFrame rows are indexed as ES documents. "
            "In ``query`` mode this field is optional (lineage only; the "
            "upstream value is not used). Mutually exclusive with `source` "
            "in ``index`` mode."
        ),
    )
    id_field: Optional[str] = Field(
        default=None,
        description=(
            "Column/field to use as the Elasticsearch document ``_id``. "
            "If omitted, Elasticsearch auto-generates IDs."
        ),
    )
    chunk_size: int = Field(
        default=500,
        description="Number of documents per bulk-index request in ``index`` mode.",
    )

    # --- Query-mode -----------------------------------------------------------
    query: str = Field(
        default="{}",
        description=(
            "JSON string representing the Elasticsearch query DSL body sent in "
            "``query`` mode. Defaults to ``{}`` which Elasticsearch treats as "
            "``match_all``."
        ),
    )
    database_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing a SQLAlchemy-compatible database URL. "
            "Used as the destination in ``query`` mode."
        ),
    )
    table_name: Optional[str] = Field(
        default=None,
        description="Destination table name for ``query`` mode results.",
    )
    if_exists: str = Field(
        default="replace",
        description=(
            "Behaviour when the destination table already exists in ``query`` mode. "
            "One of: ``replace``, ``append``, ``fail``."
        ),
    )

    # --- Asset metadata -------------------------------------------------------
    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the data being written / produced in metadata, "
            "so builder UIs can show output shape without warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows in the preview when include_preview_metadata=True. Random "
            "sample if len > 10x preview_rows; else head."
        ),
    )

    group_name: str = Field(
        default="elasticsearch",
        description="Dagster asset group name.",
    )
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

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

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
        if self.mode == "index":
            if bool(self.upstream_asset_key) == bool(self.source):
                raise ValueError("ElasticsearchAssetComponent: set exactly one of `upstream_asset_key` or `source`.")
        elif self.source:
            raise ValueError("ElasticsearchAssetComponent: `source` is only supported in the DataFrame-input mode (mode=index).")
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        component = self


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



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=component.asset_name,
            group_name=component.group_name,
            kinds={"elasticsearch"},
            ins=({"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))} if component.upstream_asset_key else None),
            required_resource_keys=({component.source["resource_key"]} if (component.source and component.source.get("resource_key")) else None),
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _elasticsearch_asset(
            context: dg.AssetExecutionContext,
            **kwargs,
        ) -> dg.MaterializeResult:
            upstream = kwargs.get("upstream")
            if upstream is None and component.source:
                upstream = _ingest_warehouse_query(component.source, context)
            from elasticsearch import Elasticsearch  # type: ignore[import]
            from elasticsearch.helpers import bulk  # type: ignore[import]

            es_url = os.environ.get(component.elasticsearch_url_env_var)
            if not es_url:
                raise ValueError(
                    f"Environment variable '{component.elasticsearch_url_env_var}' "
                    "is not set or is empty."
                )

            es_kwargs: dict = {"hosts": [es_url]}
            if component.api_key_env_var:
                api_key = os.environ.get(component.api_key_env_var)
                if not api_key:
                    raise ValueError(
                        f"Environment variable '{component.api_key_env_var}' "
                        "is not set or is empty."
                    )
                es_kwargs["api_key"] = api_key

            es = Elasticsearch(**es_kwargs)
            context.log.info(
                f"[Elasticsearch] Connected to {es_url}. Mode: {component.mode}."
            )

            # ------------------------------------------------------------------
            # INDEX mode
            # ------------------------------------------------------------------
            if component.mode == "index":
                import pandas as pd  # type: ignore[import]

                df = upstream
                if not isinstance(df, pd.DataFrame):
                    raise ValueError("Upstream asset did not provide a pandas DataFrame.")

                context.log.info(
                    f"[Elasticsearch] Received {len(df):,} rows from upstream asset. "
                    f"Indexing into '{component.index_name}' "
                    f"in chunks of {component.chunk_size} ..."
                )

                def _generate_actions(dataframe):
                    for _, row in dataframe.iterrows():
                        doc = row.to_dict()
                        action: dict = {
                            "_index": component.index_name,
                            "_source": doc,
                        }
                        if component.id_field and component.id_field in doc:
                            action["_id"] = str(doc[component.id_field])
                        yield action

                success_count, errors = bulk(
                    es,
                    _generate_actions(df),
                    chunk_size=component.chunk_size,
                    raise_on_error=True,
                )
                context.log.info(
                    f"[Elasticsearch] Indexed {success_count:,} documents into "
                    f"'{component.index_name}'."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "mode": "index",
                        "num_docs": success_count,
                        "chunk_size": component.chunk_size,
                        "id_field": component.id_field or "(auto)",
                    }
                )

            # ------------------------------------------------------------------
            # QUERY mode
            # ------------------------------------------------------------------
            if component.mode == "query":
                import pandas as pd  # type: ignore[import]
                from sqlalchemy import create_engine  # type: ignore[import]

                if not component.database_url_env_var:
                    raise ValueError(
                        "'database_url_env_var' must be set in 'query' mode."
                    )
                if not component.table_name:
                    raise ValueError(
                        "'table_name' must be set in 'query' mode."
                    )

                query_body = json.loads(component.query) if component.query else {}
                if not query_body:
                    query_body = {"query": {"match_all": {}}}
                elif "query" not in query_body:
                    query_body = {"query": query_body}

                context.log.info(
                    f"[Elasticsearch] Querying index '{component.index_name}' ..."
                )
                response = es.search(index=component.index_name, body=query_body, size=10_000)
                hits = response["hits"]["hits"]
                context.log.info(
                    f"[Elasticsearch] Query returned {len(hits):,} hits."
                )

                rows = [hit["_source"] for hit in hits]
                df = pd.DataFrame(rows)

                dest_url = os.environ.get(component.database_url_env_var)
                if not dest_url:
                    raise ValueError(
                        f"Environment variable '{component.database_url_env_var}' "
                        "is not set or is empty."
                    )

                engine = create_engine(dest_url)
                df.to_sql(
                    component.table_name,
                    engine,
                    if_exists=component.if_exists,
                    index=False,
                )
                context.log.info(
                    f"[Elasticsearch] Wrote {len(df):,} rows to table "
                    f"'{component.table_name}'."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "mode": "query",
                        "num_docs": len(df),
                        "destination_table": component.table_name,
                        "if_exists": component.if_exists,
                    **({"preview": dg.MetadataValue.md((df.sample(component.preview_rows) if len(df) > component.preview_rows * 10 else df.head(component.preview_rows)).to_markdown(index=False))} if component.include_preview_metadata and len(df) > 0 else {}),
                }
                )

            raise ValueError(
                f"Unsupported mode '{component.mode}'. Must be 'index' or 'query'."
            )

        return dg.Definitions(assets=[_elasticsearch_asset])
