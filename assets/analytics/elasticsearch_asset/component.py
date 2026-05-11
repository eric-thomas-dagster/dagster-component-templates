import json
import os
from typing import Any, Dict, List, Optional

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
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
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
    upstream_asset_key: str = Field(
        description=(
            "Asset key of the upstream asset providing a pandas DataFrame. "
            "In ``index`` mode the DataFrame rows are indexed as ES documents. "
            "In ``query`` mode this field is still required for lineage but the "
            "upstream value is not used."
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
    partition_date_column: Optional[str] = Field(
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
        )
        def _elasticsearch_asset(
            context: dg.AssetExecutionContext,
            upstream,
        ) -> dg.MaterializeResult:
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
                    **({"preview": MetadataValue.md((df.sample(preview_rows) if len(df) > preview_rows * 10 else df.head(preview_rows)).to_markdown(index=False))} if include_preview and len(df) > 0 else {}),
                }
                )

            raise ValueError(
                f"Unsupported mode '{component.mode}'. Must be 'index' or 'query'."
            )

        return dg.Definitions(assets=[_elasticsearch_asset])
