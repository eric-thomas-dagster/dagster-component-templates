"""DataFrame -> Greenhouse candidate update (tags/custom fields, update-only).

Updates existing Greenhouse candidates' tags and/or custom fields via
`PATCH /v1/candidates/{id}`. This is update-only, not an upsert --
Greenhouse candidates must already exist (created via the ATS UI or the
application-submission API, not this sink); candidate_id_column must hold
a real Greenhouse candidate ID.

Pairs with:
  - ``greenhouse`` resource -- Harvest API key auth + On-Behalf-Of header (required)"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field
class GreenhouseCandidateUpdateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Update Greenhouse candidates' tags/custom fields from an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.GreenhouseCandidateUpdateComponent
        attributes:
          asset_name: greenhouse_candidate_scores
          upstream_asset_key: candidate_screening_scores
          resource_key: greenhouse
          candidate_id_column: greenhouse_candidate_id
          tags_column: computed_tags
          custom_fields_map:
            screening_score: screening_score
          group_name: reverse_etl
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="greenhouse",
        description="Resource key registered by GreenhouseResourceComponent.",
    )

    candidate_id_column: str = Field(description="Upstream column holding the Greenhouse candidate ID to update (must already exist in Greenhouse).")

    tags_column: Optional[str] = Field(default=None, description="Upstream column holding tags to set (list, or comma-separated string). Replaces the existing tags on the candidate, does not merge.")

    custom_fields_map: Dict[str, str] = Field(default_factory=dict, description="Upstream column -> Greenhouse custom field internal name (not its display label).")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="greenhouse", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'greenhouse').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add('greenhouse')

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "GreenhouseCandidateUpdateComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if not self.tags_column and not self.custom_fields_map:
            raise ValueError(
                "GreenhouseCandidateUpdateComponent: at least one of tags_column or "
                "custom_fields_map is required."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        conn = resource.get_connection()
                        if hasattr(conn, "execute") and hasattr(conn, "df"):
                            return conn.execute(query).df()
                        return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"GreenhouseCandidateUpdateComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_write(context, upstream):
            svc = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata={"rows_written": dg.MetadataValue.int(0)})

            if len(df) > _self.max_rows:
                context.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = {_self.candidate_id_column} | set(_self.custom_fields_map.keys())
            if _self.tags_column:
                required_cols.add(_self.tags_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            def _coerce_tags(value):
                if value is None:
                    return None
                if isinstance(value, (list, tuple)):
                    return [str(v) for v in value]
                return [s.strip() for s in str(value).split(",") if s.strip()]

            success_count = 0
            skipped_no_key = 0
            errors: List[str] = []
            for _, row in df.iterrows():
                candidate_id = _row_value(row[_self.candidate_id_column])
                if not candidate_id:
                    skipped_no_key += 1
                    continue
                tags = _coerce_tags(row[_self.tags_column]) if _self.tags_column else None
                custom_fields = {}
                for col, gh_field in _self.custom_fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        custom_fields[gh_field] = v
                try:
                    svc.update_candidate(
                        str(candidate_id),
                        tags=tags,
                        custom_fields=custom_fields or None,
                    )
                    success_count += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{candidate_id}: {type(e).__name__}: {e}")

            context.log.info(
                f"Greenhouse candidate update: {success_count} succeeded, {len(errors)} errors, "
                f"{skipped_no_key} skipped (missing {_self.candidate_id_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))
            metadata = {
                "rows_updated": dg.MetadataValue.int(success_count),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])
            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or ("Update Greenhouse candidate tags/custom fields (existing candidates only, not an upsert)."),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_write(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_write(context, upstream)

        return dg.Definitions(assets=[_asset])
