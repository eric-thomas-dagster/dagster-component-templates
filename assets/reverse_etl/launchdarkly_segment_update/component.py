"""DataFrame -> LaunchDarkly segment target update (add/remove context keys).

Adds or removes context keys (typically user keys) from a LaunchDarkly
segment's target list via a semantic patch. Use this to push a computed
cohort (e.g. beta testers, at-risk accounts, internal staff) from a
warehouse into a segment for feature-flag targeting rules to reference.

Every row is treated as an ADD unless in_segment_column evaluates falsy,
in which case that row's key is REMOVED instead -- so a single sync can
both add newly-qualifying contexts and evict ones that no longer qualify.

Pairs with:
  - ``launchdarkly`` resource -- API token auth + semantic-patch segment update (required)"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field
class LaunchDarklySegmentUpdateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Sync a computed cohort into a LaunchDarkly segment's target list.

    Example:
        ```yaml
        type: dagster_component_templates.LaunchDarklySegmentUpdateComponent
        attributes:
          asset_name: launchdarkly_beta_testers_sync
          upstream_asset_key: computed_beta_cohort
          resource_key: launchdarkly
          project_key: my-project
          env_key: production
          segment_key: beta-testers
          context_key_column: user_key
          in_segment_column: is_beta_tester
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
        default="launchdarkly",
        description="Resource key registered by LaunchDarklyResourceComponent.",
    )

    project_key: str = Field(description="LaunchDarkly project key.")

    env_key: str = Field(description="LaunchDarkly environment key (e.g. production, staging).")

    segment_key: str = Field(description="LaunchDarkly segment key to update.")

    context_key_column: str = Field(description="Upstream column holding the context key (e.g. user key) to add/remove.")

    in_segment_column: Optional[str] = Field(default=None, description="Upstream column (boolean-ish) indicating whether the row should be IN the segment. If unset, every row is treated as an add.")

    context_kind: str = Field(default="user", description="LaunchDarkly context kind these keys belong to.")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="launchdarkly", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'launchdarkly').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add('launchdarkly')

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "LaunchDarklySegmentUpdateComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
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
            raise ValueError(f"LaunchDarklySegmentUpdateComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            required_cols = {_self.context_key_column}
            if _self.in_segment_column:
                required_cols.add(_self.in_segment_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            def _is_truthy(v):
                if isinstance(v, str):
                    return v.strip().lower() not in ("", "false", "0", "no")
                return bool(v)

            add_keys: List[str] = []
            remove_keys: List[str] = []
            skipped_no_key = 0
            for _, row in df.iterrows():
                key = _row_value(row[_self.context_key_column])
                if not key:
                    skipped_no_key += 1
                    continue
                if _self.in_segment_column is not None and not _is_truthy(row[_self.in_segment_column]):
                    remove_keys.append(str(key))
                else:
                    add_keys.append(str(key))

            errors: List[str] = []
            try:
                if add_keys or remove_keys:
                    svc.add_segment_targets(
                        _self.project_key,
                        _self.env_key,
                        _self.segment_key,
                        add_keys=add_keys or None,
                        remove_keys=remove_keys or None,
                        context_kind=_self.context_kind,
                        comment=f"Dagster reverse-ETL sync ({len(add_keys)} added, {len(remove_keys)} removed)",
                    )
            except Exception as e:  # noqa: BLE001
                errors.append(f"{type(e).__name__}: {e}")

            success_count = len(add_keys) + len(remove_keys) if not errors else 0

            context.log.info(
                f"LaunchDarkly segment update on {_self.project_key}/{_self.env_key}/{_self.segment_key}: "
                f"{len(add_keys)} added, {len(remove_keys)} removed, {len(errors)} errors, "
                f"{skipped_no_key} skipped (missing {_self.context_key_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))
            metadata = {
                "rows_added": dg.MetadataValue.int(len(add_keys)),
                "rows_removed": dg.MetadataValue.int(len(remove_keys)),
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
            description=_self.description or (f"Sync computed cohort into LaunchDarkly segment {_self.project_key}/{_self.env_key}/{_self.segment_key}."),
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
