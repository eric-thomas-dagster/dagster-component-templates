"""DataFrame -> Asana task create (create-only, no upsert).

Asana has no upsert concept for tasks -- every run creates one new task
per upstream row. This is for "spin up a task per row" reverse-ETL
patterns (e.g. one task per data-quality failure or flagged record), NOT
for mirroring a table that should stay in sync over time -- re-running
this on the same data creates duplicate tasks.

Pairs with:
  - ``asana_resource`` -- personal access token auth (required)"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field
class AsanaTaskCreateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create one Asana task per row of an upstream DataFrame.

    Example:
        ```yaml
        type: dagster_component_templates.AsanaTaskCreateComponent
        attributes:
          asset_name: asana_dq_failure_tasks
          upstream_asset_key: dq_check_failures
          resource_key: asana_resource
          name_column: failure_summary
          notes_column: failure_detail
          project_gids: ["1201234567890"]
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
        default="asana_resource",
        description="Resource key registered by AsanaResourceComponent.",
    )

    name_column: str = Field(description="Upstream column holding the task name.")

    notes_column: Optional[str] = Field(default=None, description="Upstream column holding the task notes/description.")

    project_gids: Optional[List[str]] = Field(default=None, description="Asana project GIDs to add every created task to. Required if the resource has no workspace_gid configured.")

    assignee_column: Optional[str] = Field(default=None, description="Upstream column holding an Asana user GID or email to assign the task to.")

    custom_fields_map: Dict[str, str] = Field(default_factory=dict, description="Upstream column -> Asana custom_field GID.")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="asana", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'asana').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add('asana')

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "AsanaTaskCreateComponent: supply exactly one of "
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
            raise ValueError(f"AsanaTaskCreateComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            required_cols = {_self.name_column} | set(_self.custom_fields_map.keys())
            if _self.notes_column:
                required_cols.add(_self.notes_column)
            if _self.assignee_column:
                required_cols.add(_self.assignee_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            success_count = 0
            skipped_no_name = 0
            errors: List[str] = []
            for _, row in df.iterrows():
                name = _row_value(row[_self.name_column])
                if not name:
                    skipped_no_name += 1
                    continue
                notes = _row_value(row[_self.notes_column]) if _self.notes_column else None
                assignee = _row_value(row[_self.assignee_column]) if _self.assignee_column else None
                custom_fields = {}
                for col, gid in _self.custom_fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        custom_fields[gid] = v
                try:
                    svc.create_task(
                        name=str(name),
                        notes=str(notes) if notes else None,
                        projects=_self.project_gids,
                        custom_fields=custom_fields or None,
                        assignee=str(assignee) if assignee else None,
                    )
                    success_count += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{name}: {type(e).__name__}: {e}")

            context.log.info(
                f"Asana task create: {success_count} created, {len(errors)} errors, "
                f"{skipped_no_name} skipped (missing {_self.name_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))
            metadata = {
                "rows_created": dg.MetadataValue.int(success_count),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_name),
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
            description=_self.description or ("Create one Asana task per upstream row."),
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
