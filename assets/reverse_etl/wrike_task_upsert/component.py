"""DataFrame -> Wrike task upsert.

Wrike has no native "upsert" endpoint for tasks. This component mirrors an
upstream DataFrame into Wrike tasks in a target Folder, matching rows to
existing tasks via a stable **key marker** embedded in the task
description (`<!-- dagster-key: INC-1001 -->`) -- the same body-marker
pattern this repo already uses for `github_issue_upsert` / `clickup_task_upsert`
/ `trello_card_upsert` (Wrike custom fields require a per-account custom
field ID set up ahead of time; a description marker needs no such setup).

Every Wrike API response wraps its payload in `{"kind": ..., "data": [...]}`
-- even single-record create/update calls return a one-element `data` list.

Matches -> updated (title, description, status, importance, due date).
Misses -> created.

Pairs with:
  - ``wrike_resource`` -- Bearer token connection (required)
"""
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_MARKER_RE = re.compile(r"<!-- dagster-key: ([^>]+?) -->")

# Wrike's own status vocabulary (Task.status).
_VALID_STATUSES = {"Active", "Completed", "Deferred", "Cancelled"}
# Wrike's own importance vocabulary (Task.importance).
_VALID_IMPORTANCE = {"High", "Normal", "Low"}


def _make_description(raw_description: str, key: str) -> str:
    marker = f"<!-- dagster-key: {key} -->"
    body_no_marker = _MARKER_RE.sub("", raw_description or "").lstrip("\n")
    return f"{marker}\n\n{body_no_marker}".rstrip()


def _extract_key(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    m = _MARKER_RE.search(description)
    return m.group(1).strip() if m else None


def _call_wrike_api(resource, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
    """Isolates the one external-API boundary (Wrike's REST v4 API) so it
    can be monkeypatched wholesale in tests -- mirrors this repo's "mock
    only the paid/external call" test convention."""
    return resource.request(method, path, params=params, json_body=json_body)


class WrikeTaskUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Wrike tasks.

    Example:
        ```yaml
        type: dagster_component_templates.WrikeTaskUpsertComponent
        attributes:
          asset_name: wrike_incidents_mirror
          upstream_asset_key: incidents_seed
          resource_key: wrike_resource
          folder_id: "IEAABBCC123456"
          key_column: incident_id
          title_column: name
          description_column: description
          status_column: status
          importance_column: severity
          due_date_column: due_date
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
        default="wrike_resource",
        description="Resource key registered by WrikeResourceComponent.",
    )

    folder_id: str = Field(description="Target Wrike Folder ID tasks are created/matched in.")
    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written into each "
            "task's description as `<!-- dagster-key: <value> -->` and used "
            "to match rows to existing tasks on subsequent runs."
        ),
    )
    title_column: str = Field(description="Column holding the task title.")
    description_column: Optional[str] = Field(default=None, description="Column holding the task description (HTML).")
    status_column: Optional[str] = Field(
        default=None,
        description="Column holding a Wrike status: 'Active', 'Completed', 'Deferred', or 'Cancelled'. Unrecognized values are skipped (logged), not a hard failure.",
    )
    importance_column: Optional[str] = Field(
        default=None,
        description="Column holding a Wrike importance: 'High', 'Normal', or 'Low'. Unrecognized values are skipped (logged), not a hard failure.",
    )
    due_date_column: Optional[str] = Field(
        default=None, description="Column holding an ISO-8601 due date (e.g. '2026-01-01'), sent as dates.due."
    )

    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="wrike", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'wrike').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("wrike")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "WrikeTaskUpsertComponent: supply exactly one of "
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
                        with resource.get_connection() as conn:
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
            raise ValueError(f"WrikeTaskUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            wrike = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}.")
                df = df.head(_self.batch_size)

            required_cols = {_self.key_column, _self.title_column}
            for c in (_self.description_column, _self.status_column, _self.importance_column, _self.due_date_column):
                if c:
                    required_cols.add(c)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # -- Index existing tasks in the folder by dagster-key marker --
            existing_by_key: Dict[str, dict] = {}
            resp = _call_wrike_api(
                wrike, "GET", f"folders/{_self.folder_id}/tasks",
                params={"fields": '["description"]'},
            )
            for task in resp.get("data") or []:
                key = _extract_key(task.get("description"))
                if key:
                    existing_by_key[key] = task

            created = 0
            updated = 0
            errors: List[str] = []
            skipped_no_key = 0

            for _, row in df.iterrows():
                key_val = _row_value(row[_self.key_column])
                if key_val is None:
                    skipped_no_key += 1
                    continue
                key_str = str(key_val)

                title = str(row[_self.title_column])
                raw_desc = str(row[_self.description_column]) if _self.description_column and _row_value(row[_self.description_column]) is not None else ""
                description = _make_description(raw_desc, key_str)

                body: dict = {"title": title, "description": description}
                if _self.status_column:
                    sv = _row_value(row[_self.status_column])
                    if sv is not None:
                        status = str(sv).strip().capitalize()
                        if status in _VALID_STATUSES:
                            body["status"] = status
                        else:
                            context.log.warning(f"Wrike status {sv!r} not in {_VALID_STATUSES} -- skipped for {key_str}.")
                if _self.importance_column:
                    iv = _row_value(row[_self.importance_column])
                    if iv is not None:
                        importance = str(iv).strip().capitalize()
                        if importance in _VALID_IMPORTANCE:
                            body["importance"] = importance
                        else:
                            context.log.warning(f"Wrike importance {iv!r} not in {_VALID_IMPORTANCE} -- skipped for {key_str}.")
                if _self.due_date_column:
                    due_val = _row_value(row[_self.due_date_column])
                    if due_val is not None:
                        body["dates"] = {"due": str(due_val)}

                existing = existing_by_key.get(key_str)
                try:
                    if existing:
                        resp = _call_wrike_api(wrike, "PUT", f"tasks/{existing['id']}", json_body=body)
                        updated += 1
                    else:
                        create_body = dict(body)
                        resp = _call_wrike_api(wrike, "POST", f"folders/{_self.folder_id}/tasks", json_body=create_body)
                        new_task = (resp.get("data") or [{}])[0]
                        if new_task.get("id"):
                            existing_by_key[key_str] = new_task
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{key_str}: {type(e).__name__}: {e}")

            context.log.info(
                f"Wrike upsert complete: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing {_self.key_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "wrike_folder_id": dg.MetadataValue.text(_self.folder_id),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
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
            description=_self.description or (f"Upsert DataFrame rows into Wrike folder {_self.folder_id}."),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
