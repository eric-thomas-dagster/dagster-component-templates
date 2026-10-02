"""DataFrame -> Basecamp to-do upsert.

Basecamp 3/4 has no native "upsert" endpoint for to-dos, and (like Trello)
no way to attach a custom external-id field to a to-do. This component
mirrors an upstream DataFrame into Basecamp to-dos in a target to-do list,
matching rows to existing to-dos via a stable **key marker** embedded in
the to-do's `description` (`<!-- dagster-key: INC-1001 -->`) -- the same
body-marker pattern this repo already uses for `github_issue_upsert` /
`clickup_task_upsert` / `trello_card_upsert` / `wrike_task_upsert`.

Matches -> updated (content, description, due_on) + completion toggled via
Basecamp's separate completion sub-resource. Misses -> created.

Basecamp's REST shape is unusual among this repo's reverse-ETL
connectors -- verified directly against basecamp/bc3-api's own docs:
  - Every URL is scoped under a per-account path:
    `https://3.basecampapi.com/<account_id>/...` (handled by
    ``basecamp_resource``, not this component).
  - A to-do's title field is literally called `content`, not `title`/`name`.
  - Updating a to-do (`PUT .../todos/{id}.json`) requires passing ALL
    fields every time -- omitting one (e.g. `assignee_ids`) CLEARS it
    rather than leaving it unchanged. This component always sends the full
    field set it manages on every update.
  - Completion is a separate sub-resource:
    `POST .../todos/{id}/completion.json` (mark done) /
    `DELETE .../todos/{id}/completion.json` (mark not done) -- there is no
    `completed` field on the main PUT body.
  - Collections paginate via the `Link` response header (RFC 5988), but
    Basecamp's own docs confirm the next-page URL is just `?page=N+1` --
    this component increments `page` directly rather than threading raw
    HTTP headers through the resource abstraction.

Pairs with:
  - ``basecamp_resource`` -- per-account URL + OAuth2 Bearer connection (required)
"""
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_MARKER_RE = re.compile(r"<!-- dagster-key: ([^>]+?) -->")


def _make_description(raw_description: str, key: str) -> str:
    marker = f"<!-- dagster-key: {key} -->"
    body_no_marker = _MARKER_RE.sub("", raw_description or "").lstrip("\n")
    return f"{marker}\n\n{body_no_marker}".rstrip()


def _extract_key(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    m = _MARKER_RE.search(description)
    return m.group(1).strip() if m else None


def _call_basecamp_api(resource, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> Any:
    """Isolates the one external-API boundary (Basecamp's REST API) so it
    can be monkeypatched wholesale in tests -- mirrors this repo's "mock
    only the paid/external call" test convention."""
    return resource.request(method, path, params=params, json_body=json_body)


class BasecampTodoUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Basecamp to-dos.

    Example:
        ```yaml
        type: dagster_component_templates.BasecampTodoUpsertComponent
        attributes:
          asset_name: basecamp_incidents_mirror
          upstream_asset_key: incidents_seed
          resource_key: basecamp_resource
          project_id: "2085958505"
          todolist_id: "1069480012"
          key_column: incident_id
          content_column: name
          description_column: description
          due_on_column: due_date
          completed_column: is_resolved
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
        default="basecamp_resource",
        description="Resource key registered by BasecampResourceComponent.",
    )

    project_id: str = Field(description="Target Basecamp project (bucket) ID.")
    todolist_id: str = Field(description="Target Basecamp to-do list ID to-dos are created/matched in.")
    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written into each "
            "to-do's description as `<!-- dagster-key: <value> -->` and used "
            "to match rows to existing to-dos on subsequent runs."
        ),
    )
    content_column: str = Field(
        description="Column holding the to-do's title text (Basecamp calls this field `content`, not `title`)."
    )
    description_column: Optional[str] = Field(default=None, description="Column holding the to-do description (HTML).")
    due_on_column: Optional[str] = Field(default=None, description="Column holding an ISO-8601 due date (e.g. '2026-01-01').")
    assignee_ids_column: Optional[str] = Field(
        default=None, description="Column holding Basecamp person IDs to assign. Accepts a list, or a comma-separated string of ints."
    )
    completed_column: Optional[str] = Field(
        default=None,
        description="Column holding a boolean/'true'/'false'. Toggles completion via Basecamp's separate completion sub-resource.",
    )

    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="basecamp", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'basecamp').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("basecamp")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "BasecampTodoUpsertComponent: supply exactly one of "
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
            raise ValueError(f"BasecampTodoUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _coerce_bool(v) -> Optional[bool]:
            if v is None:
                return None
            if isinstance(v, bool):
                return v
            s = str(v).strip().lower()
            if s in ("true", "1", "yes"):
                return True
            if s in ("false", "0", "no"):
                return False
            return None

        def _list_todos_paginated(context, basecamp, params_base: dict) -> List[dict]:
            """Pages through
            `buckets/{project}/todolists/{list}/todos.json`. Basecamp's own
            docs confirm the Link-header next-page URL is just
            `?page=N+1`, so this increments `page` directly rather than
            threading raw HTTP headers through the resource abstraction. An
            empty page always means "no more" regardless of geared page
            size (15/30/50/100)."""
            all_todos: List[dict] = []
            page = 1
            while True:
                params = dict(params_base)
                params["page"] = page
                batch = _call_basecamp_api(
                    basecamp, "GET", f"buckets/{_self.project_id}/todolists/{_self.todolist_id}/todos.json",
                    params=params,
                )
                if not batch:
                    break
                all_todos.extend(batch)
                page += 1
                if page > 1000:  # hard safety cap against runaway pagination
                    context.log.warning("Basecamp to-do listing exceeded 1000 pages -- stopping early.")
                    break
            return all_todos

        def _run_upsert(context, upstream):
            basecamp = getattr(context.resources, _self.resource_key)

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

            required_cols = {_self.key_column, _self.content_column}
            for c in (_self.description_column, _self.due_on_column, _self.assignee_ids_column, _self.completed_column):
                if c:
                    required_cols.add(c)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _coerce_list(value) -> List[str]:
                if value is None:
                    return []
                if isinstance(value, float) and pd.isna(value):
                    return []
                if isinstance(value, (list, tuple)):
                    return [str(v) for v in value]
                return [s.strip() for s in str(value).split(",") if s.strip()]

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # -- Index existing to-dos by dagster-key marker ----------------
            # Active (pending) + completed=true, to catch to-dos regardless
            # of current completion state.
            existing_by_key: Dict[str, dict] = {}
            for todos_batch in (
                _list_todos_paginated(context, basecamp, {}),
                _list_todos_paginated(context, basecamp, {"completed": "true"}),
            ):
                for todo in todos_batch:
                    key = _extract_key(todo.get("description"))
                    if key and key not in existing_by_key:
                        existing_by_key[key] = todo

            created = 0
            updated = 0
            completed_toggled = 0
            errors: List[str] = []
            skipped_no_key = 0

            for _, row in df.iterrows():
                key_val = _row_value(row[_self.key_column])
                if key_val is None:
                    skipped_no_key += 1
                    continue
                key_str = str(key_val)

                content = str(row[_self.content_column])
                raw_desc = str(row[_self.description_column]) if _self.description_column and _row_value(row[_self.description_column]) is not None else ""
                description = _make_description(raw_desc, key_str)

                # Basecamp's PUT requires the full field set every time --
                # an omitted field clears it, so `content` is always sent.
                body: dict = {"content": content, "description": description}
                if _self.due_on_column:
                    dv = _row_value(row[_self.due_on_column])
                    if dv is not None:
                        body["due_on"] = str(dv)
                if _self.assignee_ids_column:
                    assignee_ids = [int(a) for a in _coerce_list(row[_self.assignee_ids_column]) if a.lstrip("-").isdigit()]
                    body["assignee_ids"] = assignee_ids

                want_completed = None
                if _self.completed_column:
                    want_completed = _coerce_bool(_row_value(row[_self.completed_column]))

                existing = existing_by_key.get(key_str)
                try:
                    if existing:
                        todo_id = existing["id"]
                        _call_basecamp_api(
                            basecamp, "PUT", f"buckets/{_self.project_id}/todos/{todo_id}.json", json_body=body,
                        )
                        updated += 1
                    else:
                        new_todo = _call_basecamp_api(
                            basecamp, "POST",
                            f"buckets/{_self.project_id}/todolists/{_self.todolist_id}/todos.json",
                            json_body=body,
                        )
                        existing_by_key[key_str] = new_todo
                        todo_id = new_todo.get("id")
                        created += 1

                    if want_completed is not None and todo_id is not None:
                        completion_path = f"buckets/{_self.project_id}/todos/{todo_id}/completion.json"
                        if want_completed:
                            _call_basecamp_api(basecamp, "POST", completion_path)
                        else:
                            _call_basecamp_api(basecamp, "DELETE", completion_path)
                        completed_toggled += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{key_str}: {type(e).__name__}: {e}")

            context.log.info(
                f"Basecamp upsert complete: {created} created, {updated} updated, "
                f"{completed_toggled} completion toggles, {len(errors)} errors, "
                f"{skipped_no_key} skipped (missing {_self.key_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "basecamp_project_id": dg.MetadataValue.text(_self.project_id),
                "basecamp_todolist_id": dg.MetadataValue.text(_self.todolist_id),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_completion_toggled": dg.MetadataValue.int(completed_toggled),
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
            description=_self.description or (
                f"Upsert DataFrame rows into Basecamp project {_self.project_id}, to-do list {_self.todolist_id}."
            ),
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
