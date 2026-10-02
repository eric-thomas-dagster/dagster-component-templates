"""DataFrame -> ClickUp task upsert.

ClickUp has no native "upsert" endpoint for tasks. This component mirrors
an upstream DataFrame into ClickUp tasks in a target List, matching rows
to existing tasks via a stable **key marker** embedded in the task
description (e.g. `<!-- dagster-key: INC-1001 -->`) -- the same
body-marker pattern this repo already uses for `github_issue_upsert`
(ClickUp tasks have no universal "External ID" field the way Salesforce
SObjects do, so a marker is the robust zero-config option here).

Matches -> updated (name, description, status, priority, assignees, tags).
Misses -> created.

Pairs with:
  - ``clickup_resource`` -- API token connection (required)
"""
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_MARKER_RE = re.compile(r"<!-- dagster-key: ([^>]+?) -->")


def _make_description(raw_description: str, key: str) -> str:
    """Prepend / replace the key marker line in the task description."""
    marker = f"<!-- dagster-key: {key} -->"
    body_no_marker = _MARKER_RE.sub("", raw_description or "").lstrip("\n")
    return f"{marker}\n\n{body_no_marker}".rstrip()


def _extract_key(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    m = _MARKER_RE.search(description)
    return m.group(1).strip() if m else None


def _call_clickup_api(resource, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
    """Isolates the one external-API boundary (ClickUp's REST v2 API) so it
    can be monkeypatched wholesale in tests -- mirrors this repo's "mock
    only the paid/external call" test convention."""
    return resource.request(method, path, params=params, json_body=json_body)


class ClickUpTaskUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into ClickUp tasks.

    Example:
        ```yaml
        type: dagster_component_templates.ClickUpTaskUpsertComponent
        attributes:
          asset_name: clickup_incidents_mirror
          upstream_asset_key: incidents_seed
          resource_key: clickup_resource
          list_id: "901234567"
          key_column: incident_id
          name_column: name
          description_column: description
          status_column: status
          priority_column: severity
          assignees_column: assignee_user_ids
          tags_column: tags
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
        default="clickup_resource",
        description="Resource key registered by ClickUpResourceComponent.",
    )

    list_id: str = Field(description="Target ClickUp List ID tasks are created/matched in.")
    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written into each "
            "task's description as `<!-- dagster-key: <value> -->` and used "
            "to match rows to existing tasks on subsequent runs."
        ),
    )
    name_column: str = Field(description="Column holding the task name.")
    description_column: Optional[str] = Field(
        default=None, description="Column holding the task description (plain text/markdown)."
    )
    status_column: Optional[str] = Field(
        default=None,
        description="Column holding a ClickUp status name (must match a status configured on the target List, e.g. 'in progress', 'complete').",
    )
    priority_column: Optional[str] = Field(
        default=None,
        description="Column holding priority. Accepts ClickUp's int encoding (1=Urgent, 2=High, 3=Normal, 4=Low) or the name directly.",
    )
    assignees_column: Optional[str] = Field(
        default=None, description="Column holding ClickUp user IDs to assign. Accepts a list, or a comma-separated string of ints."
    )
    tags_column: Optional[str] = Field(
        default=None, description="Column holding tag names. Accepts a list, or a comma-separated string."
    )

    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="clickup", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'clickup').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("clickup")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "ClickUpTaskUpsertComponent: supply exactly one of "
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
            raise ValueError(f"ClickUpTaskUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        _PRIORITY_NAME_TO_INT = {"urgent": 1, "high": 2, "normal": 3, "medium": 3, "low": 4}

        def _run_upsert(context, upstream):
            clickup = getattr(context.resources, _self.resource_key)

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

            required_cols = {_self.key_column, _self.name_column}
            for c in (_self.description_column, _self.status_column, _self.priority_column, _self.assignees_column, _self.tags_column):
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

            # -- Index existing tasks in the List by dagster-key marker ----
            existing_by_key: Dict[str, dict] = {}
            page = 0
            while True:
                resp = _call_clickup_api(
                    clickup, "GET", f"list/{_self.list_id}/task",
                    params={"include_closed": "true", "page": page, "subtasks": "true"},
                )
                page_tasks = resp.get("tasks") or []
                for task in page_tasks:
                    key = _extract_key(task.get("description"))
                    if key:
                        existing_by_key[key] = task
                if len(page_tasks) < 100:
                    break
                page += 1
                if page > 1000:  # hard safety cap against runaway pagination
                    context.log.warning("ClickUp task listing exceeded 1000 pages -- stopping early.")
                    break

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

                name = str(row[_self.name_column])
                raw_desc = str(row[_self.description_column]) if _self.description_column and _row_value(row[_self.description_column]) is not None else ""
                description = _make_description(raw_desc, key_str)

                body: dict = {"name": name, "description": description}
                if _self.status_column:
                    sv = _row_value(row[_self.status_column])
                    if sv is not None:
                        body["status"] = str(sv)
                if _self.priority_column:
                    pv = _row_value(row[_self.priority_column])
                    if pv is not None:
                        if isinstance(pv, (int, float)) and not isinstance(pv, bool):
                            body["priority"] = int(pv)
                        else:
                            body["priority"] = _PRIORITY_NAME_TO_INT.get(str(pv).strip().lower())
                if _self.assignees_column:
                    assignees = [int(a) for a in _coerce_list(row[_self.assignees_column]) if a.lstrip("-").isdigit()]
                    if assignees:
                        body["assignees"] = assignees
                if _self.tags_column:
                    tag_names = _coerce_list(row[_self.tags_column])
                    if tag_names:
                        body["tags"] = tag_names

                existing = existing_by_key.get(key_str)
                try:
                    if existing:
                        _call_clickup_api(clickup, "PUT", f"task/{existing['id']}", json_body=body)
                        updated += 1
                    else:
                        created_task = _call_clickup_api(clickup, "POST", f"list/{_self.list_id}/task", json_body=body)
                        existing_by_key[key_str] = created_task
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{key_str}: {type(e).__name__}: {e}")

            context.log.info(
                f"ClickUp upsert complete: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing {_self.key_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "clickup_list_id": dg.MetadataValue.text(_self.list_id),
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
            description=_self.description or (f"Upsert DataFrame rows into ClickUp List {_self.list_id}."),
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
