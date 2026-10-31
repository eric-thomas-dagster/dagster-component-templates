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


class BasecampTodoUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Basecamp to-dos.

    Example:
        ```yaml
        type: dagster_community_components.BasecampTodoUpsertComponent
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
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current static/dynamic partition key.",
    )
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'basecamp').")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
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
            partitions_def=partitions_def,
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
                df = _filter_df_to_partition(df, context, _self.partition_date_column, _self.partition_static_column)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, _filter_df_to_partition(upstream, context, _self.partition_date_column, _self.partition_static_column))

        return dg.Definitions(assets=[_asset])


# BEGIN reverse-etl partition helpers (inlined per component; keep copies identical, see tests/test_partition_port_reverse_etl.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run or a non-DataFrame input.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the input is returned unchanged."""
    import pandas as pd

    if not hasattr(df, "columns") or not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df
# END reverse-etl partition helpers
