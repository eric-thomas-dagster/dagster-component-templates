"""DataFrame -> Jira Service Management customer request upsert.

Mirrors a source DataFrame into Jira Service Management (JSM) customer
requests via search-then-write per row:
  1. JQL search (`POST /rest/api/3/search/jql`, scoped by `project` and a
     match on `key_field`) to find an existing request.
  2. If found: `PUT /rest/api/3/issue/{key}` (core API) to update fields.
  3. If not found: `POST /rest/servicedeskapi/request` to create a new
     customer request (needs `service_desk_id` + `request_type_id`).

JSM has no single-call native upsert and no servicedeskapi-scoped search,
so matching goes through the core JQL search endpoint — same shape as
`servicenow_record_upsert`'s search-then-write pattern.

JQL custom-field quirk (real, confirmed Atlassian behavior, documented
here because it trips people up constantly): to filter on a custom field
by its numeric ID, JQL requires `cf[10050] = "value"` syntax, NOT
`customfield_10050 = "value"` (the latter does not parse). Built-in
fields (`summary`, `labels`, etc.) use their plain name directly, e.g.
`summary ~ "value"`. `_jql_match_clause()` below builds the right clause
for either case.

Two source shapes:
  1. `upstream_asset_key:` — chain from an upstream Dagster asset that
     produces a pandas DataFrame. Standard Dagster lineage pattern.
  2. `source:` block — read the DataFrame inline at run time, no upstream
     asset required. Supports:
       - kind: sql — query a database via a Dagster resource (`resource_key`
         with `.get_engine()` / `.get_connection()`) OR a raw
         `database_url_env_var`.
       - kind: csv — read a CSV file at `path`.
       - kind: inline — literal rows in YAML.

Pairs with:
  - ``jira_service_management_resource`` — connection + auth (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _jql_match_clause(key_field: str, value: Any) -> str:
    """Build the JQL clause that matches a single field to a value.

    Real, confirmed Atlassian quirk: custom fields must be referenced by
    numeric ID via `cf[10050] = "value"` syntax in JQL -- the
    `customfield_10050 = "value"` form (which works fine in the REST API
    body for *writes*) does NOT parse as JQL for *reads*. Built-in fields
    (summary, labels, etc.) use their plain name directly.
    """
    escaped = str(value).replace('"', '\\"')
    if key_field.startswith("customfield_"):
        numeric_id = key_field[len("customfield_"):]
        return f'cf[{numeric_id}] = "{escaped}"'
    return f'{key_field} = "{escaped}"'


class JiraServiceManagementRequestUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from a source DataFrame into Jira Service Management customer requests.

    Example — upstream asset:
        ```yaml
        type: dagster_component_templates.JiraServiceManagementRequestUpsertComponent
        attributes:
          asset_name: jsm_requests_from_tickets
          upstream_asset_key: support_tickets
          resource_key: jira_service_management_resource
          project_key: ITSM
          service_desk_id: "1"
          request_type_id: "10"
          key_field: customfield_10050
          fields_map:
            ticket_id: customfield_10050
            title: summary
            body: description
        ```

    Example — inline SQL source (no upstream asset):
        ```yaml
        attributes:
          asset_name: jsm_requests_from_tickets
          source:
            kind: sql
            resource_key: analytics_postgres
            query: |
              SELECT ticket_id, title, body
              FROM analytics.open_tickets
              WHERE created_at > now() - interval '1 day'
          resource_key: jira_service_management_resource
          project_key: ITSM
          service_desk_id: "1"
          request_type_id: "10"
          key_field: customfield_10050
          fields_map: {...}
        ```

    For every row in the source DataFrame:
      1. JQL search scoped to `project_key`, matching `key_field == <row value>`.
      2. If found, PUT the matched issue's key with the mapped fields (core API).
      3. If not found, POST a new customer request (servicedeskapi).
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes — supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes:\n"
            "  {kind: sql, resource_key: <name>, query: <sql>}\n"
            "  {kind: sql, database_url_env_var: <env>, query: <sql>}\n"
            "  {kind: csv, path: <path>, read_csv_kwargs: {...}}\n"
            "  {kind: inline, rows: [{...}, ...]}"
        ),
    )

    resource_key: str = Field(
        default="jira_service_management_resource",
        description="Resource key registered by JiraServiceManagementResourceComponent.",
    )

    project_key: str = Field(
        description="Jira project key that scopes the JQL search, e.g. 'ITSM'.",
    )
    service_desk_id: str = Field(
        description="Target service desk ID for `create_request` (servicedeskapi).",
    )
    request_type_id: str = Field(
        description="Target request type ID for `create_request` (servicedeskapi).",
    )
    key_field: str = Field(
        description=(
            "Jira field ID used to match existing requests (e.g. "
            "'customfield_10050', or a plain field like 'labels'). Must be "
            "present in `fields_map` values."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Source column -> Jira field ID ('summary', 'description', "
            "'customfield_10050', etc.). Becomes `requestFieldValues` keys on "
            "create, and `fields` keys on update."
        ),
    )
    comment_column: Optional[str] = Field(
        default=None,
        description=(
            "Source column holding comment text. If set, posted as an internal "
            "(non-public) comment via `add_request_comment` on BOTH create and "
            "update paths."
        ),
    )
    batch_size: int = Field(
        default=500,
        description="Max rows per run (safety cap). Each row is one search + one write.",
    )

    group_name: Optional[str] = Field(
        default="jira_service_management", description="Dagster asset group name."
    )
    description: Optional[str] = Field(
        default=None, description="Asset description."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners."
    )
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'jira_service_management').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("jira_service_management")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "JiraServiceManagementRequestUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate key_field is in fields_map values — otherwise the upsert
        # can't match and every row will be created as new.
        mapped_fields = set(self.fields_map.values())
        if self.key_field not in mapped_fields:
            raise ValueError(
                f"JiraServiceManagementRequestUpsertComponent: key_field={self.key_field!r} "
                f"not present in fields_map values. key_field must be a Jira field "
                f"you're upserting. fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None

        # Extra required_resource_keys when source: kind=sql uses a resource.
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            source_rk = self.source.get("resource_key")
            if source_rk:
                extra_rks.add(source_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
        def _resolve_source_df(exec_ctx):
            """Resolve DataFrame from `source:` config (called when use_source=True)."""
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
                        engine = resource.get_engine()
                        return pd.read_sql(query, engine)
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            # DuckDB fast path
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(
                        f"source kind=sql: resource {rk!r} must expose "
                        ".get_engine() or .get_connection()"
                    )
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError(
                    "source kind=sql requires 'resource_key' OR 'database_url_env_var'"
                )

            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))

            if kind == "inline":
                rows = src.get("rows") or []
                return pd.DataFrame(rows)

            raise ValueError(
                f"JiraServiceManagementRequestUpsertComponent source kind={kind!r} not "
                "supported (expected: sql / csv / inline)"
            )

        # ── Shared upsert body ─────────────────────────────────────────
        def _run_upsert(exec_ctx, df):
            svc = getattr(exec_ctx.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame([df]) if isinstance(df, dict) else pd.DataFrame(df)

            if len(df) == 0:
                exec_ctx.log.warning("Source DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_created": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                exec_ctx.log.warning(
                    f"Source has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            if _self.comment_column:
                required_cols.add(_self.comment_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in source: {missing_cols}. Available: {list(df.columns)}"
                )

            key_col = next(
                (col for col, jira_field in _self.fields_map.items() if jira_field == _self.key_field),
                None,
            )
            if key_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to key_field={_self.key_field!r}. "
                    f"fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            created = 0
            updated = 0
            errors: List[str] = []
            skipped_no_key = 0

            for i, row in df.iterrows():
                key_value = _row_value(row[key_col])
                if key_value is None or str(key_value).strip() == "":
                    skipped_no_key += 1
                    continue

                body: dict = {}
                for col, jira_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        body[jira_field] = v
                if not body:
                    continue

                comment_text = None
                if _self.comment_column:
                    comment_text = _row_value(row[_self.comment_column])

                try:
                    jql = (
                        f'project = "{_self.project_key}" AND '
                        f'{_jql_match_clause(_self.key_field, key_value)}'
                    )
                    issues = svc.search_issues_jql(jql)

                    if issues:
                        issue_key = issues[0]["key"]
                        svc.update_issue_fields(issue_key, body)
                        if comment_text:
                            svc.add_request_comment(issue_key, comment_text, public=False)
                        updated += 1
                    else:
                        result = svc.create_request(
                            service_desk_id=_self.service_desk_id,
                            request_type_id=_self.request_type_id,
                            request_field_values=body,
                        )
                        new_key = result.get("issueKey")
                        if comment_text and new_key:
                            svc.add_request_comment(new_key, comment_text, public=False)
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (key={key_value}): {type(e).__name__}: {e}")

            exec_ctx.log.info(
                f"Jira Service Management upsert into project {_self.project_key}: "
                f"{created} created, {updated} updated, {skipped_no_key} skipped "
                f"(no key), {len(errors)} errors (matched on {_self.key_field})."
            )
            if errors:
                exec_ctx.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "project_key": dg.MetadataValue.text(_self.project_key),
                "key_field": dg.MetadataValue.text(_self.key_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
                "rows_errored": dg.MetadataValue.int(len(errors)),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        # ── Two asset shapes based on source configuration ─────────────
        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Jira Service Management project "
                f"{_self.project_key} (match on {_self.key_field})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(
                required_resource_keys=required_rks,
                **common_kwargs,
            )
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
