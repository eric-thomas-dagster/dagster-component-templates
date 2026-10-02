"""DataFrame -> Linear issue upsert (deterministic client-generated id).

Linear has no native "upsert" mutation, and -- unlike Jira/GitHub -- it
does NOT require a body/label marker hack to get idempotent create-or-
update: Linear's own `IssueCreateInput.id` field is documented as

    "The identifier in UUID v4 format. If none is provided, the backend
    will generate one."

(verified directly against Linear's public GraphQL schema, `input
IssueCreateInput { ... id: String ... }`). That means a client can mint
its OWN uuid for an issue up front. This component derives a stable
uuid5 from `(team_id, key_column value)` -- the same upstream key always
maps to the same Linear issue id, every run, with no body-marker
regex and no server-side search-by-text needed:

  1. One bulk `issues(filter: { id: { in: [...] } })` query checks which
     of this batch's derived ids already exist (Linear's `IDComparator`
     supports `in`, confirmed against the schema).
  2. Existing ids -> `issueUpdate(id, input)`.
  3. New ids -> `issueCreate(input: { id, teamId, title, ... })`.

Pairs with:
  - ``linear_resource`` -- GraphQL connection (required)
"""
import uuid
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

# Linear's own numeric priority encoding (IssueCreateInput.priority doc comment).
_PRIORITY_NAME_TO_INT = {
    "no priority": 0, "none": 0,
    "urgent": 1,
    "high": 2,
    "medium": 3, "normal": 3,
    "low": 4,
}

# Fixed namespace so the same (team_id, key) always derives the same uuid5,
# across runs and across processes.
_DAGSTER_KEY_NAMESPACE = uuid.UUID("c6e7a6b0-2f1e-4f6b-9b1a-9b6f6a7c5f2e")


def _derive_issue_id(team_id: str, key_str: str) -> str:
    return str(uuid.uuid5(_DAGSTER_KEY_NAMESPACE, f"{team_id}:{key_str}"))


def _call_linear_api(resource, query: str, variables: Optional[dict] = None) -> dict:
    """Isolates the one external-API boundary (Linear's GraphQL endpoint)
    so it can be monkeypatched wholesale in tests -- mirrors this repo's
    "mock only the paid/external call" test convention. Every query AND
    mutation this component issues funnels through this single function."""
    return resource.graphql(query, variables)


_EXISTS_QUERY = """
query($ids: [ID!]!) {
  issues(filter: { id: { in: $ids } }, first: 250) {
    nodes { id }
  }
}
"""

_LABELS_QUERY = """
query($teamId: String!) {
  team(id: $teamId) {
    labels(first: 250) { nodes { id name } }
  }
}
"""

_STATES_QUERY = """
query($teamId: String!) {
  team(id: $teamId) {
    states(first: 250) { nodes { id name } }
  }
}
"""

_USERS_QUERY = """
query {
  users(first: 250) { nodes { id email } }
}
"""

_CREATE_MUTATION = """
mutation($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue { id identifier }
  }
}
"""

_UPDATE_MUTATION = """
mutation($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) {
    success
    issue { id identifier }
  }
}
"""


class LinearIssueUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Linear issues.

    Example:
        ```yaml
        type: dagster_component_templates.LinearIssueUpsertComponent
        attributes:
          asset_name: linear_incidents_mirror
          upstream_asset_key: incidents_seed
          resource_key: linear_resource
          team_id: "a1b2c3d4-..."
          key_column: incident_id
          title_column: name
          description_column: description
          priority_column: severity        # 'Urgent'/'High'/'Medium'/'Low' or 0-4
          state_name_column: status         # e.g. 'Done', 'In Progress'
          label_names_column: labels        # comma-separated or list
          default_labels: [auto-synced]
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    # Two source shapes -- supply exactly one.
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
        default="linear_resource",
        description="Resource key registered by LinearResourceComponent.",
    )

    team_id: str = Field(
        description=(
            "Target Linear team UUID (required by IssueCreateInput.teamId). "
            "Find it via the Linear API `teams { nodes { id key name } }` query "
            "or the team's Settings page URL."
        ),
    )
    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. A uuid5 is derived "
            "from (team_id, key value) and passed as Linear's own "
            "`IssueCreateInput.id` -- the same key always maps to the same "
            "Linear issue, so re-runs update rather than duplicate, with no "
            "body-marker hack needed."
        ),
    )
    title_column: str = Field(description="Column holding the issue title.")
    description_column: Optional[str] = Field(
        default=None, description="Column holding the issue description (markdown)."
    )
    priority_column: Optional[str] = Field(
        default=None,
        description=(
            "Column holding issue priority. Accepts Linear's int encoding "
            "(0=No priority, 1=Urgent, 2=High, 3=Medium, 4=Low) or the name "
            "directly ('Urgent', 'High', 'Medium'/'Normal', 'Low', 'No priority')."
        ),
    )
    state_name_column: Optional[str] = Field(
        default=None,
        description=(
            "Column holding a workflow state name to move the issue to (e.g. "
            "'Done', 'In Progress', 'Todo'). Matched case-insensitively against "
            "the team's configured workflow states; unmatched names are a "
            "no-op for that row (logged as a warning), not a hard failure."
        ),
    )
    label_names_column: Optional[str] = Field(
        default=None,
        description="Column holding label names. Accepts a list, or a comma-separated string.",
    )
    default_labels: List[str] = Field(
        default_factory=list,
        description="Label names always applied on top of label_names_column.",
    )
    assignee_email_column: Optional[str] = Field(
        default=None,
        description="Column holding an assignee's email, matched against the workspace's users.",
    )

    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="linear", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'linear').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("linear")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "LinearIssueUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # -- Source resolver (self-contained per no-shared-code rule) -------
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
            raise ValueError(f"LinearIssueUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            linear = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = {_self.key_column, _self.title_column}
            for c in (
                _self.description_column, _self.priority_column, _self.state_name_column,
                _self.label_names_column, _self.assignee_email_column,
            ):
                if c:
                    required_cols.add(c)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

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

            # -- Lazy lookup caches: only queried if the component config --
            # -- actually needs them (keeps the common case to 1 round trip)
            _label_cache: Dict[str, str] = {}
            _state_cache: Dict[str, str] = {}
            _user_cache: Dict[str, str] = {}

            def _label_ids_for(names: List[str]) -> List[str]:
                if not _label_cache:
                    data = _call_linear_api(linear, _LABELS_QUERY, {"teamId": _self.team_id})
                    for node in data["team"]["labels"]["nodes"]:
                        _label_cache[node["name"].lower()] = node["id"]
                ids = []
                for name in names:
                    label_id = _label_cache.get(name.lower())
                    if label_id:
                        ids.append(label_id)
                    else:
                        context.log.warning(f"Linear label {name!r} not found on team {_self.team_id} -- skipped.")
                return ids

            def _state_id_for(name: str) -> Optional[str]:
                if not _state_cache:
                    data = _call_linear_api(linear, _STATES_QUERY, {"teamId": _self.team_id})
                    for node in data["team"]["states"]["nodes"]:
                        _state_cache[node["name"].lower()] = node["id"]
                state_id = _state_cache.get(name.lower())
                if not state_id:
                    context.log.warning(f"Linear workflow state {name!r} not found on team {_self.team_id} -- skipped.")
                return state_id

            def _user_id_for(email: str) -> Optional[str]:
                if not _user_cache:
                    data = _call_linear_api(linear, _USERS_QUERY)
                    for node in data["users"]["nodes"]:
                        if node.get("email"):
                            _user_cache[node["email"].lower()] = node["id"]
                user_id = _user_cache.get(email.lower())
                if not user_id:
                    context.log.warning(f"Linear user with email {email!r} not found -- assignee skipped.")
                return user_id

            # -- Build per-row payload + derived id up front ----------------
            rows_payload: List[dict] = []
            skipped_no_key = 0
            for _, row in df.iterrows():
                key_val = _row_value(row[_self.key_column])
                if key_val is None:
                    skipped_no_key += 1
                    continue
                key_str = str(key_val)
                issue_id = _derive_issue_id(_self.team_id, key_str)

                title = str(row[_self.title_column])
                desc_val = _row_value(row[_self.description_column]) if _self.description_column else None
                description = str(desc_val) if desc_val is not None else None

                priority = None
                if _self.priority_column:
                    pv = _row_value(row[_self.priority_column])
                    if pv is not None:
                        if isinstance(pv, (int, float)) and not isinstance(pv, bool):
                            priority = int(pv)
                        else:
                            priority = _PRIORITY_NAME_TO_INT.get(str(pv).strip().lower())

                label_names = list(_self.default_labels)
                if _self.label_names_column:
                    label_names += _coerce_list(row[_self.label_names_column])
                label_names = list(dict.fromkeys(label_names))

                state_name = None
                if _self.state_name_column:
                    sv = _row_value(row[_self.state_name_column])
                    state_name = str(sv) if sv is not None else None

                assignee_email = None
                if _self.assignee_email_column:
                    av = _row_value(row[_self.assignee_email_column])
                    assignee_email = str(av) if av is not None else None

                rows_payload.append({
                    "key_str": key_str,
                    "issue_id": issue_id,
                    "title": title,
                    "description": description,
                    "priority": priority,
                    "label_names": label_names,
                    "state_name": state_name,
                    "assignee_email": assignee_email,
                })

            if not rows_payload:
                context.log.warning("No rows with a valid key -- nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            # -- One bulk existence check (chunked at 200 ids/request) ------
            all_ids = [r["issue_id"] for r in rows_payload]
            existing_ids: set = set()
            for chunk_start in range(0, len(all_ids), 200):
                chunk = all_ids[chunk_start:chunk_start + 200]
                data = _call_linear_api(linear, _EXISTS_QUERY, {"ids": chunk})
                existing_ids.update(n["id"] for n in data["issues"]["nodes"])

            created = 0
            updated = 0
            errors: List[str] = []
            for r in rows_payload:
                input_fields: dict = {"title": r["title"]}
                if r["description"] is not None:
                    input_fields["description"] = r["description"]
                if r["priority"] is not None:
                    input_fields["priority"] = r["priority"]
                if r["label_names"]:
                    label_ids = _label_ids_for(r["label_names"])
                    if label_ids:
                        input_fields["labelIds"] = label_ids
                if r["state_name"]:
                    state_id = _state_id_for(r["state_name"])
                    if state_id:
                        input_fields["stateId"] = state_id
                if r["assignee_email"]:
                    user_id = _user_id_for(r["assignee_email"])
                    if user_id:
                        input_fields["assigneeId"] = user_id

                try:
                    if r["issue_id"] in existing_ids:
                        resp = _call_linear_api(
                            linear, _UPDATE_MUTATION,
                            {"id": r["issue_id"], "input": input_fields},
                        )
                        payload = resp.get("issueUpdate") or {}
                        if not payload.get("success"):
                            errors.append(f"{r['key_str']}: issueUpdate reported success=false")
                            continue
                        updated += 1
                    else:
                        input_fields["id"] = r["issue_id"]
                        input_fields["teamId"] = _self.team_id
                        resp = _call_linear_api(linear, _CREATE_MUTATION, {"input": input_fields})
                        payload = resp.get("issueCreate") or {}
                        if not payload.get("success"):
                            errors.append(f"{r['key_str']}: issueCreate reported success=false")
                            continue
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{r['key_str']}: {type(e).__name__}: {e}")

            context.log.info(
                f"Linear upsert complete: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing {_self.key_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "linear_team_id": dg.MetadataValue.text(_self.team_id),
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
            description=_self.description or (
                f"Upsert DataFrame rows into Linear team {_self.team_id}."
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
