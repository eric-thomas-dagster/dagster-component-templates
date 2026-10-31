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


class LinearIssueUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Linear issues.

    Example:
        ```yaml
        type: dagster_community_components.LinearIssueUpsertComponent
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
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'linear').")

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
            partitions_def=partitions_def,
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
