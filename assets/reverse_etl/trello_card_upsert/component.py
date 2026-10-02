"""DataFrame -> Trello card upsert.

Trello has no native "upsert" endpoint for cards. This component mirrors
an upstream DataFrame into Trello cards in a target List, matching rows
to existing cards via a stable **key marker** embedded in the card
description (`<!-- dagster-key: INC-1001 -->`) -- the same body-marker
pattern this repo already uses for `github_issue_upsert` / `clickup_task_upsert`
(Trello has no per-card custom field settable at creation time -- custom
fields can only be set via a SEPARATE call after the card already exists,
so a description marker is the zero-config option here, not a workaround).

Trello's write endpoints accept parameters as query string OR JSON body;
this component sends them as query-string params (Trello's own, more
backward-compatible convention), consistent with its GET/list calls.

Matches -> updated (name, desc, due, idLabels, closed).
Misses -> created.

Pairs with:
  - ``trello_resource`` -- API key + token connection (required)
"""
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_MARKER_RE = re.compile(r"<!-- dagster-key: ([^>]+?) -->")


def _make_desc(raw_desc: str, key: str) -> str:
    marker = f"<!-- dagster-key: {key} -->"
    body_no_marker = _MARKER_RE.sub("", raw_desc or "").lstrip("\n")
    return f"{marker}\n\n{body_no_marker}".rstrip()


def _extract_key(desc: Optional[str]) -> Optional[str]:
    if not desc:
        return None
    m = _MARKER_RE.search(desc)
    return m.group(1).strip() if m else None


def _call_trello_api(resource, method: str, path: str, params: Optional[dict] = None) -> Any:
    """Isolates the one external-API boundary (Trello's REST API) so it can
    be monkeypatched wholesale in tests -- mirrors this repo's "mock only
    the paid/external call" test convention."""
    return resource.request(method, path, params=params)


class TrelloCardUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Trello cards.

    Example:
        ```yaml
        type: dagster_component_templates.TrelloCardUpsertComponent
        attributes:
          asset_name: trello_incidents_mirror
          upstream_asset_key: incidents_seed
          resource_key: trello_resource
          list_id: "5f8a1b2c3d4e5f6a7b8c9d0e"
          key_column: incident_id
          name_column: name
          desc_column: description
          due_column: due_date
          id_labels_column: label_ids
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
        default="trello_resource",
        description="Resource key registered by TrelloResourceComponent.",
    )

    list_id: str = Field(description="Target Trello List ID cards are created/matched in.")
    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written into each "
            "card's description as `<!-- dagster-key: <value> -->` and used "
            "to match rows to existing cards on subsequent runs."
        ),
    )
    name_column: str = Field(description="Column holding the card name.")
    desc_column: Optional[str] = Field(default=None, description="Column holding the card description (markdown).")
    due_column: Optional[str] = Field(default=None, description="Column holding an ISO-8601 due date/datetime.")
    id_labels_column: Optional[str] = Field(
        default=None,
        description="Column holding Trello label IDs to apply. Accepts a list, or a comma-separated string.",
    )
    closed_column: Optional[str] = Field(
        default=None, description="Column holding a boolean/'true'/'false' to archive (close) the card."
    )

    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="trello", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'trello').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("trello")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "TrelloCardUpsertComponent: supply exactly one of "
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
            raise ValueError(f"TrelloCardUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            trello = getattr(context.resources, _self.resource_key)

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
            for c in (_self.desc_column, _self.due_column, _self.id_labels_column, _self.closed_column):
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

            # -- Index existing cards in the List by dagster-key marker ----
            # Trello's /1/lists/{id}/cards is not paginated -- it returns the
            # whole list in one call.
            existing_by_key: Dict[str, dict] = {}
            cards = _call_trello_api(
                trello, "GET", f"lists/{_self.list_id}/cards",
                params={"fields": "name,desc,due,closed,idLabels"},
            )
            for card in cards or []:
                key = _extract_key(card.get("desc"))
                if key:
                    existing_by_key[key] = card

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
                raw_desc = str(row[_self.desc_column]) if _self.desc_column and _row_value(row[_self.desc_column]) is not None else ""
                desc = _make_desc(raw_desc, key_str)

                params: dict = {"name": name, "desc": desc}
                if _self.due_column:
                    dv = _row_value(row[_self.due_column])
                    if dv is not None:
                        params["due"] = str(dv)
                if _self.id_labels_column:
                    label_ids = _coerce_list(row[_self.id_labels_column])
                    if label_ids:
                        params["idLabels"] = ",".join(label_ids)
                if _self.closed_column:
                    closed = _coerce_bool(_row_value(row[_self.closed_column]))
                    if closed is not None:
                        params["closed"] = str(closed).lower()

                existing = existing_by_key.get(key_str)
                try:
                    if existing:
                        _call_trello_api(trello, "PUT", f"cards/{existing['id']}", params=params)
                        updated += 1
                    else:
                        create_params = dict(params)
                        create_params["idList"] = _self.list_id
                        created_card = _call_trello_api(trello, "POST", "cards", params=create_params)
                        existing_by_key[key_str] = created_card
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{key_str}: {type(e).__name__}: {e}")

            context.log.info(
                f"Trello upsert complete: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing {_self.key_column})."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "trello_list_id": dg.MetadataValue.text(_self.list_id),
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
            description=_self.description or (f"Upsert DataFrame rows into Trello List {_self.list_id}."),
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
