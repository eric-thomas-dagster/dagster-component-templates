"""DataFrame -> Lever candidate (opportunity) upsert (search-then-write).

Lever's Opportunities API supports a documented exact-match `email` filter
(`GET /opportunities?email=...`), so this is a true upsert (unlike
Greenhouse's update-only candidate PATCH elsewhere in this repo): each row
is searched by email; a match gets tagged/staged/archived in place, a
non-match gets a brand-new opportunity created.

Two source shapes:
  1. `upstream_asset_key:` -- chain from an upstream Dagster asset that produces
     a pandas DataFrame. Standard Dagster lineage pattern.
  2. `source:` block -- read the DataFrame inline at run time, no upstream asset
     required. Supports:
       - kind: sql -- query a database via a Dagster resource (`resource_key`
         with `.get_engine()` / `.get_connection()`) OR a raw
         `database_url_env_var`.
       - kind: csv -- read a CSV file at `path`.
       - kind: inline -- literal rows in YAML.

Pairs with:
  - ``lever_resource`` -- Basic auth (blank password) + perform_as (required)

Real Lever API facts this sink relies on:
  - Tags are ADDITIVE ONLY. Lever's `addTags` endpoint has no "replace all
    tags" counterpart, so a row's tags are merged onto whatever tags the
    opportunity already has, never replacing them.
  - `archive_reason_column` only ever applies to an EXISTING match. Lever
    has no "create pre-archived" shape worth relying on here, and archiving
    a brand-new opportunity in the same breath as creating it is not a
    pattern this sink attempts.
  - Every mutating call requires `perform_as` (the acting Lever user ID),
    which the `lever_resource` resource supplies on every write.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class LeverCandidateUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Lever opportunities (candidates).

    Example:
        ```yaml
        type: dagster_component_templates.LeverCandidateUpsertComponent
        attributes:
          asset_name: lever_candidates_mirror
          upstream_asset_key: dbt_marts_applicants
          resource_key: lever_resource
          email_column: email
          name_column: full_name
          headline_column: current_title
          tags_column: tags
          stage_id_column: stage_id
          archive_reason_column: archive_reason
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
        default="lever_resource",
        description="Resource key registered by LeverResourceComponent.",
    )

    email_column: str = Field(
        description="Upstream column holding the candidate's email -- the match key, searched via search_opportunities_by_email.",
    )
    name_column: Optional[str] = Field(
        default=None, description="Upstream column holding the candidate's full name. Used on CREATE only."
    )
    headline_column: Optional[str] = Field(
        default=None,
        description="Upstream column holding the candidate's headline (e.g. current title/company). Used on CREATE only.",
    )
    tags_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding tags (comma-separated string or list). Added via "
            "addTags on every row that has a match or gets created. ADDITIVE ONLY -- "
            "Lever has no 'replace all tags' call, so existing tags are never removed."
        ),
    )
    stage_id_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding a Lever stage ID. If the opportunity already exists, "
            "calls update_stage. If creating, included as 'stage' in the create body."
        ),
    )
    archive_reason_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding an archive reason. Only applies to an EXISTING "
            "matched opportunity -- never on create."
        ),
    )
    posting_id: Optional[str] = Field(
        default=None, description="Static Lever posting ID, passed to create_opportunity only."
    )

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="lever", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'lever').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("lever")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "LeverCandidateUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
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
            raise ValueError(f"LeverCandidateUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _row_value(v):
            import pandas as pd
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            return v

        def _parse_tags(v) -> List[str]:
            if v is None:
                return []
            if isinstance(v, (list, tuple, set)):
                return [str(t).strip() for t in v if str(t).strip()]
            return [t.strip() for t in str(v).split(",") if t.strip()]

        def _run_write(exec_ctx, upstream):
            svc = getattr(exec_ctx.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                exec_ctx.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata={
                    "rows_created": dg.MetadataValue.int(0),
                    "rows_updated": dg.MetadataValue.int(0),
                    "rows_errored": dg.MetadataValue.int(0),
                    "rows_skipped_no_key": dg.MetadataValue.int(0),
                })

            if len(df) > _self.max_rows:
                exec_ctx.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            required_cols = {_self.email_column}
            for optional_col in (
                _self.name_column,
                _self.headline_column,
                _self.tags_column,
                _self.stage_id_column,
                _self.archive_reason_column,
            ):
                if optional_col:
                    required_cols.add(optional_col)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            rows_created = 0
            rows_updated = 0
            rows_skipped_no_key = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                email = _row_value(row[_self.email_column])
                if email is None:
                    rows_skipped_no_key += 1
                    continue

                tags_value = _row_value(row[_self.tags_column]) if _self.tags_column else None
                stage_value = _row_value(row[_self.stage_id_column]) if _self.stage_id_column else None
                archive_reason_value = (
                    _row_value(row[_self.archive_reason_column]) if _self.archive_reason_column else None
                )

                try:
                    results = svc.search_opportunities_by_email(email)
                    if results:
                        opp_id = results[0]["id"]
                        did_action = False
                        if tags_value:
                            svc.add_tags(opp_id, _parse_tags(tags_value))
                            did_action = True
                        if stage_value:
                            svc.update_stage(opp_id, stage_value)
                            did_action = True
                        if archive_reason_value:
                            svc.archive_opportunity(opp_id, archive_reason_value)
                            did_action = True
                        if not did_action:
                            exec_ctx.log.debug(
                                f"Lever opportunity matched for {email} but no tags/stage/archive "
                                "action was configured for this row -- counted as updated (no-op)."
                            )
                        rows_updated += 1
                    else:
                        body: Dict[str, Any] = {"emails": [email]}
                        name_value = _row_value(row[_self.name_column]) if _self.name_column else None
                        if name_value:
                            body["name"] = name_value
                        headline_value = _row_value(row[_self.headline_column]) if _self.headline_column else None
                        if headline_value:
                            body["headline"] = headline_value
                        if stage_value:
                            body["stage"] = stage_value
                        if tags_value:
                            body["tags"] = _parse_tags(tags_value)
                        svc.create_opportunity(body, posting_id=_self.posting_id)
                        rows_created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (email={email}): {type(e).__name__}: {e}")

            exec_ctx.log.info(
                f"Lever candidate upsert: {rows_created} created, {rows_updated} updated, "
                f"{len(errors)} errors, {rows_skipped_no_key} skipped (missing {_self.email_column})."
            )
            if errors:
                exec_ctx.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "rows_created": dg.MetadataValue.int(rows_created),
                "rows_updated": dg.MetadataValue.int(rows_updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(rows_skipped_no_key),
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
                "Upsert DataFrame rows into Lever opportunities (match on email via "
                "search_opportunities_by_email)."
            ),
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
