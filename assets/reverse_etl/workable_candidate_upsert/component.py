"""DataFrame -> Workable candidate upsert (search-then-write).

Workable supports an exact-match candidate filter (`GET /candidates?email=`),
so this sink does a real search-then-write upsert: find the candidate by
email, PATCH the match (and optionally move its pipeline stage), or POST a
new candidate.

New candidates can be created two ways, controlled by `job_shortcode`:
  - `job_shortcode` set -> `POST /jobs/{shortcode}/candidates` (candidate is
    attached to that job's pipeline).
  - `job_shortcode` unset -> `POST /talent_pool/candidates` (candidate lands
    in the account-wide talent pool, not attached to any job -- useful when
    you don't want to force a job assignment).

Pairs with:
  - ``workable_resource`` -- Bearer token auth (required)

Live-validation notes:
  - `move_candidate` requires `member_id` (the acting Workable account
    member id) -- Workable's `/candidates/{id}/move` endpoint rejects
    requests without it. This component validates that `member_id` is set
    whenever `stage_column` is configured, at `build_defs` time.
  - `stage_column` only applies to EXISTING candidates (an update-time
    move); brand-new candidates are created via the job/talent-pool
    endpoints and are not immediately moved.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class WorkableCandidateUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Workable candidates.

    For every row in the source DataFrame:
      1. GET candidates filtered by email.
      2. If found: PATCH the matched candidate's fields_map fields, then
         (if `stage_column` is set and has a value for this row) POST a
         move to the configured target_stage.
      3. If not found: POST a new candidate (via the job pipeline if
         `job_shortcode` is set, else the account-wide talent pool), with
         `name` + `email` + any fields_map fields.

    Example -- upstream asset, create into a job pipeline:
        ```yaml
        type: dagster_component_templates.WorkableCandidateUpsertComponent
        attributes:
          asset_name: workable_candidates_mirror
          upstream_asset_key: dbt_marts_applicants
          resource_key: workable_resource
          email_column: email
          name_column: full_name
          job_shortcode: ABCD1234
          fields_map:
            phone: phone
            summary: summary
          group_name: reverse_etl
        ```

    Example -- inline source, talent pool (no job), with stage moves:
        ```yaml
        attributes:
          asset_name: workable_candidates_mirror
          source:
            kind: inline
            rows:
              - email: jane@example.com
                full_name: Jane Doe
                stage: sourced
          resource_key: workable_resource
          email_column: email
          name_column: full_name
          stage_column: stage
          member_id: "123456"
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
        default="workable_resource",
        description="Resource key registered by WorkableResourceComponent.",
    )

    email_column: str = Field(description="Upstream column holding the candidate's email (Workable match key).")
    name_column: str = Field(description="Upstream column holding the candidate's full name (used on CREATE, mapped to Workable's `name` field).")

    job_shortcode: Optional[str] = Field(
        default=None,
        description=(
            "Static config. If set, new candidates are created via "
            "`POST /jobs/{shortcode}/candidates` (attached to that job's pipeline). "
            "If unset, new candidates are created via `POST /talent_pool/candidates` "
            "(account-wide talent pool, no job needed)."
        ),
    )

    fields_map: Dict[str, str] = Field(
        default_factory=dict,
        description="Upstream column -> Workable candidate field (e.g. phone, summary, address, cover_letter). Applied on BOTH create and update.",
    )

    stage_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column holding a target pipeline stage slug. If present "
            "on an EXISTING match (found by email), triggers `move_candidate` "
            "after the field update. Requires `member_id` to be set."
        ),
    )
    member_id: Optional[str] = Field(
        default=None,
        description="Static config: the acting Workable account member id. Required only if `stage_column` is used.",
    )

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="workable", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'workable').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("workable")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "WorkableCandidateUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate: stage_column requires member_id.
        if self.stage_column and not self.member_id:
            raise ValueError(
                "WorkableCandidateUpsertComponent: `stage_column` is set but "
                "`member_id` is None. Workable's /candidates/{id}/move endpoint "
                "requires member_id (the acting account member) -- set it."
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
            raise ValueError(f"WorkableCandidateUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            required_cols = {_self.email_column, _self.name_column} | set(_self.fields_map.keys())
            if _self.stage_column:
                required_cols.add(_self.stage_column)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                if isinstance(v, str) and not v.strip():
                    return None
                return v

            rows_created = 0
            rows_updated = 0
            rows_skipped_no_key = 0
            errors: List[str] = []

            for _, row in df.iterrows():
                email = _row_value(row[_self.email_column])
                if email is None:
                    rows_skipped_no_key += 1
                    continue

                mapped_fields = {}
                for col, wk_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        mapped_fields[wk_field] = v

                try:
                    matches = svc.find_candidate_by_email(str(email))
                    if matches:
                        candidate_id = matches[0]["id"]
                        if mapped_fields:
                            svc.update_candidate(candidate_id, mapped_fields)
                        if _self.stage_column:
                            stage_value = _row_value(row[_self.stage_column])
                            if stage_value is not None:
                                svc.move_candidate(candidate_id, _self.member_id, str(stage_value))
                        rows_updated += 1
                    else:
                        name = _row_value(row[_self.name_column])
                        create_body = dict(mapped_fields)
                        create_body["email"] = str(email)
                        if name is not None:
                            create_body["name"] = name
                        if _self.job_shortcode:
                            svc.create_job_candidate(_self.job_shortcode, create_body)
                        else:
                            svc.create_talent_pool_candidate(create_body)
                        rows_created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{email}: {type(e).__name__}: {e}")

            exec_ctx.log.info(
                f"Workable candidate upsert: {rows_created} created, {rows_updated} updated, "
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
                "Upsert DataFrame rows into Workable candidates (match on email; "
                "create via job pipeline or talent pool)."
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
