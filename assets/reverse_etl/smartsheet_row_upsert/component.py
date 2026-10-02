"""DataFrame -> Smartsheet row upsert (client-side column match, no native upsert).

Smartsheet's row API has no column-match/upsert endpoint:
`POST /2.0/sheets/{sheetId}/rows` only adds new rows, and
`PUT /2.0/sheets/{sheetId}/rows` only updates existing rows (and requires
each row's numeric `id` -- there's no "match on column value" on write).
So the matching happens client-side: the resource fetches the sheet once
(columns + current rows), builds a title -> columnId map, scans for a cell
in the key column matching each incoming row's key value, and routes each
row to an update (found) or an add (not found). Both write endpoints cap
out at 500 rows per API call -- the resource chunks automatically.

Pairs with:
  - ``smartsheet_resource`` -- bearer-token auth + sheet/row read-write methods (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SMARTSHEET_ROWS_PER_CALL = 500


class SmartsheetRowUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into a Smartsheet sheet.

    Example:
        ```yaml
        type: dagster_component_templates.SmartsheetRowUpsertComponent
        attributes:
          asset_name: smartsheet_tasks_mirror
          upstream_asset_key: tasks_seed
          resource_key: smartsheet
          sheet_id: "4583173393803140"
          key_column: "Task ID"
          fields_map:
            task_id: "Task ID"
            name: "Task Name"
            status: Status
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
        default="smartsheet",
        description="Resource key registered by SmartsheetResourceComponent.",
    )

    sheet_id: str = Field(description="Target Smartsheet sheet ID.")

    key_column: str = Field(
        description=(
            "Smartsheet column title used as the upsert match key. Smartsheet has no "
            "server-side column-match endpoint, so the resource fetches the sheet once "
            "and matches client-side on this column. Must be present in fields_map's "
            "values -- it's the Smartsheet-side title you're writing to, not an upstream "
            "DataFrame column name."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Smartsheet column title.",
    )
    batch_size: int = Field(
        default=500,
        description=(
            "Max upstream rows per run (safety cap). Hard-capped at 500 regardless of "
            "configured value -- Smartsheet's row-add/row-update endpoints each accept "
            "at most 500 rows per API call, and this sink does a single upsert call per run."
        ),
    )

    group_name: Optional[str] = Field(default="smartsheet", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'smartsheet').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("smartsheet")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "SmartsheetRowUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate: key_column must be one of the Smartsheet-side targets we're
        # actually writing to -- otherwise the upsert can never match.
        if self.key_column not in self.fields_map.values():
            raise ValueError(
                f"SmartsheetRowUpsertComponent: key_column {self.key_column!r} not present "
                f"in fields_map values. key_column must be a Smartsheet column you're "
                f"upserting. fields_map values: {sorted(set(self.fields_map.values()))}"
            )

        # Upstream column(s) that map onto the key column -- used to read the
        # match-key value out of each incoming row (and to skip blank keys).
        key_source_cols = [c for c, title in self.fields_map.items() if title == self.key_column]

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
            raise ValueError(f"SmartsheetRowUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            smartsheet = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            effective_batch_size = min(_self.batch_size, _SMARTSHEET_ROWS_PER_CALL)
            if _self.batch_size > _SMARTSHEET_ROWS_PER_CALL:
                context.log.warning(
                    f"batch_size={_self.batch_size} exceeds Smartsheet's "
                    f"{_SMARTSHEET_ROWS_PER_CALL}-rows-per-call limit; capping at "
                    f"{_SMARTSHEET_ROWS_PER_CALL}."
                )
            if len(df) > effective_batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={effective_batch_size}."
                )
                df = df.head(effective_batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            key_source_col = key_source_cols[0] if key_source_cols else None

            rows_as_dicts: List[dict] = []
            skipped_blank_key = 0
            for _, row in df.iterrows():
                key_value = _row_value(row[key_source_col]) if key_source_col else None
                if key_value is None or (isinstance(key_value, str) and key_value.strip() == ""):
                    skipped_blank_key += 1
                    continue
                row_dict: dict = {}
                for col, title in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        row_dict[title] = v
                if row_dict:
                    rows_as_dicts.append(row_dict)

            if skipped_blank_key:
                context.log.warning(
                    f"Skipped {skipped_blank_key} row(s) with a blank/null value in the "
                    f"key column ({_self.key_column!r}) -- can't match or safely create them."
                )

            if not rows_as_dicts:
                context.log.warning("No rows with a usable key value -- nothing to upsert.")
                return dg.MaterializeResult(
                    metadata={
                        "sheet_id": dg.MetadataValue.text(_self.sheet_id),
                        "rows_created": dg.MetadataValue.int(0),
                        "rows_updated": dg.MetadataValue.int(0),
                        "rows_upserted": dg.MetadataValue.int(0),
                        "rows_skipped_blank_key": dg.MetadataValue.int(skipped_blank_key),
                    }
                )

            try:
                result = smartsheet.upsert_rows_by_column(
                    _self.sheet_id, _self.key_column, rows_as_dicts
                )
            except Exception as e:
                raise dg.Failure(
                    f"Smartsheet upsert failed for sheet {_self.sheet_id!r} "
                    f"(key_column={_self.key_column!r}, {len(rows_as_dicts)} row(s)): {e}"
                ) from e

            created = len(result.get("created") or [])
            updated = len(result.get("updated") or [])
            context.log.info(
                f"Smartsheet upsert complete: {created} created, {updated} updated "
                f"(matched on {_self.key_column!r}), {skipped_blank_key} skipped (blank key)."
            )
            return dg.MaterializeResult(
                metadata={
                    "sheet_id": dg.MetadataValue.text(_self.sheet_id),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_upserted": dg.MetadataValue.int(created + updated),
                    "rows_skipped_blank_key": dg.MetadataValue.int(skipped_blank_key),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Smartsheet sheet {_self.sheet_id} "
                f"(match on {_self.key_column})."
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
