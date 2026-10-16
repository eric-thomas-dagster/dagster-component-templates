"""DataFrame -> Google Sheets row upsert (read-all, match locally, write).

Google Sheets has no upsert concept at the API level: this sink reads the
entire worksheet, matches existing rows against `key_column` locally, then
updates changed rows in place and appends new ones. The sink OWNS the
worksheet's column layout -- header row is rewritten to match fields_map's
values in order if it doesn't already match, which will clear pre-existing
data laid out differently. Use a dedicated tab, not a human-edited sheet.

Pairs with:
  - ``google_sheets_resource`` -- service account auth + gspread client (required)"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

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


class GoogleSheetsRowUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into a Google Sheets worksheet.

    Example:
        ```yaml
        type: dagster_component_templates.GoogleSheetsRowUpsertComponent
        attributes:
          asset_name: sheets_customers_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: google_sheets_resource
          spreadsheet_id: "1AbCdEfGhIjKlMnOpQrStUvWxYz"
          worksheet_name: "Customers"
          key_column: email
          fields_map:
            email: Email
            name: Name
            health_score: "Health Score"
          group_name: reverse_etl
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
        default="google_sheets_resource",
        description="Resource key registered by GoogleSheetsResourceComponent.",
    )

    spreadsheet_id: str = Field(description="Google Sheets spreadsheet ID (from the sheet URL).")

    worksheet_name: str = Field(description="Worksheet (tab) name within the spreadsheet.")

    key_column: str = Field(description="Upstream column used to match existing rows for update-in-place. MUST be present in fields_map keys.")

    fields_map: Dict[str, str] = Field(description="Upstream column -> sheet column header, in the order they should appear in the sheet.")

    max_rows: int = Field(default=10000, description="Overall safety cap on rows per run.")

    group_name: Optional[str] = Field(default="google_sheets", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'googlesheets').")

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
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add('googlesheets')

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "GoogleSheetsRowUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.key_column not in self.fields_map:
            raise ValueError(
                "GoogleSheetsRowUpsertComponent: key_column must be present in fields_map keys."
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
            raise ValueError(f"GoogleSheetsRowUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_write(context, upstream):
            svc = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to write.")
                return dg.MaterializeResult(metadata={"rows_written": dg.MetadataValue.int(0)})

            if len(df) > _self.max_rows:
                context.log.warning(f"Upstream has {len(df)} rows; capped at max_rows={_self.max_rows}.")
                df = df.head(_self.max_rows)

            worksheet = svc.open_worksheet(_self.spreadsheet_id, _self.worksheet_name)
            header = list(_self.fields_map.values())

            existing_header = worksheet.row_values(1)
            if existing_header != header:
                worksheet.clear()
                worksheet.append_row(header)
                existing_header = header

            existing_records = worksheet.get_all_records() if worksheet.row_count > 1 else []

            key_header = _self.fields_map[_self.key_column]
            existing_row_by_key = {}
            for idx, rec in enumerate(existing_records, start=2):
                if rec.get(key_header) not in (None, ""):
                    existing_row_by_key[str(rec[key_header])] = idx

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return ""
                return v

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}")

            rows_to_append: List[list] = []
            updates: List[tuple] = []
            for _, row in df.iterrows():
                key_value = str(_row_value(row[_self.key_column]))
                values = [_row_value(row[col]) for col in _self.fields_map.keys()]
                existing_row_num = existing_row_by_key.get(key_value)
                if existing_row_num:
                    updates.append((existing_row_num, values))
                else:
                    rows_to_append.append(values)

            for row_num, values in updates:
                worksheet.update(f"A{row_num}", [values])
            if rows_to_append:
                worksheet.append_rows(rows_to_append)

            rows_created = len(rows_to_append)
            rows_updated = len(updates)
            context.log.info(
                f"Google Sheets upsert into {_self.spreadsheet_id}/{_self.worksheet_name}: "
                f"{rows_created} appended, {rows_updated} updated -- matched on {_self.key_column}."
            )
            metadata = {
                "spreadsheet_id": dg.MetadataValue.text(_self.spreadsheet_id),
                "worksheet_name": dg.MetadataValue.text(_self.worksheet_name),
                "rows_created": dg.MetadataValue.int(rows_created),
                "rows_updated": dg.MetadataValue.int(rows_updated),
                "rows_upserted": dg.MetadataValue.int(rows_created + rows_updated),
            }
            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (f"Upsert DataFrame rows into Google Sheets {_self.spreadsheet_id}/{_self.worksheet_name} (match on {_self.key_column})."),
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
