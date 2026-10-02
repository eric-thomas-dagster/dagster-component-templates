"""DataFrame -> OpenSearch index upsert (search-index activation).

Mirrors an upstream DataFrame into an OpenSearch index via the native
Bulk API:

    POST {host}/_bulk
    Content-Type: application/x-ndjson

    {"index": {"_index": "...", "_id": "..."}}\\n
    {...document...}\\n
    ...
    {"delete": {"_index": "...", "_id": "..."}}\\n   (no source line after)

Each upstream row becomes one document: `id_field`'s value becomes the
`_id` in the action/metadata line (NOT part of the document source --
OpenSearch tracks it separately), and every other upstream column becomes
a source field.

`operation: upsert` (default) uses the `index` action, which creates the
document if absent or fully replaces it if present -- OpenSearch's
create-or-replace semantics in one call. `operation: delete` uses the
`delete` action, which (per the Bulk API spec) takes NO source line after
its metadata line.

Auth is primarily HTTP basic (username/password) via
``OpenSearchResource`` -- the common case for self-managed clusters and
AWS OpenSearch fine-grained access control.

Pairs with:
  - ``opensearch_resource`` -- host + basic-auth (or API key) (required)
"""
import json
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field

_ROWS_PER_REQUEST = 1000


def _build_document(row: Dict[str, Any], id_field: str) -> Tuple[Any, Dict[str, Any]]:
    """Pure row -> (_id value, source document without id_field) builder.

    Returns (None, {}) when the row has no usable id_field value -- callers
    should skip such rows."""
    import math

    raw_id = row.get(id_field)
    if raw_id is None:
        return None, {}
    try:
        if isinstance(raw_id, float) and math.isnan(raw_id):
            return None, {}
    except Exception:  # noqa: BLE001
        pass

    source: Dict[str, Any] = {}
    for col, val in row.items():
        if col == id_field:
            continue
        if val is None:
            continue
        try:
            if isinstance(val, float) and math.isnan(val):
                continue
        except Exception:  # noqa: BLE001
            pass
        source[col] = val
    return raw_id, source


def _build_ndjson_body(index_name: str, items: List[Tuple[Any, Dict[str, Any]]], operation: str) -> str:
    """Pure (metadata-line [+ source-line]) NDJSON builder, one entry per
    (_id, source) pair. `delete` actions emit no source line, per the Bulk
    API spec."""
    lines: List[str] = []
    for doc_id, source in items:
        if operation == "delete":
            lines.append(json.dumps({"delete": {"_index": index_name, "_id": str(doc_id)}}))
        else:
            lines.append(json.dumps({"index": {"_index": index_name, "_id": str(doc_id)}}))
            lines.append(json.dumps(source))
    return "\n".join(lines) + "\n"


def _call_opensearch_api(resource, ndjson_body: str) -> dict:
    """Isolates the one real external-API boundary (the OpenSearch _bulk
    REST call) so it can be monkeypatched wholesale in tests without
    network access or a real cluster."""
    import requests

    url = f"{resource.get_base_url()}/_bulk"
    response = requests.post(
        url,
        data=ndjson_body.encode("utf-8"),
        headers=resource.get_headers(),
        auth=resource.get_auth(),
        verify=resource.verify_ssl,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


class OpenSearchIndexUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert or delete rows from an upstream DataFrame into an OpenSearch index.

    Example:
        ```yaml
        type: dagster_component_templates.OpenSearchIndexUpsertComponent
        attributes:
          asset_name: opensearch_product_catalog_sync
          upstream_asset_key: dbt_marts_product_catalog
          resource_key: opensearch_resource
          index_name: products
          id_field: product_id
          operation: upsert
        ```

    Each upstream row becomes one OpenSearch document: `id_field`'s value
    becomes the document's `_id`, and every other column becomes a source
    field.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

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
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="opensearch_resource",
        description="Resource key registered by OpenSearchResourceComponent.",
    )

    index_name: str = Field(description="Target OpenSearch index name.")
    id_field: str = Field(
        description=(
            "Upstream column whose value becomes the OpenSearch document's "
            "`_id`. Required. Excluded from the document source (its value "
            "already lives in the bulk action's metadata line)."
        ),
    )
    operation: str = Field(
        default="upsert",
        description=(
            "'upsert' (default) -- create-or-replace via the `index` bulk "
            "action. 'delete' -- remove the document via the `delete` bulk "
            "action (no source line)."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="opensearch", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'opensearch')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("opensearch")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "OpenSearchIndexUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("upsert", "delete"):
            raise ValueError(
                f"OpenSearchIndexUpsertComponent: operation must be "
                f"'upsert' or 'delete', got {self.operation!r}."
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
            raise ValueError(f"OpenSearchIndexUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to sync.")
                return dg.MaterializeResult(metadata={"rows_submitted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            if _self.id_field not in df.columns:
                raise dg.Failure(
                    f"id_field {_self.id_field!r} not in upstream columns: {list(df.columns)}"
                )

            items: List[Tuple[Any, Dict[str, Any]]] = []
            skipped_no_id = 0
            for _, row in df.iterrows():
                doc_id, source = _build_document(row.to_dict(), _self.id_field)
                if doc_id is None:
                    skipped_no_id += 1
                    continue
                items.append((doc_id, source))

            if not items:
                context.log.warning("No rows had a valid id_field value -- nothing to sync.")
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_id": dg.MetadataValue.int(skipped_no_id),
                    }
                )

            resource = getattr(context.resources, _self.resource_key)
            requests_made = 0
            rows_errored = 0
            rows_submitted = 0
            errors: List[str] = []
            for chunk_start in range(0, len(items), _ROWS_PER_REQUEST):
                chunk = items[chunk_start:chunk_start + _ROWS_PER_REQUEST]
                ndjson_body = _build_ndjson_body(_self.index_name, chunk, _self.operation)
                try:
                    result = _call_opensearch_api(resource, ndjson_body)
                except Exception as e:  # noqa: BLE001
                    rows_errored += len(chunk)
                    errors.append(
                        f"chunk {chunk_start}-{chunk_start + len(chunk) - 1}: "
                        f"{type(e).__name__}: {e}"
                    )
                    requests_made += 1
                    continue

                # Bulk response: {"errors": bool, "items": [{"index": {"status":...}} | {"delete": {...}}]}.
                response_items = result.get("items") or []
                if result.get("errors") and response_items:
                    for i, item in enumerate(response_items):
                        action_result = item.get("index") or item.get("delete") or item.get("create") or item.get("update") or {}
                        status = action_result.get("status")
                        if status is not None and status >= 300:
                            rows_errored += 1
                            errors.append(
                                f"row {chunk_start + i} (_id={action_result.get('_id')}): "
                                f"status={status} {action_result.get('error')}"
                            )
                        else:
                            rows_submitted += 1
                else:
                    rows_submitted += len(chunk)
                requests_made += 1

            context.log.info(
                f"OpenSearch {_self.operation} into index={_self.index_name}: "
                f"rows_submitted={rows_submitted} rows_skipped_no_id={skipped_no_id} "
                f"requests={requests_made} errors={len(errors)}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "index_name": dg.MetadataValue.text(_self.index_name),
                "operation": dg.MetadataValue.text(_self.operation),
                "rows_submitted": dg.MetadataValue.int(rows_submitted),
                "rows_skipped_no_id": dg.MetadataValue.int(skipped_no_id),
                "api_requests": dg.MetadataValue.int(requests_made),
                "rows_errored": dg.MetadataValue.int(rows_errored),
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
                f"{_self.operation.title()} DataFrame rows into OpenSearch index "
                f"{_self.index_name} (id_field={_self.id_field})."
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
