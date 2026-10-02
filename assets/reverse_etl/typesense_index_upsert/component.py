"""DataFrame -> Typesense collection upsert (search-index activation).

Mirrors an upstream DataFrame into a Typesense collection via the native
Documents Import endpoint:

    POST {host}/collections/{collection_name}/documents/import?action=upsert
    Content-Type: text/plain
    {"id": "...", ...fields...}\\n
    {"id": "...", ...fields...}\\n
    ...

Unlike Algolia (`objectID` in the batch body) or OpenSearch (`_id` in a
separate bulk metadata line), Typesense requires the document ID to be a
**string field named `id` inside the document body itself** -- there is
no out-of-band ID slot. This component takes `id_field` (an upstream
column), stringifies its value, and sets it as `id` in the document;
`id_field`'s original column is excluded from the rest of the body to
avoid a duplicate/conflicting key.

Typesense's bulk import endpoint does **not support deletes** -- deleting
is a separate, per-document call: `DELETE /collections/{collection}/documents/{id}`.
So `operation: delete` loops one HTTP call per row instead of batching.

Pairs with:
  - ``typesense_resource`` -- host + API key auth (required)
"""
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field

_DOCS_PER_IMPORT_REQUEST = 1000


def _build_document(row: Dict[str, Any], id_field: str) -> Tuple[Any, Dict[str, Any]]:
    """Pure row -> (id value, document body WITH a string 'id' key) builder.

    Returns (None, {}) when the row has no usable id_field value -- callers
    should skip such rows. Typesense requires 'id' to live inside the
    document JSON, unlike Algolia/OpenSearch where it's out-of-band."""
    import math

    raw_id = row.get(id_field)
    if raw_id is None:
        return None, {}
    try:
        if isinstance(raw_id, float) and math.isnan(raw_id):
            return None, {}
    except Exception:  # noqa: BLE001
        pass

    doc: Dict[str, Any] = {}
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
        doc[col] = val
    doc["id"] = str(raw_id)
    return raw_id, doc


def _build_ndjson_body(docs: List[Dict[str, Any]]) -> str:
    """Pure one-JSON-object-per-line NDJSON body builder for the import endpoint."""
    import json
    return "\n".join(json.dumps(doc) for doc in docs) + "\n"


def _call_typesense_import_api(resource, collection_name: str, ndjson_body: str, action: str) -> List[dict]:
    """Isolates the one real external-API boundary (the Typesense
    Documents Import REST call) so it can be monkeypatched wholesale in
    tests without network access or a real cluster. Returns one parsed
    JSON result per input line."""
    import json

    import requests

    url = f"{resource.get_base_url()}/collections/{collection_name}/documents/import"
    headers = dict(resource.get_headers())
    headers["Content-Type"] = "text/plain"
    response = requests.post(
        url,
        params={"action": action},
        data=ndjson_body.encode("utf-8"),
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return [json.loads(line) for line in response.text.strip("\n").split("\n") if line]


def _call_typesense_delete_api(resource, collection_name: str, doc_id: str) -> dict:
    """Isolates the one real external-API boundary for per-document
    deletes -- Typesense's import endpoint has no bulk-delete mode, so
    deletes loop one DELETE call per document ID."""
    import requests

    url = f"{resource.get_base_url()}/collections/{collection_name}/documents/{doc_id}"
    response = requests.delete(url, headers=resource.get_headers(), timeout=30)
    response.raise_for_status()
    return response.json()


class TypesenseIndexUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert or delete rows from an upstream DataFrame into a Typesense collection.

    Example:
        ```yaml
        type: dagster_component_templates.TypesenseIndexUpsertComponent
        attributes:
          asset_name: typesense_product_catalog_sync
          upstream_asset_key: dbt_marts_product_catalog
          resource_key: typesense_resource
          collection_name: products
          id_field: product_id
          operation: upsert
        ```

    Each upstream row becomes one Typesense document: `id_field`'s value
    becomes the document's `id` (a required string field inside the
    document body, unlike Algolia/OpenSearch), and every other column
    becomes a document field.

    `operation: delete` loops one `DELETE .../documents/{id}` call per
    row -- Typesense's bulk import endpoint does not support deletes.
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
        default="typesense_resource",
        description="Resource key registered by TypesenseResourceComponent.",
    )

    collection_name: str = Field(description="Target Typesense collection name.")
    id_field: str = Field(
        description=(
            "Upstream column whose (stringified) value becomes the "
            "Typesense document's `id` field. Required -- Typesense needs "
            "`id` inside the document body itself. Excluded from the rest "
            "of the document to avoid a duplicate key."
        ),
    )
    operation: str = Field(
        default="upsert",
        description=(
            "'upsert' (default) -- create-or-replace via the Import endpoint's "
            "`action=upsert`. 'delete' -- removes each row via a per-document "
            "`DELETE .../documents/{id}` call (Typesense's import endpoint has "
            "no bulk-delete mode)."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="typesense", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'typesense')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("typesense")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "TypesenseIndexUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("upsert", "delete"):
            raise ValueError(
                f"TypesenseIndexUpsertComponent: operation must be "
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
            raise ValueError(f"TypesenseIndexUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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
                doc_id, doc = _build_document(row.to_dict(), _self.id_field)
                if doc_id is None:
                    skipped_no_id += 1
                    continue
                items.append((doc_id, doc))

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

            if _self.operation == "delete":
                # Typesense's import endpoint has NO bulk-delete mode --
                # loop one DELETE call per document ID.
                for doc_id, _doc in items:
                    try:
                        _call_typesense_delete_api(resource, _self.collection_name, str(doc_id))
                        rows_submitted += 1
                    except Exception as e:  # noqa: BLE001
                        rows_errored += 1
                        errors.append(f"id={doc_id}: {type(e).__name__}: {e}")
                    requests_made += 1
            else:
                for chunk_start in range(0, len(items), _DOCS_PER_IMPORT_REQUEST):
                    chunk = items[chunk_start:chunk_start + _DOCS_PER_IMPORT_REQUEST]
                    docs = [doc for _id, doc in chunk]
                    ndjson_body = _build_ndjson_body(docs)
                    try:
                        results = _call_typesense_import_api(
                            resource, _self.collection_name, ndjson_body, "upsert"
                        )
                    except Exception as e:  # noqa: BLE001
                        rows_errored += len(chunk)
                        errors.append(
                            f"chunk {chunk_start}-{chunk_start + len(chunk) - 1}: "
                            f"{type(e).__name__}: {e}"
                        )
                        requests_made += 1
                        continue

                    for i, line_result in enumerate(results):
                        if line_result.get("success"):
                            rows_submitted += 1
                        else:
                            rows_errored += 1
                            errors.append(
                                f"row {chunk_start + i}: {line_result.get('error') or 'unknown error'}"
                            )
                    requests_made += 1

            context.log.info(
                f"Typesense {_self.operation} into collection={_self.collection_name}: "
                f"rows_submitted={rows_submitted} rows_skipped_no_id={skipped_no_id} "
                f"requests={requests_made} errors={len(errors)}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "collection_name": dg.MetadataValue.text(_self.collection_name),
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
                f"{_self.operation.title()} DataFrame rows into Typesense collection "
                f"{_self.collection_name} (id_field={_self.id_field})."
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
