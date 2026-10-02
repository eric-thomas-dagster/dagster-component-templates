"""DataFrame -> Algolia index upsert (search-index activation).

Mirrors an upstream DataFrame into an Algolia index via the native batch
endpoint:

    POST https://{app_id}-dsn.algolia.net/1/indexes/{index_name}/batch
    {"requests": [{"action": "updateObject"|"deleteObject", "body": {...}}]}

Every record Algolia indexes needs an `objectID` -- the component takes
`id_field` (an upstream column) and uses its value as that `objectID`.
All other upstream columns become document fields (`id_field` itself is
excluded from the body since its value already lives in `objectID`).

`operation: upsert` (default) uses the `updateObject` action, which fully
replaces (or creates) the record addressed by `objectID` -- this is
Algolia's create-or-update semantics in one call, no search-then-write.
`operation: delete` uses `deleteObject`, which needs only the `objectID`
in the body.

Both operations go through the same `/batch` endpoint and are chunked at
1,000 operations per HTTP call (Algolia's own helper libraries use the
same default chunk size for indexing/delete-by-id operations).

Pairs with:
  - ``algolia_resource`` -- App ID + Admin API Key auth (required)
"""
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field

_OPERATIONS_PER_REQUEST = 1000


def _build_document(row: Dict[str, Any], id_field: str) -> Tuple[Any, Dict[str, Any]]:
    """Pure row -> (objectID value, document body without id_field) builder.

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

    body: Dict[str, Any] = {}
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
        body[col] = val
    return raw_id, body


def _call_algolia_api(resource, index_name: str, operations: List[dict]) -> dict:
    """Isolates the one real external-API boundary (the Algolia REST batch
    call) so it can be monkeypatched wholesale in tests without network
    access or the real `algoliasearch` package installed."""
    import requests

    url = f"{resource.get_base_url()}/1/indexes/{index_name}/batch"
    response = requests.post(
        url,
        json={"requests": operations},
        headers=resource.get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


class AlgoliaIndexUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert or delete rows from an upstream DataFrame into an Algolia index.

    Example:
        ```yaml
        type: dagster_component_templates.AlgoliaIndexUpsertComponent
        attributes:
          asset_name: algolia_product_catalog_sync
          upstream_asset_key: dbt_marts_product_catalog
          resource_key: algolia_resource
          index_name: products
          id_field: product_id
          operation: upsert
        ```

    Each upstream row becomes one Algolia record: `id_field`'s value
    becomes the record's `objectID`, and every other column becomes a
    document field.
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
        default="algolia_resource",
        description="Resource key registered by AlgoliaResourceComponent.",
    )

    index_name: str = Field(description="Target Algolia index name.")
    id_field: str = Field(
        description=(
            "Upstream column whose value becomes the Algolia record's "
            "`objectID`. Required -- every Algolia record needs one. "
            "Excluded from the document body (its value already lives in "
            "objectID)."
        ),
    )
    operation: str = Field(
        default="upsert",
        description=(
            "'upsert' (default) -- create-or-replace via the `updateObject` "
            "action. 'delete' -- remove the record via `deleteObject`."
        ),
    )
    batch_size: int = Field(
        default=50000,
        description="Max upstream rows per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="algolia", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'algolia')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("algolia")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "AlgoliaIndexUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.operation not in ("upsert", "delete"):
            raise ValueError(
                f"AlgoliaIndexUpsertComponent: operation must be "
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
            raise ValueError(f"AlgoliaIndexUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            operations: List[dict] = []
            skipped_no_id = 0
            for _, row in df.iterrows():
                obj_id, body = _build_document(row.to_dict(), _self.id_field)
                if obj_id is None:
                    skipped_no_id += 1
                    continue
                if _self.operation == "delete":
                    operations.append({"action": "deleteObject", "body": {"objectID": str(obj_id)}})
                else:
                    body["objectID"] = str(obj_id)
                    operations.append({"action": "updateObject", "body": body})

            if not operations:
                context.log.warning("No rows had a valid id_field value -- nothing to sync.")
                return dg.MaterializeResult(
                    metadata={
                        "rows_submitted": dg.MetadataValue.int(0),
                        "rows_skipped_no_id": dg.MetadataValue.int(skipped_no_id),
                    }
                )

            resource = getattr(context.resources, _self.resource_key)
            requests_made = 0
            errors: List[str] = []
            rows_errored = 0
            rows_submitted = 0
            for chunk_start in range(0, len(operations), _OPERATIONS_PER_REQUEST):
                chunk = operations[chunk_start:chunk_start + _OPERATIONS_PER_REQUEST]
                try:
                    _call_algolia_api(resource, _self.index_name, chunk)
                    rows_submitted += len(chunk)
                except Exception as e:  # noqa: BLE001
                    rows_errored += len(chunk)
                    errors.append(
                        f"chunk {chunk_start}-{chunk_start + len(chunk) - 1}: "
                        f"{type(e).__name__}: {e}"
                    )
                requests_made += 1

            context.log.info(
                f"Algolia {_self.operation} into index={_self.index_name}: "
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
                f"{_self.operation.title()} DataFrame rows into Algolia index "
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
