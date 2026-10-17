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

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
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
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'typesense')."
    )

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
        )
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
            partitions_def=partitions_def,
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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
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
