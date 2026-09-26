"""DataFrame → Stripe Customers upsert.

Mirrors an upstream DataFrame into Stripe customers. Stripe doesn't have
a native "upsert" endpoint (unlike HubSpot), and neither email nor any
custom field is globally unique on Stripe customers — you can technically
have multiple customers with the same email in one account.

Two lookup strategies, chosen per demo/pipeline needs:

1. **`match_by: email`** (default) — uses `customers.list?email=X`. This is
   immediately consistent (list index is real-time). Best when your data
   guarantees one customer per email (usually true for a curated CRM sync).

2. **`match_by: metadata`** — uses `customers/search` with
   `metadata['dagster_key']:'X'`. This is more precise but Stripe's Search
   API is eventually consistent — indexing lag can be 30s+ after create.
   Best for cases where multiple customers legitimately share an email.

Every managed customer also gets a `metadata.dagster_key = <key_column value>`
stamped on it either way — for observability + fallback lookup.

Creates use an `Idempotency-Key` header derived from the row key so
same-run retries are safe.

Pairs with:
  - ``stripe_resource`` — connection (required)
"""
import hashlib
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class StripeCustomerUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Stripe customers.

    Example:
        ```yaml
        type: dagster_community_components.StripeCustomerUpsertComponent
        attributes:
          asset_name: stripe_customers_mirror
          upstream_asset_key: customers_seed
          resource_key: stripe
          key_column: customer_id
          email_column: email
          name_column: full_name
          description_column: notes
          extra_metadata_columns: [source, plan_tier]
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    # Two source shapes — supply exactly one.
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
        default="stripe",
        description="Resource key registered by StripeResourceComponent.",
    )

    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written to each Stripe "
            "customer's `metadata.dagster_key` field. On subsequent runs we search "
            "customers by that key to find matches."
        ),
    )
    email_column: str = Field(description="Column holding the customer email.")
    name_column: Optional[str] = Field(default=None, description="Column holding the customer name.")
    description_column: Optional[str] = Field(
        default=None, description="Column holding the customer description."
    )
    extra_metadata_columns: List[str] = Field(
        default_factory=list,
        description=(
            "Additional columns whose values get written into the Stripe customer's "
            "`metadata` dict (using the column name as the metadata key)."
        ),
    )

    metadata_key_field: str = Field(
        default="dagster_key",
        description="Name of the metadata field used to store the dedup key.",
    )
    match_by: str = Field(
        default="email",
        description=(
            "How to match rows to existing customers: 'email' (uses "
            "customers.list?email=X — immediately consistent) or 'metadata' "
            "(uses customers/search on `metadata.<metadata_key_field>` — more "
            "precise but eventually consistent, up to 30s+ indexing lag). "
            "Default 'email' is safer for demos and small pipelines."
        ),
    )
    batch_size: int = Field(default=100, description="Max upstream rows per run (safety cap).")

    group_name: Optional[str] = Field(default="stripe", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'stripe').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("stripe")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "StripeCustomerUpsertComponent: supply exactly one of "
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
                        conn = resource.get_connection()
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
            raise ValueError(f"StripeCustomerUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            stripe_res = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required = {_self.key_column, _self.email_column}
            for c in (_self.name_column, _self.description_column, *_self.extra_metadata_columns):
                if c:
                    required.add(c)
            missing = [c for c in required if c not in df.columns]
            if missing:
                raise dg.Failure(
                    f"Columns not in upstream: {missing}. Available: {list(df.columns)}"
                )

            def _val(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return str(v)

            created = 0
            updated = 0
            existing_by_key: Dict[str, dict] = {}

            for _, row in df.iterrows():
                key_val = _val(row[_self.key_column])
                if not key_val:
                    context.log.warning(f"Row has null key ({_self.key_column}) — skipping.")
                    continue

                email = _val(row[_self.email_column])
                name = _val(row[_self.name_column]) if _self.name_column else None
                description = _val(row[_self.description_column]) if _self.description_column else None
                metadata: dict = {_self.metadata_key_field: key_val}
                for c in _self.extra_metadata_columns:
                    v = _val(row[c])
                    if v is not None:
                        metadata[c] = v

                # Match strategy: email (immediate) vs. metadata (eventually consistent).
                # `match_by: email` uses customers.list?email=X, immediately consistent.
                # `match_by: metadata` uses customers/search on metadata.<field>.
                existing = existing_by_key.get(key_val)
                if existing is None:
                    if _self.match_by == "metadata":
                        query = f"metadata['{_self.metadata_key_field}']:'{key_val}'"
                        hits = stripe_res.search_customers(query, page_size=1)
                    else:
                        hits = stripe_res.list_customers(email=email, page_size=1) if email else []
                    if hits:
                        existing = hits[0]
                        existing_by_key[key_val] = existing

                if existing:
                    updates: dict = {"metadata": metadata}
                    if email is not None:
                        updates["email"] = email
                    if name is not None:
                        updates["name"] = name
                    if description is not None:
                        updates["description"] = description
                    stripe_res.update_customer(existing["id"], **updates)
                    updated += 1
                else:
                    # Idempotency key = SHA256(key_val + Dagster run_id) — unique
                    # per run, so retries WITHIN a run are safe (Stripe returns
                    # the cached response) but retries ACROSS runs create fresh
                    # customers. Without the run_id, a deleted-then-recreated
                    # customer would return Stripe's cached "already created"
                    # response and skip the actual create silently.
                    salt = f"{key_val}:{context.run_id}"
                    idem = "dagster-" + hashlib.sha256(salt.encode()).hexdigest()[:32]
                    new = stripe_res.create_customer(
                        email=email,
                        name=name,
                        description=description,
                        metadata=metadata,
                        idempotency_key=idem,
                    )
                    existing_by_key[key_val] = new
                    created += 1

            context.log.info(
                f"Stripe upsert complete: {created} created, {updated} updated."
            )
            return dg.MaterializeResult(
                metadata={
                    "metadata_key_field": dg.MetadataValue.text(_self.metadata_key_field),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_upserted": dg.MetadataValue.int(created + updated),
                }
            )

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or "Upsert DataFrame rows into Stripe customers.",
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
