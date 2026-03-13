"""OpenAPI Asset Component.

Fetches an OpenAPI spec (URL or local file) once at prepare time, discovers all
GET endpoints, groups them by tag, and creates one Dagster asset per tag group.

At execution time each asset calls its endpoints and writes the responses to a
destination database table. Uses StateBackedComponent so the spec is only fetched
once — code-server reloads are instant even for large specs.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


def _fetch_spec(spec_url: Optional[str], spec_path: Optional[str]) -> dict:
    """Fetch an OpenAPI spec from a URL or local file. Returns the parsed dict."""
    if spec_url:
        import requests
        resp = requests.get(spec_url, timeout=30)
        resp.raise_for_status()
        content = resp.text
    elif spec_path:
        content = Path(spec_path).read_text()
    else:
        raise ValueError("Must set spec_url or spec_path")

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        import yaml
        return yaml.safe_load(content)


def _extract_endpoints(spec: dict, include_tags: Optional[list], exclude_tags: Optional[list]) -> dict[str, list[dict]]:
    """Extract GET endpoints grouped by first tag. Returns {tag: [endpoint_info]}."""
    grouped: dict[str, list[dict]] = {}
    paths = spec.get("paths", {})
    servers = spec.get("servers", [{}])
    base_path = servers[0].get("url", "") if servers else ""

    for path, path_item in paths.items():
        op = path_item.get("get")
        if not op:
            continue
        tags = op.get("tags", ["default"])
        tag = tags[0] if tags else "default"

        if include_tags and tag not in include_tags:
            continue
        if exclude_tags and tag in exclude_tags:
            continue

        # Find array/list response schema for result extraction
        responses = op.get("responses", {})
        ok_resp = responses.get("200", responses.get("201", {}))
        content = ok_resp.get("content", {})
        json_content = content.get("application/json", {})
        schema = json_content.get("schema", {})

        # Detect pagination parameters
        params = op.get("parameters", [])
        param_names = {p.get("name", "") for p in params}
        has_page_param = "page" in param_names or "page_number" in param_names
        has_offset_param = "offset" in param_names
        has_cursor_param = "cursor" in param_names or "next_cursor" in param_names

        grouped.setdefault(tag, []).append({
            "path": path,
            "full_path": f"{base_path}{path}" if base_path else path,
            "operation_id": op.get("operationId", path.replace("/", "_").strip("_")),
            "summary": op.get("summary", ""),
            "response_array_key": _find_array_key(schema),
            "has_page_param": has_page_param,
            "has_offset_param": has_offset_param,
            "has_cursor_param": has_cursor_param,
            "params": [p for p in params if p.get("in") == "query"],
        })

    return grouped


def _find_array_key(schema: dict) -> Optional[str]:
    """Find the key in a response object that holds the array of results."""
    if schema.get("type") == "array":
        return None  # top-level array
    props = schema.get("properties", {})
    for key, val in props.items():
        if val.get("type") == "array" or "$ref" in str(val.get("items", {})):
            return key
    return None


def _build_openapi_defs(
    grouped: dict[str, list[dict]],
    api_base_url_env_var: str,
    api_key_env_var: Optional[str],
    auth_header: str,
    auth_prefix: str,
    database_url_env_var: str,
    schema_name: Optional[str],
    if_exists: str,
    group_name: Optional[str],
    page_size: int,
    max_pages: int,
) -> dg.Definitions:
    from dagster import AssetExecutionContext

    assets = []
    for tag, endpoints in grouped.items():
        safe_tag = tag.lower().replace(" ", "_").replace("-", "_")
        spec = dg.AssetSpec(
            key=dg.AssetKey(safe_tag),
            description=f"OpenAPI tag: {tag} ({len(endpoints)} GET endpoint{'s' if len(endpoints) != 1 else ''})",
            group_name=group_name or "openapi",
            kinds={"api", "sql"},
            metadata={
                "openapi/tag": dg.MetadataValue.text(tag),
                "openapi/endpoints": dg.MetadataValue.json([e["path"] for e in endpoints]),
            },
        )

        _endpoints = endpoints
        _tag = tag
        _safe_tag = safe_tag

        @dg.asset(
            name=_safe_tag,
            description=spec.description,
            group_name=group_name or "openapi",
            kinds={"api", "sql"},
        )
        def _asset_fn(context: AssetExecutionContext, _ep=_endpoints, _t=_tag, _st=_safe_tag):
            import os
            import requests
            import pandas as pd
            from sqlalchemy import create_engine

            base_url = os.environ[api_base_url_env_var]
            db_url = os.environ[database_url_env_var]
            headers = {}
            if api_key_env_var:
                key = os.environ[api_key_env_var]
                headers[auth_header] = f"{auth_prefix} {key}" if auth_prefix else key

            all_records = []

            for ep in _ep:
                path = ep["full_path"] if ep["full_path"].startswith("http") else f"{base_url}{ep['path']}"
                context.log.info(f"Fetching {path}")
                records = []

                if ep["has_page_param"]:
                    for page in range(1, max_pages + 1):
                        resp = requests.get(path, headers=headers, params={"page": page, "per_page": page_size}, timeout=30)
                        resp.raise_for_status()
                        data = resp.json()
                        key = ep["response_array_key"]
                        items = data[key] if key and isinstance(data, dict) else data
                        if not items:
                            break
                        records.extend(items if isinstance(items, list) else [items])
                        if len(items) < page_size:
                            break
                elif ep["has_offset_param"]:
                    offset = 0
                    while offset < max_pages * page_size:
                        resp = requests.get(path, headers=headers, params={"offset": offset, "limit": page_size}, timeout=30)
                        resp.raise_for_status()
                        data = resp.json()
                        key = ep["response_array_key"]
                        items = data[key] if key and isinstance(data, dict) else data
                        if not items:
                            break
                        records.extend(items if isinstance(items, list) else [items])
                        if len(items) < page_size:
                            break
                        offset += page_size
                else:
                    resp = requests.get(path, headers=headers, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    key = ep["response_array_key"]
                    items = data[key] if key and isinstance(data, dict) else data
                    records.extend(items if isinstance(items, list) else [items])

                for r in records:
                    if isinstance(r, dict):
                        r["_endpoint"] = ep["path"]
                all_records.extend(records)

            if not all_records:
                context.log.info(f"No records for tag {_t}")
                return dg.MaterializeResult(metadata={"num_rows": 0, "tag": _t})

            df = pd.json_normalize(all_records)
            context.log.info(f"Fetched {len(df)} rows from {len(_ep)} endpoints")
            engine = create_engine(db_url)
            df.to_sql(_st, con=engine, schema=schema_name, if_exists=if_exists, index=False, method="multi", chunksize=1000)
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "endpoints": len(_ep),
                "table": f"{schema_name + '.' if schema_name else ''}{_st}",
            })

        assets.append(_asset_fn)

    return dg.Definitions(assets=assets)


if _HAS_STATE_BACKED:
    @dataclass
    class OpenAPIAssetComponent(StateBackedComponent, dg.Resolvable):
        """Create one Dagster asset per OpenAPI tag group, backed by a cached spec.

        Fetches the OpenAPI spec once (write_state_to_path), then builds assets from
        the cache on every reload (build_defs_from_state) — no network calls at startup.

        Each asset calls all GET endpoints in its tag group and writes results to a
        destination database table.

        Example:
            ```yaml
            type: dagster_component_templates.OpenAPIAssetComponent
            attributes:
              spec_url: "https://petstore3.swagger.io/api/v3/openapi.json"
              api_base_url_env_var: API_BASE_URL
              api_key_env_var: API_KEY
              database_url_env_var: DATABASE_URL
            ```
        """
        spec_url: Optional[str] = None
        spec_path: Optional[str] = None
        api_base_url_env_var: str = "API_BASE_URL"
        api_key_env_var: Optional[str] = None
        auth_header: str = "Authorization"
        auth_prefix: str = "Bearer"
        database_url_env_var: str = "DATABASE_URL"
        schema_name: Optional[str] = None
        if_exists: str = "replace"
        include_tags: Optional[list] = None
        exclude_tags: Optional[list] = None
        group_name: Optional[str] = "openapi"
        page_size: int = 100
        max_pages: int = 100
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            key_part = (self.spec_url or self.spec_path or "openapi").replace("/", "_").replace(":", "")
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"OpenAPIAssetComponent[{key_part}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Fetch OpenAPI spec and extract endpoint groups. Cached to disk."""
            spec = _fetch_spec(self.spec_url, self.spec_path)
            grouped = _extract_endpoints(spec, self.include_tags, self.exclude_tags)
            state = {
                "title": spec.get("info", {}).get("title", "API"),
                "version": spec.get("info", {}).get("version", ""),
                "grouped": grouped,
            }
            state_path.write_text(json.dumps(state))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build one asset per tag from cached spec — no network calls."""
            if state_path is None or not state_path.exists():
                return dg.Definitions()

            state = json.loads(state_path.read_text())
            grouped = state["grouped"]
            return _build_openapi_defs(
                grouped=grouped,
                api_base_url_env_var=self.api_base_url_env_var,
                api_key_env_var=self.api_key_env_var,
                auth_header=self.auth_header,
                auth_prefix=self.auth_prefix,
                database_url_env_var=self.database_url_env_var,
                schema_name=self.schema_name,
                if_exists=self.if_exists,
                group_name=self.group_name,
                page_size=self.page_size,
                max_pages=self.max_pages,
            )

else:
    class OpenAPIAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Fallback: StateBackedComponent not available. Fetches spec on every reload."""
        spec_url: Optional[str] = dg.Field(default=None)
        spec_path: Optional[str] = dg.Field(default=None)
        api_base_url_env_var: str = dg.Field(default="API_BASE_URL")
        api_key_env_var: Optional[str] = dg.Field(default=None)
        auth_header: str = dg.Field(default="Authorization")
        auth_prefix: str = dg.Field(default="Bearer")
        database_url_env_var: str = dg.Field(default="DATABASE_URL")
        schema_name: Optional[str] = dg.Field(default=None)
        if_exists: str = dg.Field(default="replace")
        include_tags: Optional[list] = dg.Field(default=None)
        exclude_tags: Optional[list] = dg.Field(default=None)
        group_name: Optional[str] = dg.Field(default="openapi")
        page_size: int = dg.Field(default=100)
        max_pages: int = dg.Field(default=100)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            spec = _fetch_spec(self.spec_url, self.spec_path)
            grouped = _extract_endpoints(spec, self.include_tags, self.exclude_tags)
            return _build_openapi_defs(
                grouped=grouped,
                api_base_url_env_var=self.api_base_url_env_var,
                api_key_env_var=self.api_key_env_var,
                auth_header=self.auth_header,
                auth_prefix=self.auth_prefix,
                database_url_env_var=self.database_url_env_var,
                schema_name=self.schema_name,
                if_exists=self.if_exists,
                group_name=self.group_name,
                page_size=self.page_size,
                max_pages=self.max_pages,
            )
