"""Notion Workspace Component.

Auto-enumerates Notion databases via the Search API and emits one Dagster
asset per database. Materializing an asset queries the database and returns
its rows as a DataFrame.
"""
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

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
    StateBackedComponent = None  # type: ignore
    DefsStateConfig = None  # type: ignore
    DefsStateConfigArgs = None  # type: ignore
    ResolvedDefsStateConfig = Any  # type: ignore
    _HAS_STATE_BACKED = False


@dataclass
class NotionDatabaseSelector(dg.Resolvable):
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


def _enumerate_databases(access_token, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

    out: dict = {"databases": []}
    try:
        r = session.post(
            "https://api.notion.com/v1/search",
            headers=headers,
            json={"filter": {"value": "database", "property": "object"}},
            timeout=60,
        )
        r.raise_for_status()
        body = r.json() or {}
        for db in body.get("results", []):
            db_id = db.get("id")
            title = ""
            for t in db.get("title", []):
                title += t.get("plain_text", "")
            if db_id:
                out["databases"].append({"id": db_id, "title": title or db_id})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class NotionWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        access_token_env_var: str
        verify_ssl: bool = True

        database_selector: Optional[NotionDatabaseSelector] = None
        page_size: int = 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["notion"])
        compute_kind: str = "notion"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"NotionWorkspace[{hashlib.sha256(self.access_token_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            tok = os.environ.get(self.access_token_env_var, "")
            snapshot = _enumerate_databases(tok, self.verify_ssl)
            if self.database_selector is not None:
                snapshot["databases"] = [d for d in snapshot["databases"] if self.database_selector.matches(d["title"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for db in state.get("databases", []):
                assets.append(self._build_asset(db["id"], db["title"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, db_id: str, title: str):
            _self = self
            safe_title = "".join(c if c.isalnum() or c == "_" else "_" for c in title)[:40] or "db"
            key = dg.AssetKey([*self.asset_key_prefix, safe_title])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "notion_database_id": dg.MetadataValue.text(db_id),
                    "notion_database_title": dg.MetadataValue.text(title),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                tok = os.environ.get(_self.access_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {
                    "Authorization": f"Bearer {tok}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                }

                r = session.post(
                    f"https://api.notion.com/v1/databases/{db_id}/query",
                    headers=headers, json={"page_size": _self.page_size}, timeout=60,
                )
                r.raise_for_status()
                body = r.json() or {}
                pages = body.get("results", [])
                rows = []
                for pg in pages:
                    row: dict = {"id": pg.get("id"), "created_time": pg.get("created_time"), "last_edited_time": pg.get("last_edited_time")}
                    # Flatten properties. Each is {name: {type: <t>, <t>: value}}.
                    for prop_name, prop_val in (pg.get("properties") or {}).items():
                        ptype = prop_val.get("type")
                        val: Any = None
                        if ptype in ("title", "rich_text"):
                            val = "".join(t.get("plain_text", "") for t in (prop_val.get(ptype) or []))
                        elif ptype == "number":
                            val = prop_val.get("number")
                        elif ptype == "select":
                            val = (prop_val.get("select") or {}).get("name")
                        elif ptype == "multi_select":
                            val = ", ".join((s.get("name") or "") for s in (prop_val.get("multi_select") or []))
                        elif ptype == "date":
                            val = (prop_val.get("date") or {}).get("start")
                        elif ptype == "checkbox":
                            val = prop_val.get("checkbox")
                        elif ptype == "url":
                            val = prop_val.get("url")
                        elif ptype == "email":
                            val = prop_val.get("email")
                        elif ptype == "people":
                            val = ", ".join((u.get("name") or u.get("id") or "") for u in (prop_val.get("people") or []))
                        else:
                            val = prop_val.get(ptype)
                        row[prop_name] = val
                    rows.append(row)
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "notion_database_id": db_id,
                })
                return df

            return _asset

else:
    class NotionWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("NotionWorkspaceComponent requires Dagster with StateBackedComponent support.")
