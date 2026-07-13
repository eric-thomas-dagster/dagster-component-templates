"""Airtable Workspace Component.

Auto-enumerates Airtable bases × tables via the Meta API and emits one
Dagster asset per table. Materializing an asset reads records via the
Records API and emits a DataFrame with the flattened fields.
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
class AirtableTableSelector(dg.Resolvable):
    """Selector for filtering Airtable tables. Fivetran-shape.

    Matches on `<base_name>.<table_name>` OR just `<table_name>` depending on
    whether patterns include a dot.
    """
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, qualified_name: str) -> bool:
        """qualified_name = 'BaseName.TableName' — selectors can match either."""
        import fnmatch
        table_only = qualified_name.split(".", 1)[-1] if "." in qualified_name else qualified_name
        candidates = [qualified_name, table_only]

        if self.exclude_by_name and any(n in self.exclude_by_name for n in candidates):
            return False
        if self.exclude_by_pattern:
            for p in self.exclude_by_pattern:
                if any(fnmatch.fnmatch(n, p) for n in candidates):
                    return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and any(n in self.by_name for n in candidates):
            return True
        if self.by_pattern:
            for p in self.by_pattern:
                if any(fnmatch.fnmatch(n, p) for n in candidates):
                    return True
        return False


def _enumerate_airtable(pat, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": f"Bearer {pat}", "Accept": "application/json"}

    out: dict = {"bases": []}
    try:
        r = session.get("https://api.airtable.com/v0/meta/bases", headers=headers, timeout=60)
        r.raise_for_status()
        body = r.json() or {}
        for b in body.get("bases", []):
            base_id = b.get("id")
            base_name = b.get("name") or base_id
            if not base_id:
                continue
            tables: list = []
            try:
                tr = session.get(
                    f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
                    headers=headers, timeout=30,
                )
                tr.raise_for_status()
                tbody = tr.json() or {}
                for t in tbody.get("tables", []):
                    tid = t.get("id")
                    tname = t.get("name") or tid
                    if tid and tname:
                        tables.append({"id": tid, "name": tname})
            except Exception:  # noqa: BLE001
                pass
            out["bases"].append({"id": base_id, "name": base_name, "tables": tables})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class AirtableWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        pat_env_var: str  # Airtable personal access token
        verify_ssl: bool = True

        table_selector: Optional[AirtableTableSelector] = None
        page_size: int = 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["airtable"])
        compute_kind: str = "airtable"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"AirtableWorkspace[{hashlib.sha256(self.pat_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            pat = os.environ.get(self.pat_env_var, "")
            snapshot = _enumerate_airtable(pat, self.verify_ssl)
            if self.table_selector is not None:
                for b in snapshot["bases"]:
                    b["tables"] = [
                        t for t in b["tables"]
                        if self.table_selector.matches(f"{b['name']}.{t['name']}")
                    ]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for b in state.get("bases", []):
                for t in b.get("tables", []):
                    assets.append(self._build_asset(b["id"], b["name"], t["id"], t["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, base_id: str, base_name: str, table_id: str, table_name: str):
            _self = self
            safe_base = "".join(c if c.isalnum() or c == "_" else "_" for c in base_name)[:40] or "base"
            safe_table = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)[:40] or "table"
            key = dg.AssetKey([*self.asset_key_prefix, safe_base, safe_table])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "airtable_base_id": dg.MetadataValue.text(base_id),
                    "airtable_base_name": dg.MetadataValue.text(base_name),
                    "airtable_table_id": dg.MetadataValue.text(table_id),
                    "airtable_table_name": dg.MetadataValue.text(table_name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                pat = os.environ.get(_self.pat_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": f"Bearer {pat}", "Accept": "application/json"}

                url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
                r = session.get(url, headers=headers, params={"pageSize": _self.page_size}, timeout=120)
                r.raise_for_status()
                body = r.json() or {}
                records = body.get("records", [])
                rows = []
                for rec in records:
                    row = {"id": rec.get("id"), "createdTime": rec.get("createdTime")}
                    row.update(rec.get("fields") or {})
                    rows.append(row)
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "airtable_table": f"{base_name}.{table_name}",
                })
                return df

            return _asset

else:
    class AirtableWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("AirtableWorkspaceComponent requires Dagster with StateBackedComponent support.")
