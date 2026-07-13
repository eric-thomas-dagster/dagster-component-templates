"""Google Sheets Workspace Component.

Auto-enumerates Google Sheets spreadsheets in a Drive folder and emits one
Dagster asset per (spreadsheet, sheet). Materializing reads the sheet's
values as a DataFrame.
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
class GoogleSheetsSpreadsheetSelector(dg.Resolvable):
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


def _get_access_token(credentials_json: str) -> str:
    import json as _json, time, requests
    try:
        import jwt as _jwt
    except ImportError as e:
        raise Exception("pyjwt required — pip install pyjwt cryptography") from e
    creds = _json.loads(credentials_json)
    now = int(time.time())
    claims = {
        "iss": creds["client_email"],
        "scope": "https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/spreadsheets.readonly",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }
    signed = _jwt.encode(claims, creds["private_key"], algorithm="RS256")
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": signed},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _enumerate_sheets(credentials_json, folder_id, verify_ssl) -> dict:
    import requests
    try:
        token = _get_access_token(credentials_json)
    except Exception:
        return {"spreadsheets": []}
    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": f"Bearer {token}"}

    out: dict = {"spreadsheets": []}
    q = f"mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false" if folder_id else "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    try:
        r = session.get(
            "https://www.googleapis.com/drive/v3/files",
            headers=headers,
            params={"q": q, "pageSize": 1000, "fields": "files(id,name)"},
            timeout=60,
        )
        r.raise_for_status()
        for f in (r.json() or {}).get("files", []):
            sheet_id = f.get("id")
            sheet_name = f.get("name", sheet_id)
            if not sheet_id:
                continue
            tabs: list = []
            try:
                sr = session.get(
                    f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}",
                    headers=headers, params={"fields": "sheets.properties.title"},
                    timeout=30,
                )
                sr.raise_for_status()
                for s in (sr.json() or {}).get("sheets", []):
                    title = (s.get("properties") or {}).get("title")
                    if title:
                        tabs.append({"title": title})
            except Exception:  # noqa: BLE001
                pass
            out["spreadsheets"].append({"id": sheet_id, "name": sheet_name, "tabs": tabs})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class GoogleSheetsWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        credentials_json_env_var: str
        folder_id: Optional[str] = None  # None = all shared spreadsheets
        verify_ssl: bool = True

        spreadsheet_selector: Optional[GoogleSheetsSpreadsheetSelector] = None
        header_row: int = 1  # 1-indexed; the row to treat as column headers

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["google_sheets"])
        compute_kind: str = "google_sheets"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"GoogleSheetsWorkspace[{hashlib.sha256(self.credentials_json_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            creds = os.environ.get(self.credentials_json_env_var, "")
            snapshot = _enumerate_sheets(creds, self.folder_id, self.verify_ssl)
            if self.spreadsheet_selector is not None:
                snapshot["spreadsheets"] = [s for s in snapshot["spreadsheets"] if self.spreadsheet_selector.matches(s["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for s in state.get("spreadsheets", []):
                for tab in s.get("tabs", []):
                    assets.append(self._build_asset(s["id"], s["name"], tab["title"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, sheet_id: str, sheet_name: str, tab_title: str):
            _self = self
            safe_ss = "".join(c if c.isalnum() or c == "_" else "_" for c in sheet_name)[:40] or sheet_id
            safe_tab = "".join(c if c.isalnum() or c == "_" else "_" for c in tab_title)[:40] or "tab"
            key = dg.AssetKey([*self.asset_key_prefix, safe_ss, safe_tab])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "sheet_id": dg.MetadataValue.text(sheet_id),
                    "sheet_name": dg.MetadataValue.text(sheet_name),
                    "tab_title": dg.MetadataValue.text(tab_title),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                creds = os.environ.get(_self.credentials_json_env_var, "")
                token = _get_access_token(creds)
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": f"Bearer {token}"}

                url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{tab_title}"
                r = session.get(url, headers=headers, timeout=60)
                r.raise_for_status()
                values = (r.json() or {}).get("values", [])
                if not values:
                    df = pd.DataFrame()
                elif _self.header_row and len(values) >= _self.header_row:
                    header = values[_self.header_row - 1]
                    data = values[_self.header_row:]
                    # Normalize row lengths to header length.
                    padded = [row + [""] * (len(header) - len(row)) for row in data]
                    df = pd.DataFrame(padded, columns=header)
                else:
                    df = pd.DataFrame(values)
                context.add_output_metadata({
                    "row_count": len(df),
                    "spreadsheet": sheet_name,
                    "tab": tab_title,
                })
                return df

            return _asset

else:
    class GoogleSheetsWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("GoogleSheetsWorkspaceComponent requires Dagster with StateBackedComponent support.")
