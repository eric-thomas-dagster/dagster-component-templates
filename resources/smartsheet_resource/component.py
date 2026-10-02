"""Smartsheet Resource component.

Self-contained Smartsheet API workhorse using raw HTTP + a static bearer
token (personal access token or an OAuth2 app token — either way, just a
bearer string from this resource's point of view; there is no refresh flow
to implement here).

Auth:

    Authorization: Bearer <access_token>

against the single global base URL ``https://api.smartsheet.com/2.0``.

Smartsheet has **no native upsert / column-match endpoint**:

    POST /2.0/sheets/{sheetId}/rows   # adds new rows
    PUT  /2.0/sheets/{sheetId}/rows   # updates existing rows (row `id` required)

There is no way to ask the API to "update the row where column X == value" —
the numeric row `id` is mandatory for an update. So "upsert by column match"
has to happen client-side: fetch the sheet once (``GET /2.0/sheets/{id}``
returns both ``columns`` and the current ``rows``), build a
``title -> columnId`` map, scan the fetched rows for a cell in the key
column matching the incoming value, and route each incoming row to PUT
(found) or POST (not found) accordingly.

Both the add and update row endpoints cap out at **500 rows per API call** —
``add_rows`` / ``update_rows`` chunk automatically.

Convenience methods:

    get_sheet(sheet_id)                                    # raw GET, full payload (columns + rows)
    get_column_map(sheet_id)                               # title -> columnId
    find_row_by_column_value(sheet_id, key_column, value)  # client-side scan -> row dict or None
    add_rows(sheet_id, rows)                               # POST, chunked at 500
    update_rows(sheet_id, rows)                            # PUT, chunked at 500 (each row needs 'id')
    upsert_rows_by_column(sheet_id, key_column, rows)      # one get_sheet + batched add/update
"""
import time
from typing import Any, Dict, Iterable, List, Optional

import dagster as dg
from pydantic import Field

_ROWS_PER_REQUEST = 500


def _chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class SmartsheetResource(dg.ConfigurableResource):
    """Smartsheet API workhorse: bearer auth + read/write row methods."""

    access_token: str
    request_timeout_seconds: int = 60
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return "https://api.smartsheet.com/2.0"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ── HTTP wrapper ──────────────────────────────────────────────
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute a request with retry on 429 (honors Retry-After) / 5xx."""
        import requests

        url = path if path.startswith("http") else f"{self.base_url}{path}"
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    params=params or {},
                    json=json_body,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2**attempt, 10))
                continue
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", "2"))
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(max(retry_after, 1.0), 30.0))
                continue
            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2**attempt, 10))
                continue
            r.raise_for_status()
            if r.status_code == 204 or not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return {}

    def _get(self, path: str, **params) -> Dict[str, Any]:
        return self._request("GET", path, params=params) or {}

    def _post(self, path: str, body: Any) -> Dict[str, Any]:
        return self._request("POST", path, json_body=body) or {}

    def _put(self, path: str, body: Any) -> Dict[str, Any]:
        return self._request("PUT", path, json_body=body) or {}

    # ── Sheet / row methods ─────────────────────────────────────────
    def get_sheet(self, sheet_id: str) -> Dict[str, Any]:
        """GET /sheets/{sheetId} — full payload including `columns` + `rows`."""
        return self._get(f"/sheets/{sheet_id}")

    def get_column_map(self, sheet_id: str) -> Dict[str, int]:
        """`{column title: columnId}` for the sheet, built from `get_sheet`."""
        sheet = self.get_sheet(sheet_id)
        return {c["title"]: c["id"] for c in sheet.get("columns", []) or []}

    @staticmethod
    def _cell_value(cell: Dict[str, Any]) -> Any:
        if "value" in cell:
            return cell.get("value")
        return cell.get("displayValue")

    def find_row_by_column_value(
        self, sheet_id: str, key_column_title: str, key_value: Any
    ) -> Optional[Dict[str, Any]]:
        """Client-side scan: fetch the sheet and look for a row whose cell in
        `key_column_title` matches `key_value`. Returns the row dict, or None.

        Smartsheet has no server-side column-match endpoint — this is the
        one GET + scan every lookup has to do.
        """
        sheet = self.get_sheet(sheet_id)
        col_map = {c["title"]: c["id"] for c in sheet.get("columns", []) or []}
        col_id = col_map.get(key_column_title)
        if col_id is None:
            raise ValueError(
                f"Smartsheet sheet {sheet_id} has no column titled {key_column_title!r}. "
                f"Available columns: {sorted(col_map)}"
            )
        target = str(key_value)
        for row in sheet.get("rows", []) or []:
            for cell in row.get("cells", []) or []:
                if cell.get("columnId") == col_id and str(self._cell_value(cell)) == target:
                    return row
        return None

    def add_rows(self, sheet_id: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """POST /sheets/{sheetId}/rows — chunked at 500 rows per call.

        Each item in `rows` is a raw Smartsheet row body, e.g.
        `{"cells": [{"columnId": ..., "value": ...}], "toBottom": True}`.
        Returns the combined list of created row objects.
        """
        created: List[Dict[str, Any]] = []
        for chunk in _chunked(rows, _ROWS_PER_REQUEST):
            if not chunk:
                continue
            payload = self._post(f"/sheets/{sheet_id}/rows", chunk)
            created.extend(payload.get("result") or [])
        return created

    def update_rows(self, sheet_id: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """PUT /sheets/{sheetId}/rows — chunked at 500 rows per call.

        Each item in `rows` MUST carry the row's numeric `id` — Smartsheet
        has no match-by-column-value on write, only on this client-side scan.
        Returns the combined list of updated row objects.
        """
        updated: List[Dict[str, Any]] = []
        for chunk in _chunked(rows, _ROWS_PER_REQUEST):
            if not chunk:
                continue
            payload = self._put(f"/sheets/{sheet_id}/rows", chunk)
            updated.extend(payload.get("result") or [])
        return updated

    def upsert_rows_by_column(
        self,
        sheet_id: str,
        key_column_title: str,
        rows_as_dicts: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Client-side upsert: one `get_sheet` fetch, then batched add/update.

        `rows_as_dicts` is a list of `{column_title: value, ...}` dicts. The
        column map and the existing-row lookup are both built from a single
        sheet fetch (not one GET per row — that would defeat the purpose of
        batching and hammer the API with N requests for N rows).

        Returns `{"created": [...], "updated": [...]}` of the raw Smartsheet
        row objects returned by the add/update calls.
        """
        sheet = self.get_sheet(sheet_id)
        columns = sheet.get("columns", []) or []
        col_map = {c["title"]: c["id"] for c in columns}
        if key_column_title not in col_map:
            raise ValueError(
                f"Smartsheet sheet {sheet_id} has no column titled {key_column_title!r}. "
                f"Available columns: {sorted(col_map)}"
            )
        key_col_id = col_map[key_column_title]

        # Build the key-value -> row id lookup from a single pass over the
        # sheet's current rows (one fetch total, not one per incoming row).
        existing_row_id_by_key: Dict[str, int] = {}
        for row in sheet.get("rows", []) or []:
            for cell in row.get("cells", []) or []:
                if cell.get("columnId") == key_col_id:
                    v = self._cell_value(cell)
                    if v is not None:
                        existing_row_id_by_key[str(v)] = row["id"]
                    break

        rows_to_add: List[Dict[str, Any]] = []
        rows_to_update: List[Dict[str, Any]] = []
        for row_dict in rows_as_dicts:
            cells = [
                {"columnId": col_map[title], "value": value}
                for title, value in row_dict.items()
                if title in col_map
            ]
            key_value = row_dict.get(key_column_title)
            existing_row_id = (
                existing_row_id_by_key.get(str(key_value)) if key_value is not None else None
            )
            if existing_row_id is not None:
                rows_to_update.append({"id": existing_row_id, "cells": cells})
            else:
                rows_to_add.append({"cells": cells, "toBottom": True})

        created = self.add_rows(sheet_id, rows_to_add) if rows_to_add else []
        updated = self.update_rows(sheet_id, rows_to_update) if rows_to_update else []
        return {"created": created, "updated": updated}


class SmartsheetResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Smartsheet resource for use by other components.

    Uses a static bearer token (long-lived, no OAuth flow to implement):
      a personal access token (Account → Personal Settings → API Access),
      or an OAuth2 app's access token. Either way, store it in an env var.

    Pairs with:
      - `smartsheet_ingestion` — bulk pull of sheets/users/reports/rows (dlt-backed).
      - `smartsheet_row_upsert` — reverse-ETL sink (client-side column-match upsert).

    Example:

        ```yaml
        type: dagster_component_templates.SmartsheetResourceComponent
        attributes:
          resource_key: smartsheet
          access_token_env_var: SMARTSHEET_ACCESS_TOKEN
        ```
    """

    resource_key: str = Field(
        default="smartsheet",
        description="Resource key. Other components reference it via this name.",
    )
    access_token_env_var: str = Field(
        description=(
            "Env var holding the Smartsheet API access token (a personal "
            "access token, or an OAuth2 app's access token)."
        ),
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 (honors Retry-After) / 5xx.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        access_token = os.environ.get(self.access_token_env_var, "")
        resource = SmartsheetResource(
            access_token=access_token,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
