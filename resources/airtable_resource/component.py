"""Airtable Resource component.

Personal-access-token wrapper over the Airtable REST API with ergonomic
read + write convenience methods so Dagster assets can call
`context.resources.airtable.list_records(base_id, "Table 1")` or
`.upsert_records(base_id, "Table 1", rows, key_fields=["Name"])`
without touching HTTP.

Airtable has a **native upsert endpoint** —
`PATCH /v0/{baseId}/{tableName}?performUpsert[fieldsToMergeOn][]=<field>`
does server-side create-or-update. The `upsert_records` method wraps it,
handling the required 10-records-per-batch limit transparently.

Drop to `.get_client()` for anything not covered — returns an
authenticated `requests.Session` pointed at `api.airtable.com/v0`.
"""
from typing import Iterator, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class AirtableResource(ConfigurableResource):
    """Dagster resource wrapping the Airtable REST API with convenience methods.

    Covers bases, tables, records (CRUD + native upsert), fields, comments,
    and attachments. For anything not covered, use `.get_client()` to get an
    authenticated `requests.Session`.

    Auth is a Personal Access Token — create one at
    https://airtable.com/create/tokens with the scopes you need
    (`data.records:read` / `data.records:write` / `schema.bases:read`).
    """

    api_key: str = Field(description="Airtable Personal Access Token (starts with `pat`).")
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    _BASE: str = "https://api.airtable.com/v0"

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        session = requests.Session()
        session.verify = self.verify_ssl
        session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        return session

    def _url(self, path: str) -> str:
        return f"{self._BASE}{path}"

    def _get(self, path: str, **params) -> dict:
        r = self.get_client().get(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict, **params) -> dict:
        r = self.get_client().post(self._url(path), json=body, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _patch(self, path: str, body: dict, **params) -> dict:
        r = self.get_client().patch(self._url(path), json=body, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str, **params) -> dict:
        r = self.get_client().delete(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------ reads

    def list_bases(self) -> List[dict]:
        """List all bases the PAT has access to."""
        return self._get("/meta/bases").get("bases", [])

    def get_base_schema(self, base_id: str) -> List[dict]:
        """Retrieve the schema (all tables + fields) for a base."""
        return self._get(f"/meta/bases/{base_id}/tables").get("tables", [])

    def get_record(self, base_id: str, table: str, record_id: str) -> dict:
        """Retrieve a single record by ID."""
        return self._get(f"/{base_id}/{table}/{record_id}")

    def list_records(
        self,
        base_id: str,
        table: str,
        filter_by_formula: Optional[str] = None,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
        max_records: Optional[int] = None,
        view: Optional[str] = None,
    ) -> List[dict]:
        """List records in a table. First page only — use `iter_records` for full pagination.

        `filter_by_formula` accepts Airtable formula syntax, e.g.:
          - `{Status}="Todo"`
          - `AND({Priority}="High", {Assignee}="ada@example.com")`
        """
        params: dict = {"pageSize": page_size}
        if filter_by_formula:
            params["filterByFormula"] = filter_by_formula
        if fields:
            for f in fields:
                params.setdefault("fields[]", []).append(f)
        if max_records:
            params["maxRecords"] = max_records
        if view:
            params["view"] = view
        return self._get(f"/{base_id}/{table}", **params).get("records", [])

    def iter_records(
        self,
        base_id: str,
        table: str,
        filter_by_formula: Optional[str] = None,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
        view: Optional[str] = None,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `list_records`. Uses Airtable's `offset` token."""
        session = self.get_client()
        offset: Optional[str] = None
        while True:
            params: dict = {"pageSize": page_size}
            if filter_by_formula:
                params["filterByFormula"] = filter_by_formula
            if fields:
                params["fields[]"] = fields
            if view:
                params["view"] = view
            if offset:
                params["offset"] = offset
            r = session.get(self._url(f"/{base_id}/{table}"), params=params, timeout=60)
            r.raise_for_status()
            body = r.json()
            for rec in body.get("records", []):
                yield rec
            offset = body.get("offset")
            if not offset:
                return

    # ----------------------------------------------------------------- writes

    def create_records(
        self, base_id: str, table: str, rows: List[dict], typecast: bool = False
    ) -> List[dict]:
        """Batch create records. Airtable caps at 10 records per request; we
        chunk automatically. `rows` is a list of `{"fields": {...}}` dicts —
        or just `{...}` dicts, we wrap them.

        `typecast: true` lets Airtable auto-coerce string values into typed
        fields (e.g. accept "2026-08-15" for a date field).
        """
        created: List[dict] = []
        for chunk_start in range(0, len(rows), 10):
            chunk = rows[chunk_start:chunk_start + 10]
            normalized = [r if "fields" in r else {"fields": r} for r in chunk]
            body: dict = {"records": normalized}
            if typecast:
                body["typecast"] = True
            resp = self._post(f"/{base_id}/{table}", body)
            created.extend(resp.get("records", []))
        return created

    def update_records(
        self, base_id: str, table: str, rows: List[dict], typecast: bool = False
    ) -> List[dict]:
        """Batch update records by ID. Each row is `{"id": "recXXX", "fields": {...}}`.
        Chunked at 10 records per request.
        """
        updated: List[dict] = []
        for chunk_start in range(0, len(rows), 10):
            chunk = rows[chunk_start:chunk_start + 10]
            body: dict = {"records": chunk}
            if typecast:
                body["typecast"] = True
            resp = self._patch(f"/{base_id}/{table}", body)
            updated.extend(resp.get("records", []))
        return updated

    def upsert_records(
        self,
        base_id: str,
        table: str,
        rows: List[dict],
        key_fields: List[str],
        typecast: bool = False,
    ) -> dict:
        """Native server-side upsert via `performUpsert[fieldsToMergeOn][]=...`.

        `rows` = list of `{"fields": {...}}` dicts (or plain `{...}` — we wrap).
        `key_fields` = column names Airtable will match on (all must uniquely
        identify a record; typically just `["Name"]` or `["email"]`).

        Returns a dict with `records`, `createdRecords`, and `updatedRecords`
        aggregated across all batches. Chunked at 10 records per request.
        """
        session = self.get_client()
        merged: dict = {"records": [], "createdRecords": [], "updatedRecords": []}
        for chunk_start in range(0, len(rows), 10):
            chunk = rows[chunk_start:chunk_start + 10]
            normalized = [r if "fields" in r else {"fields": r} for r in chunk]
            body: dict = {
                "records": normalized,
                "performUpsert": {"fieldsToMergeOn": key_fields},
            }
            if typecast:
                body["typecast"] = True
            r = session.patch(self._url(f"/{base_id}/{table}"), json=body, timeout=60)
            r.raise_for_status()
            resp = r.json()
            merged["records"].extend(resp.get("records", []))
            merged["createdRecords"].extend(resp.get("createdRecords", []))
            merged["updatedRecords"].extend(resp.get("updatedRecords", []))
        return merged

    def delete_records(self, base_id: str, table: str, record_ids: List[str]) -> List[dict]:
        """Batch delete by record ID. Chunked at 10 records per request."""
        deleted: List[dict] = []
        session = self.get_client()
        for chunk_start in range(0, len(record_ids), 10):
            chunk = record_ids[chunk_start:chunk_start + 10]
            # Airtable's batch delete uses repeated `records[]` query params.
            params = [("records[]", rid) for rid in chunk]
            r = session.delete(self._url(f"/{base_id}/{table}"), params=params, timeout=60)
            r.raise_for_status()
            deleted.extend(r.json().get("records", []))
        return deleted


class AirtableResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AirtableResource for use by other components.

    Example:
        ```yaml
        type: dagster_community_components.AirtableResourceComponent
        attributes:
          resource_key: airtable
          api_key_env_var: AIRTABLE_API_KEY
        ```
    """

    resource_key: str = Field(
        default="airtable",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="AIRTABLE_API_KEY",
        description="Env var holding an Airtable Personal Access Token.",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = AirtableResource(
            api_key=dg.EnvVar(self.api_key_env_var),
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
