"""Salesforce Resource component.

Self-contained Salesforce REST API workhorse — raw HTTP for the API surface,
two auth modes (password + JWT Bearer), read/write convenience methods for
downstream sink / read components (`salesforce_record_upsert`, custom SOQL
readers, etc.).

Auth modes:

  `auth_mode: password` (default) — OAuth 2.0 password grant against a
  Connected App:

      POST /services/oauth2/token
          grant_type=password
          client_id=<consumer_key>
          client_secret=<consumer_secret>
          username=<username>
          password=<password><security_token>   # concatenated

  `auth_mode: jwt_bearer` — OAuth 2.0 JWT Bearer flow (headless, modern
  replacement for password grant; SF is deprecating password grant for new
  orgs). Requires a certificate uploaded to the Connected App:

      POST /services/oauth2/token
          grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
          assertion=<signed JWT>

  JWT payload:
      iss = consumer_key
      sub = salesforce_username
      aud = https://login.salesforce.com  (or test.salesforce.com)
      exp = now + 180s

  Signed RS256 with the private key matching the cert uploaded to the
  Connected App (Setup → App Manager → Digital Certificates).

Both return `access_token + instance_url`. Subsequent calls hit
`{instance_url}/services/data/v{api_version}/sobjects/...` with
`Authorization: Bearer <access_token>`.

Convenience methods:

    describe_sobject(sobject)                        # metadata
    soql(query, batch_size)                          # SOQL cursor (paginated)
    get_record(sobject, id)                          # single record by Id
    find_record(sobject, ext_id_field, ext_id_value) # single record by external Id
    create_record(sobject, body)                     # POST
    update_record(sobject, id, body)                 # PATCH by Id
    upsert_record(sobject, ext_id_field, ext_id_value, body)  # native upsert
    delete_record(sobject, id)                       # DELETE
    composite_upsert(sobject, ext_id_field, records) # batch upsert (up to 200)
    bulk_upsert(sobject, ext_id_field, records)      # Bulk 2.0 async job (10k+ per job)

Note: Salesforce's native upsert is one of the cleanest write APIs of any SaaS —
`PATCH /services/data/vXX.0/sobjects/{Object}/{ExtIdField}/{ExtIdValue}` atomically
creates or updates in a single call, using any custom field marked `External ID` as
the merge key. For >200-record loads, Bulk 2.0 (`bulk_upsert`) is 10-100x faster
than looped composite calls — Salesforce chunks + parallelizes server-side.
"""
import time
from typing import Any, Dict, Iterator, List, Optional

import dagster as dg
from pydantic import Field


class SalesforceResource(dg.ConfigurableResource):
    """Salesforce REST API workhorse: password OR JWT Bearer auth + read/write methods."""

    # Common
    username: str
    consumer_key: str
    domain: str = "login"                # 'login' (prod), 'test' (sandbox), or custom
    api_version: str = "58.0"            # bump when you rely on newer surface
    request_timeout_seconds: int = 60
    max_retries: int = 3

    # Auth mode selector
    auth_mode: str = "password"          # 'password' | 'jwt_bearer'

    # Password-grant fields (auth_mode='password')
    password: str = ""
    security_token: str = ""
    consumer_secret: str = ""

    # JWT Bearer fields (auth_mode='jwt_bearer')
    #
    # `private_key_pem` holds the RSA private key content (PEM-encoded) whose
    # matching cert has been uploaded to the Connected App under Digital
    # Certificates. `jwt_subject` defaults to `username` if unset.
    private_key_pem: str = ""
    jwt_subject: str = ""
    jwt_audience: str = ""               # empty → derive from `domain`

    # Internal state populated lazily (kept out of the pydantic surface).
    def _access(self) -> Dict[str, str]:
        """Return {'access_token', 'instance_url'} — cached per instance."""
        cache = getattr(self, "__auth_cache__", None)
        if cache is not None:
            return cache
        cache = self._login()
        object.__setattr__(self, "__auth_cache__", cache)
        return cache

    def _login(self) -> Dict[str, str]:
        import requests
        url = f"https://{self.domain}.salesforce.com/services/oauth2/token"
        mode = (self.auth_mode or "password").lower()
        if mode == "jwt_bearer":
            payload = {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": self._build_jwt_assertion(),
            }
        elif mode == "password":
            if not self.consumer_secret:
                raise RuntimeError(
                    "Salesforce auth_mode='password' requires consumer_secret."
                )
            payload = {
                "grant_type": "password",
                "client_id": self.consumer_key,
                "client_secret": self.consumer_secret,
                "username": self.username,
                "password": self.password + (self.security_token or ""),
            }
        else:
            raise RuntimeError(
                f"Salesforce auth_mode={self.auth_mode!r} not supported "
                f"(use 'password' or 'jwt_bearer')."
            )
        r = requests.post(
            url, data=payload, timeout=self.request_timeout_seconds
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Salesforce OAuth login failed ({r.status_code}) via "
                f"auth_mode={mode}: {r.text}"
            )
        payload_out = r.json()
        return {
            "access_token": payload_out["access_token"],
            "instance_url": payload_out["instance_url"],
        }

    def _build_jwt_assertion(self) -> str:
        """Sign a JWT assertion for the OAuth 2.0 JWT Bearer flow.

        `iss` = Connected App consumer key, `sub` = Salesforce username,
        `aud` = SF token endpoint host (login vs test), `exp` = now + 180s.
        Signed RS256 with `private_key_pem`.
        """
        if not self.private_key_pem:
            raise RuntimeError(
                "Salesforce auth_mode='jwt_bearer' requires private_key_pem."
            )
        try:
            import jwt as _pyjwt  # PyJWT
        except ImportError as e:
            raise RuntimeError(
                "Salesforce auth_mode='jwt_bearer' requires the `PyJWT[crypto]` "
                "package. Install with `pip install 'PyJWT[crypto]>=2.8'`."
            ) from e
        aud = self.jwt_audience or f"https://{self.domain}.salesforce.com"
        sub = self.jwt_subject or self.username
        now = int(time.time())
        claims = {
            "iss": self.consumer_key,
            "sub": sub,
            "aud": aud,
            "exp": now + 180,
        }
        # PyJWT >=2 returns str; <2 returned bytes. Coerce for safety.
        token = _pyjwt.encode(claims, self.private_key_pem, algorithm="RS256")
        if isinstance(token, bytes):
            token = token.decode("ascii")
        return token

    def _refresh(self) -> None:
        """Force a re-login on the next request (call after a 401)."""
        try:
            object.__delattr__(self, "__auth_cache__")
        except AttributeError:
            pass

    # ── HTTP wrappers ─────────────────────────────────────────────
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access()['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        base = self._access()["instance_url"]
        # `path` may or may not include leading slash — normalize
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/services/data/"):
            path = f"/services/data/v{self.api_version}{path}"
        return base + path

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute a request with retry on 401 (token refresh) / 429 / 5xx."""
        import requests
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method, self._url(path),
                    headers=self._headers(),
                    params=params or {},
                    json=json_body,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 401:
                # Expired access token — invalidate + retry once.
                self._refresh()
                if attempt >= self.max_retries:
                    r.raise_for_status()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 204:
                return None
            r.raise_for_status()
            if not r.content:
                return None
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return None

    def _get(self, path: str, **params) -> Any:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: Any) -> Any:
        return self._request("POST", path, json_body=body)

    def _patch(self, path: str, body: Any) -> Any:
        return self._request("PATCH", path, json_body=body)

    def _delete(self, path: str) -> Any:
        return self._request("DELETE", path)

    # ── Read methods ──────────────────────────────────────────────
    def describe_sobject(self, sobject: str) -> Dict[str, Any]:
        """Return the field metadata for an SObject (name / type / creatable / updateable / …)."""
        return self._get(f"/sobjects/{sobject}/describe") or {}

    def soql(
        self, query: str, *, batch_size: int = 2000, max_records: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """Iterate every record from a SOQL query, transparently following nextRecordsUrl."""
        params = {"q": query}
        # Salesforce doesn't have a client-side batch_size for /query — it
        # returns up to 2000 records per response and includes nextRecordsUrl
        # when there are more. batch_size is informational for downstream.
        _ = batch_size
        emitted = 0
        payload = self._get("/query", **params)
        while payload:
            for rec in payload.get("records", []) or []:
                yield rec
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return
            next_url = payload.get("nextRecordsUrl")
            if not next_url:
                return
            # nextRecordsUrl is absolute-path shaped — normalized by _url
            payload = self._get(next_url)

    def get_record(self, sobject: str, record_id: str) -> Optional[Dict[str, Any]]:
        """GET a single record by Id. Returns None on 404."""
        try:
            return self._get(f"/sobjects/{sobject}/{record_id}")
        except Exception as e:
            # requests wraps 404 as HTTPError — treat as absent
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    def find_record(
        self, sobject: str, ext_id_field: str, ext_id_value: Any
    ) -> Optional[Dict[str, Any]]:
        """GET a single record by external Id field. Returns None on 404."""
        try:
            return self._get(f"/sobjects/{sobject}/{ext_id_field}/{ext_id_value}")
        except Exception as e:
            import requests
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                if e.response.status_code == 404:
                    return None
            raise

    # ── Write methods ─────────────────────────────────────────────
    def create_record(self, sobject: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST — create a new record. Returns {'id', 'success', 'errors'}."""
        return self._post(f"/sobjects/{sobject}", body) or {}

    def update_record(
        self, sobject: str, record_id: str, body: Dict[str, Any]
    ) -> None:
        """PATCH by Id. 204 No Content on success — returns None."""
        self._patch(f"/sobjects/{sobject}/{record_id}", body)

    def upsert_record(
        self,
        sobject: str,
        ext_id_field: str,
        ext_id_value: Any,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """PATCH — Salesforce native External-ID upsert.

        Returns {'action': 'created'|'updated', 'id': <sf_id>}.

        Salesforce returns 201 Created when the record is new, 204 No Content
        when it was updated. The response body only exists on 201.
        """
        import requests
        path = f"/sobjects/{sobject}/{ext_id_field}/{ext_id_value}"
        # Do the request manually so we can inspect status_code (204 has no body).
        # Retry loop mirrors _request but returns response object for status inspection.
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.patch(
                    self._url(path),
                    headers=self._headers(),
                    json=body,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 401:
                self._refresh()
                if attempt >= self.max_retries:
                    r.raise_for_status()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            if r.status_code == 201:
                data = r.json() or {}
                return {"action": "created", "id": data.get("id")}
            # 204 (updated) — we don't get the Id back; look it up.
            existing = self.find_record(sobject, ext_id_field, ext_id_value)
            return {
                "action": "updated",
                "id": (existing or {}).get("Id") or (existing or {}).get("id"),
            }
        if last_exc:
            raise last_exc
        return {"action": "unknown", "id": None}

    def composite_upsert(
        self,
        sobject: str,
        ext_id_field: str,
        records: List[Dict[str, Any]],
        *,
        all_or_none: bool = False,
    ) -> List[Dict[str, Any]]:
        """Bulk upsert up to 200 records via /composite/sobjects.

        Records MUST include the `ext_id_field` key with a scalar value.
        Salesforce's Composite Sobjects Collections upsert applies External-ID
        matching for each record. Returns a list of {'id', 'success', 'errors'}
        matching the input order.
        """
        if len(records) > 200:
            raise ValueError("composite_upsert accepts at most 200 records per call")
        # Each record needs an `attributes.type` marker.
        body_records = []
        for rec in records:
            entry = {"attributes": {"type": sobject}}
            entry.update(rec)
            body_records.append(entry)
        payload = {"allOrNone": all_or_none, "records": body_records}
        result = self._patch(
            f"/composite/sobjects/{sobject}/{ext_id_field}", payload
        )
        # Salesforce returns a list of results.
        if isinstance(result, list):
            return result
        return []

    def delete_record(self, sobject: str, record_id: str) -> None:
        """DELETE by Id. 204 No Content on success — returns None."""
        self._delete(f"/sobjects/{sobject}/{record_id}")

    # ── Bulk 2.0 (large-volume async upsert) ──────────────────────
    def bulk_upsert(
        self,
        sobject: str,
        ext_id_field: str,
        records: List[Dict[str, Any]],
        *,
        poll_interval_seconds: float = 5.0,
        poll_timeout_seconds: int = 900,
        line_ending: str = "LF",
    ) -> Dict[str, Any]:
        """Bulk 2.0 asynchronous upsert.

        For >200-record loads this is 10-100x faster than looped composite
        calls — Salesforce chunks + parallelizes server-side. Flow:

          1. POST /services/data/vXX/jobs/ingest — create job (returns id + contentUrl)
          2. PUT {contentUrl} — upload records as CSV
          3. PATCH /jobs/ingest/{id}  {state: 'UploadComplete'} — kick off
          4. Poll GET /jobs/ingest/{id} until state ∈
             {JobComplete, Failed, Aborted}
          5. Return summary with counts.

        Returns:
            {
              'job_id': str,
              'state': 'JobComplete' | 'Failed' | 'Aborted',
              'records_processed': int,
              'records_failed': int,
              'apex_processing_time_ms': int,
              'total_processing_time_ms': int,
              'poll_seconds': float,
            }

        Notes:
          - Salesforce's per-job CSV limit is 150 MB / 100M chars.
            For larger loads, chunk `records` and call `bulk_upsert` per chunk.
          - `records` must all include the `ext_id_field` column.
          - Field values are stringified for CSV. Nulls (None / NaN) become
            the literal `#N/A` (Bulk API's null sentinel). Datetimes / dates
            should be pre-formatted as ISO 8601 strings.
        """
        import io
        import csv

        if not records:
            return {"job_id": "", "state": "JobComplete", "records_processed": 0,
                    "records_failed": 0, "apex_processing_time_ms": 0,
                    "total_processing_time_ms": 0, "poll_seconds": 0.0}

        # Union of all fields across records (Bulk 2.0 CSV must be uniform).
        columns: List[str] = []
        seen: set = set()
        for rec in records:
            for k in rec.keys():
                if k not in seen:
                    seen.add(k)
                    columns.append(k)
        if ext_id_field not in seen:
            raise ValueError(
                f"bulk_upsert: no record contained the ext_id_field={ext_id_field!r}. "
                f"columns seen: {columns}"
            )

        # Serialize to CSV with Salesforce's `#N/A` null sentinel.
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(columns)
        for rec in records:
            row = []
            for c in columns:
                v = rec.get(c)
                if v is None:
                    row.append("#N/A")
                elif isinstance(v, bool):
                    row.append("true" if v else "false")
                else:
                    row.append(str(v))
            writer.writerow(row)
        csv_body = buf.getvalue()

        # 1. Create job.
        job = self._post(
            f"/jobs/ingest",
            {
                "object": sobject,
                "externalIdFieldName": ext_id_field,
                "contentType": "CSV",
                "operation": "upsert",
                "lineEnding": line_ending,
            },
        ) or {}
        job_id = job.get("id")
        if not job_id:
            raise RuntimeError(f"bulk_upsert: create job returned no id: {job!r}")

        # 2. Upload CSV. This endpoint is NOT under /services/data/vXX — the
        # returned `contentUrl` is absolute-path shaped ("services/data/...").
        # We `PUT` raw CSV with Content-Type: text/csv (bypassing _request()
        # since it hardcodes application/json).
        import requests
        content_url = job.get("contentUrl") or f"jobs/ingest/{job_id}/batches"
        upload_url = self._access()["instance_url"] + (
            "/" + content_url if not content_url.startswith("/") else content_url
        )
        headers = {
            "Authorization": f"Bearer {self._access()['access_token']}",
            "Content-Type": "text/csv",
            "Accept": "application/json",
        }
        up = requests.put(
            upload_url, headers=headers, data=csv_body.encode("utf-8"),
            timeout=self.request_timeout_seconds,
        )
        if up.status_code not in (201, 200):
            raise RuntimeError(
                f"bulk_upsert: CSV upload failed ({up.status_code}): {up.text}"
            )

        # 3. Close upload — transitions job to UploadComplete.
        self._patch(f"/jobs/ingest/{job_id}", {"state": "UploadComplete"})

        # 4. Poll.
        start = time.time()
        while True:
            status = self._get(f"/jobs/ingest/{job_id}") or {}
            state = status.get("state") or ""
            if state in ("JobComplete", "Failed", "Aborted"):
                elapsed = time.time() - start
                return {
                    "job_id": job_id,
                    "state": state,
                    "records_processed": int(status.get("numberRecordsProcessed") or 0),
                    "records_failed": int(status.get("numberRecordsFailed") or 0),
                    "apex_processing_time_ms": int(status.get("apexProcessingTime") or 0),
                    "total_processing_time_ms": int(status.get("totalProcessingTime") or 0),
                    "poll_seconds": elapsed,
                }
            if (time.time() - start) > poll_timeout_seconds:
                raise RuntimeError(
                    f"bulk_upsert: job {job_id} exceeded poll_timeout "
                    f"({poll_timeout_seconds}s); last state={state!r}."
                )
            time.sleep(poll_interval_seconds)


class SalesforceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Salesforce REST API resource for use by other components.

    Two auth modes — pick one:

    - `auth_mode: password` (default) — OAuth 2.0 password grant against a
      Connected App. Simple, works everywhere, but SF is deprecating for new
      orgs. Requires: `password_env_var`, `security_token_env_var` (optional),
      `consumer_key_env_var`, `consumer_secret_env_var`.

    - `auth_mode: jwt_bearer` — OAuth 2.0 JWT Bearer flow. Modern headless
      auth using a certificate uploaded to the Connected App. Requires:
      `private_key_pem_env_var`, `consumer_key_env_var`. Optional:
      `jwt_subject` (defaults to `username`).

    Pairs with:
      - `salesforce_ingestion` — bulk pull from Salesforce (dlt-backed).
      - `salesforce_record_upsert` — reverse-ETL sink (native External-ID
        upsert; composite mode + Bulk 2.0 mode).

    Example (password auth, production org):

        ```yaml
        type: dagster_community_components.SalesforceResourceComponent
        attributes:
          resource_key: salesforce
          username: svc-account@mycompany.com
          auth_mode: password
          password_env_var: SF_PASSWORD
          security_token_env_var: SF_SECURITY_TOKEN
          consumer_key_env_var: SF_CONSUMER_KEY
          consumer_secret_env_var: SF_CONSUMER_SECRET
          domain: login
          api_version: "58.0"
        ```

    Example (JWT Bearer auth):

        ```yaml
        type: dagster_community_components.SalesforceResourceComponent
        attributes:
          resource_key: salesforce
          username: svc-account@mycompany.com
          auth_mode: jwt_bearer
          consumer_key_env_var: SF_CONSUMER_KEY
          private_key_pem_env_var: SF_PRIVATE_KEY_PEM   # full PEM contents
          domain: login
        ```
    """

    resource_key: str = Field(
        default="salesforce",
        description="Resource key. Other components reference it via this name.",
    )
    username: str = Field(
        description="Salesforce username (usually an email)."
    )

    auth_mode: str = Field(
        default="password",
        description=(
            "OAuth flow: 'password' (default; legacy but universal) or "
            "'jwt_bearer' (modern headless; SF-recommended for new orgs). "
            "Field requirements differ per mode — see class docstring."
        ),
    )

    # Password grant fields
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Salesforce password. Required when auth_mode='password'.",
    )
    security_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding the Salesforce security token (password grant only). "
            "Appended to the password at auth time. Required for IP-restricted "
            "orgs; leave unset for orgs with IP-relaxed access."
        ),
    )
    consumer_secret_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding the Connected App Consumer Secret. Required when "
            "auth_mode='password'. Not used for JWT Bearer."
        ),
    )

    # Shared
    consumer_key_env_var: str = Field(
        description=(
            "Env var holding the Connected App Consumer Key (from Setup → App Manager). "
            "Required for both auth modes."
        ),
    )

    # JWT Bearer fields
    private_key_pem_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding the RSA private key PEM content (full '-----BEGIN "
            "RSA PRIVATE KEY-----' block through '-----END ...-----'). Whose "
            "matching cert has been uploaded to the Connected App under Digital "
            "Certificates. Required when auth_mode='jwt_bearer'."
        ),
    )
    jwt_subject: Optional[str] = Field(
        default=None,
        description=(
            "JWT `sub` claim — the Salesforce user the assertion authorizes as. "
            "Defaults to `username`. Only used when auth_mode='jwt_bearer'."
        ),
    )
    jwt_audience: Optional[str] = Field(
        default=None,
        description=(
            "JWT `aud` claim — SF token endpoint host. Defaults to "
            "`https://{domain}.salesforce.com`. Only used when auth_mode='jwt_bearer'."
        ),
    )

    domain: str = Field(
        default="login",
        description=(
            "Salesforce login domain — `login` for production, `test` for "
            "sandbox, or `mycompany.my` for custom domains. Used to construct "
            "`https://<domain>.salesforce.com/services/oauth2/token`."
        ),
    )
    api_version: str = Field(
        default="58.0",
        description="Salesforce REST API version. Bump when you rely on newer surface.",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, capped at 10s) + one on 401 (token refresh).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        mode = (self.auth_mode or "password").lower()
        consumer_key = os.environ.get(self.consumer_key_env_var, "")

        if mode == "password":
            if not self.password_env_var:
                raise ValueError(
                    "SalesforceResourceComponent: auth_mode='password' requires "
                    "password_env_var."
                )
            if not self.consumer_secret_env_var:
                raise ValueError(
                    "SalesforceResourceComponent: auth_mode='password' requires "
                    "consumer_secret_env_var."
                )
            password = os.environ.get(self.password_env_var, "")
            security_token = (
                os.environ.get(self.security_token_env_var, "")
                if self.security_token_env_var
                else ""
            )
            consumer_secret = os.environ.get(self.consumer_secret_env_var, "")
            resource = SalesforceResource(
                username=self.username,
                consumer_key=consumer_key,
                auth_mode="password",
                password=password,
                security_token=security_token,
                consumer_secret=consumer_secret,
                domain=self.domain,
                api_version=self.api_version,
                request_timeout_seconds=self.request_timeout_seconds,
                max_retries=self.max_retries,
            )
        elif mode == "jwt_bearer":
            if not self.private_key_pem_env_var:
                raise ValueError(
                    "SalesforceResourceComponent: auth_mode='jwt_bearer' requires "
                    "private_key_pem_env_var."
                )
            private_key_pem = os.environ.get(self.private_key_pem_env_var, "")
            resource = SalesforceResource(
                username=self.username,
                consumer_key=consumer_key,
                auth_mode="jwt_bearer",
                private_key_pem=private_key_pem,
                jwt_subject=self.jwt_subject or "",
                jwt_audience=self.jwt_audience or "",
                domain=self.domain,
                api_version=self.api_version,
                request_timeout_seconds=self.request_timeout_seconds,
                max_retries=self.max_retries,
            )
        else:
            raise ValueError(
                f"SalesforceResourceComponent: auth_mode={self.auth_mode!r} not "
                f"supported (use 'password' or 'jwt_bearer')."
            )
        return dg.Definitions(resources={self.resource_key: resource})
