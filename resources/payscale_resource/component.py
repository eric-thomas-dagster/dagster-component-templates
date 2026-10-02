"""PayScale Jobalyzer Resource component.

Wraps PayScale's **Jobalyzer** API -- the compensation-benchmarking service
documented at https://developers.payscale.com/jobalyzer/. Jobalyzer is NOT a
bulk/paginated feed: it is a per-lookup REST service. A caller submits a set
of "compensable factors" (job title, location, years of experience, highest
degree, skills, certifications, ...) and PayScale returns one or more
compensation reports (Pay, Years-of-Experience, Skills/Certifications/
Education impact, ...) built from its salary-survey database.

VERIFIED (via developers.payscale.com, read directly -- not guessed):

Auth -- OAuth2 client_credentials grant:
    POST https://accounts.payscale.com/connect/token
    body (form-encoded): client_id, client_secret,
                          grant_type=client_credentials, scope=jobalyzer
    -> {"access_token": "...", "token_type": "Bearer", "expires_in": 600}
    Tokens are short-lived (~10 minutes per PayScale's docs).

Report request -- async submit-then-poll pattern:
    POST https://jobalyzer.payscale.com/jobalyzer/v1/reports
    headers: Authorization: Bearer <token>, Content-Type: application/json
    body:
        {
          "customerId": "<payscale-issued customer id>",
          "user": "<payscale-issued customer id>",
          "AutoResolveJobTitle": true,
          "requestedReports": ["pay"],
          "answers": {
            "JobTitle": "Software Developer",
            "City": "Seattle",
            "State": "Washington",
            "Country": "United States",
            "YearsExperience": 5,
            "HighestDegreeEarned": "Bachelor's Degree",
            "Skills": ["Python", "JavaScript"],
            "Certifications": ["Certified Public Accountant (CPA)"]
          }
        }
    (Only JobTitle and a Country are documented as strictly required;
    everything else refines the match. `customerId`/`user` are PayScale
    account identifiers, not secrets, but are required on every call.)

    Response is NOT the report itself -- it is a set of links to poll:
        {
          "Links": {
            "Self": ".../v2/reports/<id>",
            "PayReport": ".../v1/reports/<id>/pay",
            "YearsExperienceReport": ".../v2/reports/<id>/yoe"
          },
          "Warnings": null,
          "Errors": null
        }
    The caller polls the relevant link until it returns HTTP 200 (PayScale's
    docs describe polling "until status code 200 is received" -- this
    resource treats any non-200/429 response as "still processing" and
    retries up to `poll_timeout_seconds`).

Pay report shape (per PayScale's "Reports" docs): a `BasePayReport` /
`TotalPayReport` / `HourlyPayReport` / `Bonus` / `Commission` / `ProfitShare`
set of sub-reports, each with `Percentile10/25/50/75/90`, `Average`,
`Count`, `Percent`, `CurrencyName`, plus report-level `ReportRating`,
`TotalProfilesAnalyzed`, `LevelUsed`, and `Context.MatchedJobTitle` /
`Context.JobTitleRating`.

Billing: PayScale charges per report ("report credits"); a request naming
multiple `requestedReports` is billed per report produced. Reports with a
zero rating (insufficient data, or AutoResolveJobTitle finding no match)
are documented as NOT charged.

*** CREDENTIALS ARE NOT SELF-SERVE ***
PayScale does not offer a public signup form for Jobalyzer API credentials.
`client_id`/`client_secret`/`customer_id` are only issued as part of a
direct commercial agreement with PayScale (the same account team that
sells PayScale's compensation-data subscriptions). There is no sandbox or
trial key to request online -- see this component's README for the full
caveat before anyone goes looking for a "create an app" button on
developers.payscale.com. It will not be found.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

# PayScale's documented Jobalyzer endpoints (see module docstring for
# sources). Exposed as component defaults, not hardcoded constants, so a
# differently-provisioned PayScale account (e.g. a region-specific
# deployment) can override them without a code change.
DEFAULT_TOKEN_URL = "https://accounts.payscale.com/connect/token"
DEFAULT_API_BASE_URL = "https://jobalyzer.payscale.com/jobalyzer/v1"
DEFAULT_SCOPE = "jobalyzer"


def _payscale_http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
):
    """The ONE place this resource ever touches the network.

    Every PayScale call -- token acquisition, report submission, and report
    polling -- routes through this single module-level function so tests can
    monkeypatch exactly one thing (`payscale_resource.component._payscale_http_request`)
    instead of reaching into `requests` internals.
    """
    import requests

    return requests.request(
        method, url, headers=headers, data=data, json=json_body, timeout=timeout
    )


class PayscaleResource(dg.ConfigurableResource):
    """Dagster resource wrapping PayScale's Jobalyzer API (OAuth2
    client_credentials + async submit/poll report retrieval)."""

    customer_id: str = Field(
        description=(
            "PayScale-issued customer/client identifier, sent as both "
            "`customerId` and `user` in every Jobalyzer request body (per "
            "PayScale's own quickstart example). Issued alongside your "
            "client_id/client_secret when PayScale sets up your account -- "
            "there is no self-serve way to obtain one."
        )
    )
    client_id_env_var: str = Field(
        default="PAYSCALE_CLIENT_ID",
        description="Env var holding the OAuth2 client_credentials client ID.",
    )
    client_secret_env_var: str = Field(
        default="PAYSCALE_CLIENT_SECRET",
        description="Env var holding the OAuth2 client_credentials client secret.",
    )
    token_url: str = Field(
        default=DEFAULT_TOKEN_URL,
        description="OAuth2 token endpoint. PayScale's documented default; override only if your account uses a different issuer.",
    )
    api_base_url: str = Field(
        default=DEFAULT_API_BASE_URL,
        description="Jobalyzer REST API base URL (documented default is the v1 /reports endpoint).",
    )
    scope: str = Field(
        default=DEFAULT_SCOPE,
        description="OAuth2 scope requested at the token endpoint.",
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-HTTP-call timeout in seconds.",
    )
    poll_interval_seconds: float = Field(
        default=2.0,
        description="Seconds to wait between polling attempts while a report is still processing.",
    )
    poll_timeout_seconds: float = Field(
        default=60.0,
        description="Give up waiting for a report after this many seconds and raise TimeoutError.",
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        # Keep the dict on `self._token_cache` (the instance-level pydantic
        # PrivateAttr value), NOT `PayscaleResource._token_cache` (the class
        # attribute descriptor) -- the same class-vs-instance footgun found
        # and fixed in this repo's marketo_resource/auth0_resource.
        import os

        cache_key = f"{self.token_url}:{self.client_id_env_var}"
        cached = self._token_cache.get(cache_key) or {}
        if cached.get("expires", 0) > time.time() + 30:
            return cached["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not client_id or not client_secret:
            raise RuntimeError(
                f"Missing PayScale OAuth2 credentials: env vars "
                f"{self.client_id_env_var!r} and {self.client_secret_env_var!r} "
                f"must both be set. These are only issued via a direct "
                f"commercial agreement with PayScale -- there is no self-serve "
                f"signup (see README.md)."
            )

        resp = _payscale_http_request(
            "POST",
            self.token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
                "scope": self.scope,
            },
            timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        token_data = resp.json()
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 600)
        self._token_cache[cache_key] = {
            "access_token": access_token,
            # Refresh a little early rather than racing expiry mid-call.
            "expires": time.time() + expires_in - 30,
        }
        return access_token

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def submit_report_request(
        self,
        answers: Dict[str, Any],
        requested_reports: Optional[List[str]] = None,
        auto_resolve_job_title: bool = True,
    ) -> Dict[str, Any]:
        """POST /reports. Returns the `{"Links": ..., "Warnings": ...,
        "Errors": ...}` envelope -- NOT the report itself. Use
        `poll_until_ready` (or `get_pay_report` for the common case) to
        retrieve the actual report body from one of the returned Links."""
        body = {
            "customerId": self.customer_id,
            "user": self.customer_id,
            "AutoResolveJobTitle": auto_resolve_job_title,
            "requestedReports": requested_reports or ["pay"],
            "answers": answers,
        }
        resp = _payscale_http_request(
            "POST",
            f"{self.api_base_url}/reports",
            headers=self._auth_headers(),
            json_body=body,
            timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()

    def poll_until_ready(self, report_url: str) -> Dict[str, Any]:
        """GET `report_url` repeatedly until PayScale returns HTTP 200
        (the documented "ready" signal) or `poll_timeout_seconds` elapses."""
        deadline = time.time() + self.poll_timeout_seconds
        last_status: Optional[int] = None
        while time.time() < deadline:
            resp = _payscale_http_request(
                "GET",
                report_url,
                headers=self._auth_headers(),
                timeout=self.request_timeout_seconds,
            )
            last_status = resp.status_code
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in (202, 204):
                # Any other status (4xx/5xx) is a real error, not "still
                # processing" -- surface it immediately instead of burning
                # the poll budget.
                resp.raise_for_status()
            time.sleep(self.poll_interval_seconds)
        raise TimeoutError(
            f"PayScale report at {report_url!r} did not complete within "
            f"{self.poll_timeout_seconds}s (last HTTP status: {last_status})."
        )

    def get_pay_report(
        self,
        answers: Dict[str, Any],
        requested_reports: Optional[List[str]] = None,
        auto_resolve_job_title: bool = True,
    ) -> Dict[str, Any]:
        """Convenience: submit a report request and poll the `PayReport`
        link to completion. This is the single call most consumers need."""
        submitted = self.submit_report_request(
            answers, requested_reports=requested_reports or ["pay"],
            auto_resolve_job_title=auto_resolve_job_title,
        )
        errors = submitted.get("Errors")
        if errors:
            raise RuntimeError(f"PayScale rejected the report request: {errors}")
        links = submitted.get("Links") or {}
        pay_report_url = links.get("PayReport")
        if not pay_report_url:
            raise RuntimeError(
                f"PayScale response had no 'PayReport' link to poll. "
                f"Full response: {submitted!r}"
            )
        return self.poll_until_ready(pay_report_url)


class PayscaleResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `PayscaleResource` for use by other components (e.g.
    `payscale_compensation_enrichment`).

    *** PayScale does not offer self-serve API signup. *** `customer_id`,
    `client_id`, and `client_secret` are only issued once you have a direct
    commercial agreement with PayScale -- see README.md before trying to
    find a developer-portal "create app" flow; it does not exist.

    Example:
        ```yaml
        type: dagster_component_templates.PayscaleResourceComponent
        attributes:
          resource_key: payscale_resource
          customer_id: "123456"
          client_id_env_var: PAYSCALE_CLIENT_ID
          client_secret_env_var: PAYSCALE_CLIENT_SECRET
        ```
    """

    resource_key: str = Field(
        default="payscale_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    customer_id: str = Field(
        description=(
            "PayScale-issued customer/client identifier, sent as both "
            "`customerId` and `user` on every request. Issued by PayScale "
            "alongside your OAuth2 credentials -- not self-serve."
        )
    )
    client_id_env_var: str = Field(
        default="PAYSCALE_CLIENT_ID",
        description="Env var holding the OAuth2 client_credentials client ID.",
    )
    client_secret_env_var: str = Field(
        default="PAYSCALE_CLIENT_SECRET",
        description="Env var holding the OAuth2 client_credentials client secret.",
    )
    token_url: str = Field(
        default=DEFAULT_TOKEN_URL,
        description="OAuth2 token endpoint (documented PayScale default).",
    )
    api_base_url: str = Field(
        default=DEFAULT_API_BASE_URL,
        description="Jobalyzer REST API base URL (documented PayScale default).",
    )
    scope: str = Field(default=DEFAULT_SCOPE, description="OAuth2 scope.")
    request_timeout_seconds: int = Field(default=30, description="Per-HTTP-call timeout in seconds.")
    poll_interval_seconds: float = Field(
        default=2.0, description="Seconds between polling attempts for an in-progress report."
    )
    poll_timeout_seconds: float = Field(
        default=60.0, description="Give up waiting for a report after this many seconds."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PayscaleResource(
            customer_id=self.customer_id,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            token_url=self.token_url,
            api_base_url=self.api_base_url,
            scope=self.scope,
            request_timeout_seconds=self.request_timeout_seconds,
            poll_interval_seconds=self.poll_interval_seconds,
            poll_timeout_seconds=self.poll_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
