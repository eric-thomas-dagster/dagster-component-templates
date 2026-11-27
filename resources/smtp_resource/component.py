"""SMTP Resource component."""
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, unquote

import dagster as dg
from pydantic import Field


def _parse_connection_uri(uri: str) -> Dict[str, Any]:
    """Parse an Airflow-style connection URI
    ('smtp://login:password@host:port', per
    https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
    into its parts. Login/password are percent-decoded."""
    parts = urlsplit(uri)
    return {
        "scheme": parts.scheme or None,
        "login": unquote(parts.username) if parts.username else None,
        "password": unquote(parts.password) if parts.password else None,
        "host": parts.hostname,
        "port": parts.port,
    }


class SMTPResource(dg.ConfigurableResource):
    """Provides an smtplib connection factory. `username`/`password` are optional --
    when both are unset, connects without calling `.login()` (a local relay/sandbox
    SMTP server that needs no auth). Setting only one of the two is a misconfiguration
    and raises at send time."""

    host: str
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = True

    def send_email(
        self,
        to: str,
        subject: str,
        body: str = "",
        from_addr: str = "",
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        html_body: Optional[str] = None,
        reply_to: Optional[str] = None,
        attachments: Optional[List[Tuple[str, bytes, str]]] = None,
        custom_headers: Optional[Dict[str, str]] = None,
        timeout_seconds: int = 30,
    ) -> None:
        """Send one email. `to` accepts a single address or a comma-separated list.
        `body` defaults to an empty string so an HTML-only email (`html_body` set, no plain-text
        part) can be sent without a caller having to pass a dummy body string themselves --
        `msg.set_content("")` still produces a valid empty plain-text alternative part.
        `attachments` is a list of (filename, content_bytes, mime_type) tuples, e.g.
        ("report.csv", b"a,b\\n1,2", "text/csv"). `bcc` recipients get the message (they're
        passed to send_message's own to_addrs) but are never added as a header, same as any
        real BCC. `custom_headers` sets arbitrary extra headers (e.g. Reply-To overrides,
        X-Priority, tracking headers) -- set after the standard headers so a custom header can
        override Reply-To/Cc if both are given."""
        import smtplib
        from email.message import EmailMessage

        if bool(self.username) != bool(self.password):
            raise ValueError(
                "smtp_resource: set both username and password to authenticate, or neither to "
                "connect without a login -- got only one of the two."
            )

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr or self.username or ""
        msg["To"] = to
        if cc:
            msg["Cc"] = ", ".join(cc)
        if reply_to:
            msg["Reply-To"] = reply_to
        for header_name, header_value in (custom_headers or {}).items():
            if header_name in msg:
                del msg[header_name]
            msg[header_name] = header_value
        msg.set_content(body)
        if html_body:
            msg.add_alternative(html_body, subtype="html")
        for filename, content, mime_type in attachments or []:
            maintype, _, subtype = (mime_type or "application/octet-stream").partition("/")
            msg.add_attachment(
                content, maintype=maintype or "application", subtype=subtype or "octet-stream", filename=filename,
            )

        recipients = [a.strip() for a in to.split(",") if a.strip()] + list(cc or []) + list(bcc or [])
        with smtplib.SMTP(self.host, self.port, timeout=timeout_seconds) as server:
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
            server.send_message(msg, from_addr=from_addr or self.username or "", to_addrs=recipients)


class SMTPResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an SMTP resource for sending emails from other components."""

    resource_key: str = Field(default="smtp_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: Optional[str] = Field(default=None, description="SMTP server host, e.g. 'smtp.gmail.com'. Set this OR connection_uri_env_var.")
    connection_uri_env_var: Optional[str] = Field(default=None, description="Env var holding an Airflow-style connection URI ('smtp://login:password@host:port') as an alternative to setting host/port/username/password_env_var individually. Explicit fields, if also set, win over the URI's corresponding value.")
    port: int = Field(default=587, description="SMTP port (587 for STARTTLS, 465 for SSL, 25 for plain)")
    username: Optional[str] = Field(default=None, description="SMTP username / sender email address. Omit along with password_env_var to connect without a login.")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the SMTP password. Omit along with username to connect without a login.")
    use_tls: bool = Field(default=True, description="Enable STARTTLS encryption")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='SMTP Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        host = self.host
        port = self.port
        username = self.username
        password: Optional[Any] = dg.EnvVar(self.password_env_var) if self.password_env_var else None
        if self.connection_uri_env_var:
            uri_value = dg.EnvVar(self.connection_uri_env_var).get_value()
            if uri_value:
                parsed = _parse_connection_uri(uri_value)
                if not host:
                    host = parsed["host"]
                if port == 587 and parsed["port"]:
                    port = parsed["port"]
                if username is None and parsed["login"] is not None:
                    username = parsed["login"]
                if password is None and parsed["password"] is not None:
                    password = parsed["password"]
        if not host:
            raise ValueError("Set either 'host' or 'connection_uri_env_var' (with a URI that includes a host).")

        resource = SMTPResource(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=self.use_tls,
        )
        return dg.Definitions(resources={self.resource_key: resource})
