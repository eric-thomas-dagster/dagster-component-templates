"""SMTP Resource component."""
from typing import List, Optional, Tuple

import dagster as dg
from pydantic import Field


class SMTPResource(dg.ConfigurableResource):
    """Provides an authenticated smtplib connection factory."""

    host: str
    port: int = 587
    username: str
    password: str
    use_tls: bool = True

    def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        from_addr: str = "",
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        html_body: Optional[str] = None,
        reply_to: Optional[str] = None,
        attachments: Optional[List[Tuple[str, bytes, str]]] = None,
        timeout_seconds: int = 30,
    ) -> None:
        """Send one email. `to` accepts a single address or a comma-separated list.
        `attachments` is a list of (filename, content_bytes, mime_type) tuples, e.g.
        ("report.csv", b"a,b\\n1,2", "text/csv"). `bcc` recipients get the message (they're
        passed to send_message's own to_addrs) but are never added as a header, same as any
        real BCC."""
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr or self.username
        msg["To"] = to
        if cc:
            msg["Cc"] = ", ".join(cc)
        if reply_to:
            msg["Reply-To"] = reply_to
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
            server.login(self.username, self.password)
            server.send_message(msg, from_addr=from_addr or self.username, to_addrs=recipients)


class SMTPResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an SMTP resource for sending emails from other components."""

    resource_key: str = Field(default="smtp_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="SMTP server host, e.g. 'smtp.gmail.com'")
    port: int = Field(default=587, description="SMTP port (587 for STARTTLS, 465 for SSL, 25 for plain)")
    username: str = Field(description="SMTP username / sender email address")
    password_env_var: str = Field(description="Environment variable holding the SMTP password")
    use_tls: bool = Field(default=True, description="Enable STARTTLS encryption")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='SMTP Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SMTPResource(
            host=self.host,
            port=self.port,
            username=self.username,
            password=dg.EnvVar(self.password_env_var),
            use_tls=self.use_tls,
        )
        return dg.Definitions(resources={self.resource_key: resource})
