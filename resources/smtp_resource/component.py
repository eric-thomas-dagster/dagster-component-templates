"""SMTP Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from pydantic import Field


class SMTPResource(dg.ConfigurableResource):
    """Provides an authenticated smtplib connection factory."""

    host: str
    port: int = 587
    username: str
    password: str
    use_tls: bool = True

    def send_email(self, to: str, subject: str, body: str, from_addr: str = "") -> None:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = from_addr or self.username
        msg["To"] = to
        with smtplib.SMTP(self.host, self.port) as server:
            if self.use_tls:
                server.starttls()
            server.login(self.username, self.password)
            server.send_message(msg)


@dataclass
class SMTPResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an SMTP resource for sending emails from other components."""

    resource_key: str = Field(default="smtp_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="SMTP server host, e.g. 'smtp.gmail.com'")
    port: int = Field(default=587, description="SMTP port (587 for STARTTLS, 465 for SSL, 25 for plain)")
    username: str = Field(description="SMTP username / sender email address")
    password_env_var: str = Field(description="Environment variable holding the SMTP password")
    use_tls: bool = Field(default=True, description="Enable STARTTLS encryption")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SMTPResource(
            host=self.host,
            port=self.port,
            username=self.username,
            password=os.environ.get(self.password_env_var, ""),
            use_tls=self.use_tls,
        )
        return dg.Definitions(resources={self.resource_key: resource})
