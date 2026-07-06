"""Supabase Resource component."""
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class SupabaseResource(ConfigurableResource):
    """Dagster resource wrapping the supabase-py client.

    Auth model: Supabase projects expose a URL like
    ``https://<project-ref>.supabase.co`` plus two API keys:
      - **anon key** — safe for browser use, RLS-restricted
      - **service-role key** — bypasses RLS; server-only, treat like a DB password

    For asset ingestion / storage admin / edge function invocation with
    elevated privileges, use the service-role key.
    """

    url: str = Field(description="Supabase project URL, e.g. https://<project-ref>.supabase.co")
    key: str = Field(description="Supabase API key (service-role for admin ops, anon for RLS-scoped reads)")

    def get_client(self):
        """Return an initialized ``supabase.Client``."""
        from supabase import create_client
        return create_client(self.url, self.key)


class SupabaseResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SupabaseResource for use by other components."""

    resource_key: str = Field(
        default="supabase_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    url_env_var: str = Field(
        default="SUPABASE_URL",
        description="Env var holding the Supabase project URL (https://<project-ref>.supabase.co)",
    )
    key_env_var: str = Field(
        default="SUPABASE_KEY",
        description="Env var holding the Supabase API key (service-role for admin ops)",
    )
    use_service_role: bool = Field(
        default=True,
        description=(
            "Whether the configured key is a service-role key (bypasses RLS). "
            "Set false when using an anon key so downstream components can adjust "
            "their behavior accordingly."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SupabaseResource(
            url=dg.EnvVar(self.url_env_var),
            key=dg.EnvVar(self.key_env_var),
        )
        return dg.Definitions(resources={self.resource_key: resource})
