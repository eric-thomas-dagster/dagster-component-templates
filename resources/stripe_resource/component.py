"""Stripe Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class StripeResource(ConfigurableResource):
    """Dagster resource wrapping the Stripe Python SDK."""

    api_key: str = Field(description="Stripe secret key (sk_live_... or sk_test_...)")

    def get_client(self):
        import stripe
        stripe.api_key = self.api_key
        return stripe


@dataclass
class StripeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a StripeResource for use by other components."""

    resource_key: str = Field(
        default="stripe_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding Stripe secret key (sk_live_... or sk_test_...)",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = StripeResource(
            api_key=os.environ.get(self.api_key_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
