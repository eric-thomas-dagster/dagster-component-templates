"""Shopify Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class ShopifyResource(ConfigurableResource):
    """Dagster resource wrapping the ShopifyAPI client."""

    shop_url: str = Field(description="Shopify store URL e.g. mystore.myshopify.com")
    access_token: str = Field(description="Shopify Admin API access token")
    api_version: str = Field(default="2024-01", description="Shopify API version")

    def get_session(self):
        import shopify
        session = shopify.Session(self.shop_url, self.api_version, self.access_token)
        shopify.ShopifyResource.activate_session(session)
        return session


@dataclass
class ShopifyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ShopifyResource for use by other components."""

    resource_key: str = Field(
        default="shopify_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    shop_url: str = Field(
        description="Shopify store URL e.g. mystore.myshopify.com",
    )
    access_token_env_var: str = Field(
        description="Env var holding Shopify Admin API access token",
    )
    api_version: Optional[str] = Field(
        default="2024-01",
        description="Shopify API version",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ShopifyResource(
            shop_url=self.shop_url,
            access_token=os.environ.get(self.access_token_env_var, ""),
            api_version=self.api_version or "2024-01",
        )
        return dg.Definitions(resources={self.resource_key: resource})
