"""E-commerce Data Standardizer Component.

Transform platform-specific e-commerce data (Shopify, Stripe, WooCommerce) into a
standardized common schema for cross-platform e-commerce analysis.
"""

from typing import Optional, Literal
import pandas as pd
import numpy as np
from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class EcommerceStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing e-commerce data across platforms.

    Transforms platform-specific schemas (Shopify, Stripe, WooCommerce) into a
    unified e-commerce data model with consistent field names and structure.

    Standard Schema Output for Orders:
    - order_id, order_number, platform, customer_id, customer_email
    - order_date, order_status, payment_status, fulfillment_status
    - subtotal, tax, shipping, discount, total
    - currency, items_count
    - shipping_address, billing_address (JSON)

    Standard Schema Output for Products:
    - product_id, sku, platform, name, description
    - price, compare_at_price, cost
    - inventory_quantity, inventory_policy
    - vendor, product_type, tags (JSON array)
    - created_date, updated_date

    Standard Schema Output for Customers:
    - customer_id, email, phone, platform
    - first_name, last_name, company
    - total_orders, total_spent, avg_order_value
    - first_order_date, last_order_date
    - tags (JSON array)

    Example:
        ```yaml
        type: dagster_component_templates.EcommerceStandardizerComponent
        attributes:
          asset_name: standardized_shopify_orders
          platform: "shopify"
          resource_type: "orders"
          source_asset: "shopify_orders"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized output asset"
    )

    platform: Literal["shopify", "stripe", "woocommerce"] = Field(
        description="Source e-commerce platform to standardize"
    )

    resource_type: Literal["orders", "products", "customers"] = Field(
        description="Type of e-commerce resource to standardize"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)"
    )

    order_id_field: Optional[str] = Field(
        default=None,
        description="Field name for order ID (auto-detected if not provided)"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Field name for customer ID (auto-detected if not provided)"
    )

    # Optional filters
    filter_status: Optional[str] = Field(
        default=None,
        description="Filter by order/payment status (comma-separated)"
    )

    filter_date_from: Optional[str] = Field(
        default=None,
        description="Filter orders from this date (YYYY-MM-DD)"
    )

    filter_date_to: Optional[str] = Field(
        default=None,
        description="Filter orders to this date (YYYY-MM-DD)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="ecommerce",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        resource_type = self.resource_type
        source_asset = self.source_asset
        order_id_field = self.order_id_field
        customer_id_field = self.customer_id_field
        filter_status = self.filter_status
        filter_date_from = self.filter_date_from
        filter_date_to = self.filter_date_to
        description = self.description or f"Standardized {platform} {resource_type} data"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Parse upstream asset keys
        upstream_keys = []
        if source_asset:
            upstream_keys = [source_asset]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def ecommerce_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that standardizes platform-specific e-commerce data."""

            context.log.info(f"Standardizing {platform} {resource_type} data")

            # Load upstream data
            if upstream_keys and hasattr(context, 'load_asset_value'):
                context.log.info(f"Loading data from upstream asset: {source_asset}")
                raw_data = context.load_asset_value(AssetKey(source_asset))
            elif kwargs:
                raw_data = list(kwargs.values())[0]
            else:
                raise ValueError(
                    f"E-commerce Standardizer '{asset_name}' requires upstream data. "
                    f"Connect to an e-commerce ingestion component (Shopify, Stripe, etc.)"
                )

            # Convert to DataFrame if needed
            if isinstance(raw_data, dict):
                if 'data' in raw_data:
                    df = pd.DataFrame(raw_data['data'])
                elif 'rows' in raw_data:
                    df = pd.DataFrame(raw_data['rows'])
                else:
                    df = pd.DataFrame([raw_data])
            elif isinstance(raw_data, pd.DataFrame):
                df = raw_data
            else:
                raise TypeError(f"Unexpected data type: {type(raw_data)}")

            context.log.info(f"Raw data: {len(df)} rows, {len(df.columns)} columns")
            original_rows = len(df)

            # Platform-specific field mappings per resource type
            field_mappings = {
                "shopify": {
                    "orders": {
                        "order_id": ["id", "order_id"],
                        "order_number": ["order_number", "name"],
                        "customer_id": ["customer.id", "customer_id"],
                        "customer_email": ["customer.email", "email"],
                        "order_date": ["created_at", "processed_at"],
                        "order_status": ["financial_status"],
                        "payment_status": ["financial_status"],
                        "fulfillment_status": ["fulfillment_status"],
                        "subtotal": ["subtotal_price"],
                        "tax": ["total_tax"],
                        "shipping": ["total_shipping_price_set"],
                        "discount": ["total_discounts"],
                        "total": ["total_price"],
                        "currency": ["currency"],
                        "items_count": ["line_items"],
                    },
                    "products": {
                        "product_id": ["id", "product_id"],
                        "sku": ["variants.sku", "sku"],
                        "name": ["title"],
                        "description": ["body_html"],
                        "price": ["variants.price", "price"],
                        "compare_at_price": ["variants.compare_at_price"],
                        "cost": ["variants.cost"],
                        "inventory_quantity": ["variants.inventory_quantity"],
                        "inventory_policy": ["variants.inventory_policy"],
                        "vendor": ["vendor"],
                        "product_type": ["product_type"],
                        "tags": ["tags"],
                        "created_date": ["created_at"],
                        "updated_date": ["updated_at"],
                    },
                    "customers": {
                        "customer_id": ["id", "customer_id"],
                        "email": ["email"],
                        "phone": ["phone"],
                        "first_name": ["first_name"],
                        "last_name": ["last_name"],
                        "company": ["default_address.company"],
                        "total_orders": ["orders_count"],
                        "total_spent": ["total_spent"],
                        "first_order_date": ["created_at"],
                        "last_order_date": ["updated_at"],
                        "tags": ["tags"],
                    },
                },
                "stripe": {
                    "orders": {
                        "order_id": ["id", "payment_intent"],
                        "customer_id": ["customer"],
                        "customer_email": ["billing_details.email", "receipt_email"],
                        "order_date": ["created"],
                        "order_status": ["status"],
                        "payment_status": ["status"],
                        "total": ["amount"],
                        "currency": ["currency"],
                    },
                    "products": {
                        "product_id": ["id"],
                        "sku": ["sku", "product.sku"],
                        "name": ["name"],
                        "description": ["description"],
                        "price": ["price.unit_amount"],
                        "created_date": ["created"],
                        "updated_date": ["updated"],
                    },
                    "customers": {
                        "customer_id": ["id"],
                        "email": ["email"],
                        "phone": ["phone"],
                        "first_name": ["name"],
                        "company": ["metadata.company"],
                        "created_date": ["created"],
                    },
                },
                "woocommerce": {
                    "orders": {
                        "order_id": ["id"],
                        "order_number": ["number"],
                        "customer_id": ["customer_id"],
                        "customer_email": ["billing.email"],
                        "order_date": ["date_created"],
                        "order_status": ["status"],
                        "payment_status": ["status"],
                        "subtotal": ["subtotal"],
                        "tax": ["total_tax"],
                        "shipping": ["shipping_total"],
                        "discount": ["discount_total"],
                        "total": ["total"],
                        "currency": ["currency"],
                        "items_count": ["line_items"],
                    },
                    "products": {
                        "product_id": ["id"],
                        "sku": ["sku"],
                        "name": ["name"],
                        "description": ["description"],
                        "price": ["price", "regular_price"],
                        "compare_at_price": ["sale_price"],
                        "inventory_quantity": ["stock_quantity"],
                        "product_type": ["type"],
                        "tags": ["tags"],
                        "created_date": ["date_created"],
                        "updated_date": ["date_modified"],
                    },
                    "customers": {
                        "customer_id": ["id"],
                        "email": ["email"],
                        "first_name": ["first_name"],
                        "last_name": ["last_name"],
                        "created_date": ["date_created"],
                    },
                },
            }

            mapping = field_mappings.get(platform, {}).get(resource_type)
            if not mapping:
                raise ValueError(f"Unsupported platform/resource: {platform}/{resource_type}")

            # Helper function to find field in DataFrame
            def find_field(possible_names, custom_field=None):
                if custom_field and custom_field in df.columns:
                    return custom_field
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Build standardized DataFrame
            standardized_data = {}

            # Platform identifier
            standardized_data['platform'] = platform

            # Common fields based on resource type
            if resource_type == "orders":
                order_id_col = find_field(mapping['order_id'], order_id_field)
                if order_id_col:
                    standardized_data['order_id'] = df[order_id_col].astype(str)

                order_number_col = find_field(mapping.get('order_number', []))
                if order_number_col:
                    standardized_data['order_number'] = df[order_number_col].astype(str)

                customer_id_col = find_field(mapping.get('customer_id', []), customer_id_field)
                if customer_id_col:
                    standardized_data['customer_id'] = df[customer_id_col].astype(str)

                customer_email_col = find_field(mapping.get('customer_email', []))
                if customer_email_col:
                    standardized_data['customer_email'] = df[customer_email_col]

                order_date_col = find_field(mapping.get('order_date', []))
                if order_date_col:
                    standardized_data['order_date'] = pd.to_datetime(df[order_date_col], errors='coerce')

                order_status_col = find_field(mapping.get('order_status', []))
                if order_status_col:
                    standardized_data['order_status'] = df[order_status_col]

                payment_status_col = find_field(mapping.get('payment_status', []))
                if payment_status_col:
                    standardized_data['payment_status'] = df[payment_status_col]

                fulfillment_col = find_field(mapping.get('fulfillment_status', []))
                if fulfillment_col:
                    standardized_data['fulfillment_status'] = df[fulfillment_col]

                subtotal_col = find_field(mapping.get('subtotal', []))
                if subtotal_col:
                    standardized_data['subtotal'] = pd.to_numeric(df[subtotal_col], errors='coerce')

                tax_col = find_field(mapping.get('tax', []))
                if tax_col:
                    standardized_data['tax'] = pd.to_numeric(df[tax_col], errors='coerce')

                shipping_col = find_field(mapping.get('shipping', []))
                if shipping_col:
                    standardized_data['shipping'] = pd.to_numeric(df[shipping_col], errors='coerce')

                discount_col = find_field(mapping.get('discount', []))
                if discount_col:
                    standardized_data['discount'] = pd.to_numeric(df[discount_col], errors='coerce')

                total_col = find_field(mapping.get('total', []))
                if total_col:
                    standardized_data['total'] = pd.to_numeric(df[total_col], errors='coerce')

                currency_col = find_field(mapping.get('currency', []))
                if currency_col:
                    standardized_data['currency'] = df[currency_col]

            elif resource_type == "products":
                product_id_col = find_field(mapping['product_id'])
                if product_id_col:
                    standardized_data['product_id'] = df[product_id_col].astype(str)

                sku_col = find_field(mapping.get('sku', []))
                if sku_col:
                    standardized_data['sku'] = df[sku_col]

                name_col = find_field(mapping.get('name', []))
                if name_col:
                    standardized_data['name'] = df[name_col]

                description_col = find_field(mapping.get('description', []))
                if description_col:
                    standardized_data['description'] = df[description_col]

                price_col = find_field(mapping.get('price', []))
                if price_col:
                    standardized_data['price'] = pd.to_numeric(df[price_col], errors='coerce')

                compare_col = find_field(mapping.get('compare_at_price', []))
                if compare_col:
                    standardized_data['compare_at_price'] = pd.to_numeric(df[compare_col], errors='coerce')

                cost_col = find_field(mapping.get('cost', []))
                if cost_col:
                    standardized_data['cost'] = pd.to_numeric(df[cost_col], errors='coerce')

                inventory_col = find_field(mapping.get('inventory_quantity', []))
                if inventory_col:
                    standardized_data['inventory_quantity'] = pd.to_numeric(df[inventory_col], errors='coerce')

                vendor_col = find_field(mapping.get('vendor', []))
                if vendor_col:
                    standardized_data['vendor'] = df[vendor_col]

                product_type_col = find_field(mapping.get('product_type', []))
                if product_type_col:
                    standardized_data['product_type'] = df[product_type_col]

                created_col = find_field(mapping.get('created_date', []))
                if created_col:
                    standardized_data['created_date'] = pd.to_datetime(df[created_col], errors='coerce')

                updated_col = find_field(mapping.get('updated_date', []))
                if updated_col:
                    standardized_data['updated_date'] = pd.to_datetime(df[updated_col], errors='coerce')

            elif resource_type == "customers":
                customer_id_col = find_field(mapping['customer_id'], customer_id_field)
                if customer_id_col:
                    standardized_data['customer_id'] = df[customer_id_col].astype(str)

                email_col = find_field(mapping.get('email', []))
                if email_col:
                    standardized_data['email'] = df[email_col]

                phone_col = find_field(mapping.get('phone', []))
                if phone_col:
                    standardized_data['phone'] = df[phone_col]

                first_name_col = find_field(mapping.get('first_name', []))
                if first_name_col:
                    standardized_data['first_name'] = df[first_name_col]

                last_name_col = find_field(mapping.get('last_name', []))
                if last_name_col:
                    standardized_data['last_name'] = df[last_name_col]

                company_col = find_field(mapping.get('company', []))
                if company_col:
                    standardized_data['company'] = df[company_col]

                total_orders_col = find_field(mapping.get('total_orders', []))
                if total_orders_col:
                    standardized_data['total_orders'] = pd.to_numeric(df[total_orders_col], errors='coerce')

                total_spent_col = find_field(mapping.get('total_spent', []))
                if total_spent_col:
                    standardized_data['total_spent'] = pd.to_numeric(df[total_spent_col], errors='coerce')

                first_order_col = find_field(mapping.get('first_order_date', []))
                if first_order_col:
                    standardized_data['first_order_date'] = pd.to_datetime(df[first_order_col], errors='coerce')

                last_order_col = find_field(mapping.get('last_order_date', []))
                if last_order_col:
                    standardized_data['last_order_date'] = pd.to_datetime(df[last_order_col], errors='coerce')

            # Create standardized DataFrame
            std_df = pd.DataFrame(standardized_data)

            # Calculate derived metrics
            if resource_type == "customers" and 'total_spent' in std_df.columns and 'total_orders' in std_df.columns:
                std_df['avg_order_value'] = (std_df['total_spent'] / std_df['total_orders']).round(2)

            # Apply filters
            if filter_status and resource_type == "orders" and 'order_status' in std_df.columns:
                statuses = [s.strip() for s in filter_status.split(',')]
                std_df = std_df[std_df['order_status'].isin(statuses)]
                context.log.info(f"Filtered to statuses: {statuses}")

            if filter_date_from and resource_type == "orders" and 'order_date' in std_df.columns:
                std_df = std_df[std_df['order_date'] >= pd.to_datetime(filter_date_from)]
                context.log.info(f"Filtered from date: {filter_date_from}")

            if filter_date_to and resource_type == "orders" and 'order_date' in std_df.columns:
                std_df = std_df[std_df['order_date'] <= pd.to_datetime(filter_date_to)]
                context.log.info(f"Filtered to date: {filter_date_to}")

            # Replace inf and -inf with NaN
            std_df = std_df.replace([float('inf'), float('-inf')], pd.NA)

            final_rows = len(std_df)
            context.log.info(
                f"Standardization complete: {original_rows} → {final_rows} rows, "
                f"{len(std_df.columns)} columns"
            )

            # Add metadata
            metadata = {
                "platform": platform,
                "resource_type": resource_type,
                "original_rows": original_rows,
                "final_rows": final_rows,
                "columns": list(std_df.columns),
            }

            # Add resource-specific metadata
            if resource_type == "orders" and 'total' in std_df.columns:
                metadata["total_revenue"] = float(std_df['total'].sum())

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(std_df) > 0:
                return Output(
                    value=std_df,
                    metadata={
                        "row_count": len(std_df),
                        "columns": std_df.columns.tolist(),
                        "sample": MetadataValue.md(std_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(std_df.head(10))
                    }
                )
            else:
                return std_df

        return Definitions(assets=[ecommerce_standardizer_asset])
